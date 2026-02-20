use std::{fs::File, io::Read, iter, path::PathBuf};

use arrow::{
    array::{
        Array, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
        DictionaryArray, DurationMillisecondArray, DurationNanosecondArray, DurationSecondArray,
        FixedSizeBinaryArray, FixedSizeListArray, Float64Array, Int64Array, LargeBinaryArray,
        LargeListArray, LargeListViewArray, LargeStringArray, ListArray, ListViewArray,
        RecordBatch, StringArray, StringViewArray, StructArray, Time32MillisecondArray,
        Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray, timezone::Tz,
    },
    compute::kernels::cast,
    datatypes::{
        DataType, Int8Type, Int16Type, Int32Type, Int64Type, TimeUnit, UInt8Type, UInt16Type,
        UInt32Type, UInt64Type,
    },
    util::display::ArrayFormatter,
};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use connectorx::{
    get_arrow::get_arrow,
    partition::{PartitionQuery, partition},
    prelude::{CXQuery, parse_source},
};
use nu_path::expand_path_with;
use nu_plugin::{MsgPackSerializer, Plugin, SimplePluginCommand, serve_plugin};
use nu_protocol::{Category, LabeledError, Record, Signature, Span, SyntaxShape, Type, Value};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use rootcause::{hooks::Hooks, option_ext::OptionExt, prelude::*};
use rootcause_tracing::{RootcauseLayer, SpanCollector};
use tracing::instrument;
use tracing_subscriber::{Registry, layer::SubscriberExt as _};

fn main() {
    tracing::subscriber::set_global_default(
        Registry::default()
            .with(RootcauseLayer)
            .with(tracing_subscriber::fmt::layer()),
    )
    .expect("failed to set subscriber");

    Hooks::new()
        .report_creation_hook(SpanCollector::new())
        .install()
        .expect("failed to install hooks");

    serve_plugin(&ConnectorXPlugin {}, MsgPackSerializer {});
}

struct ConnectorXPlugin;

impl Plugin for ConnectorXPlugin {
    fn version(&self) -> String {
        env!("CARGO_PKG_VERSION").into()
    }

    fn commands(&self) -> Vec<Box<dyn nu_plugin::PluginCommand<Plugin = Self>>> {
        vec![Box::new(CX)]
    }
}

struct CX;

impl SimplePluginCommand for CX {
    type Plugin = ConnectorXPlugin;

    fn name(&self) -> &str {
        "cx"
    }

    fn description(&self) -> &str {
        "Load data from DBs using ConnectorX"
    }

    fn signature(&self) -> nu_protocol::Signature {
        Signature::build(self.name())
            .required("uri", SyntaxShape::String, "ConnectorX connection URI")
            .named("query", SyntaxShape::String, "SQL Query to run", Some('q'))
            .named(
                "sql-file",
                SyntaxShape::String,
                "File with SQL Query to run",
                Some('f'),
            )
            .named(
                "partition-on",
                SyntaxShape::String,
                "Column to partition the data on",
                Some('p'),
            )
            .named(
                "num-partitions",
                SyntaxShape::Int,
                "Number of partitions to make",
                Some('n'),
            )
            .named(
                "output-parquet",
                SyntaxShape::Filepath,
                "Parquet file save location, if provided no pipeline output will be produced",
                Some('o'),
            )
            .input_output_types(vec![
                (Type::Nothing, Type::Nothing),
                (Type::Nothing, Type::table()),
            ])
            .category(Category::Database)
    }

    fn run(
        &self,
        _plugin: &Self::Plugin,
        engine: &nu_plugin::EngineInterface,
        call: &nu_plugin::EvaluatedCall,
        _input: &nu_protocol::Value,
    ) -> Result<nu_protocol::Value, nu_protocol::LabeledError> {
        let cwd = engine.get_current_dir()?;

        let uri: String = call.req(0)?;

        let mut query = String::new();
        match (
            call.get_flag("query")?,
            call.get_flag::<String>("sql-file")?,
        ) {
            (Some(_), Some(_)) => {
                return Err(LabeledError::new(
                    "Do not pass both `--query` and `--sql-file` flags",
                ));
            }
            (Some(q), None) => {
                query = q;
            }
            (None, Some(sql_file)) => {
                let sql_file = expand_path_with(sql_file, &cwd, true);
                File::open(sql_file.clone())
                    .map_err(|e| LabeledError::new(format!("Failed to open {sql_file:?}: {e}")))?
                    .read_to_string(&mut query)
                    .map_err(|e| {
                        LabeledError::new(format!("Failed to read the sql file {sql_file:?}: {e}"))
                    })?;
            }
            (None, None) => {
                return Err(LabeledError::new(
                    "Atleast one of `--query` or `--sql-file` must be passed",
                ));
            }
        }

        let data = match (
            call.get_flag::<String>("partition-on")?,
            call.get_flag::<i32>("num-partitions")?,
        ) {
            (Some(p), Some(n)) => {
                if n <= 0 {
                    return Err(LabeledError::new("`--num-partitions` must be >= 0"));
                }
                run_query(uri, query, Some(p), Some(n))
                    .map_err(|e| LabeledError::new(e.to_string()))?
            }
            (None, None) => {
                run_query(uri, query, None, None).map_err(|e| LabeledError::new(e.to_string()))?
            }
            (Some(_), None) => {
                return Err(LabeledError::new("`--num-partitions` must be provided"));
            }
            (None, Some(_)) => {
                return Err(LabeledError::new("`--partition-on` must be provided"));
            }
        };

        if let Some(save_path) = call.get_flag::<String>("output-parquet")? {
            let save_path = expand_path_with(save_path, &cwd, true);
            save_as_parquet(&data, save_path).map_err(|e| LabeledError::new(e.to_string()))?;
            Ok(Value::nothing(call.head))
        } else {
            let records = convert_arrow_to_nu(data, call.head)
                .map_err(|e| LabeledError::new(e.to_string()))?;
            Ok(Value::list(records, call.head))
        }
    }
}

#[instrument(skip_all)]
fn convert_arrow_to_nu(data: Vec<RecordBatch>, span: Span) -> Result<Vec<Value>, Report> {
    let column_names: Vec<_> = data[0]
        .schema_ref()
        .fields
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let record: Record = column_names
        .into_iter()
        .zip(iter::repeat(Value::nothing(span)))
        .collect();
    let n_rows: Vec<_> = data.iter().map(|d| d.num_rows()).collect();
    let mut records: Vec<Value> =
        iter::repeat_n(Value::record(record, span), n_rows.iter().sum()).collect();
    let _ = data
        .into_iter()
        .zip(n_rows)
        .try_fold(0, |acc, (data, n_rows)| {
            data.columns()
                .iter()
                .enumerate()
                .try_for_each(|(col_idx, column)| {
                    convert_column_to_nu(column, col_idx, &mut records[acc..(acc + n_rows)], span)
                })?;
            Ok::<_, Report>(acc + n_rows)
        })?;
    Ok(records)
}

#[instrument(skip(data))]
fn save_as_parquet(data: &[RecordBatch], save_path: PathBuf) -> Result<(), Report> {
    let file = File::create(save_path.clone())
        .attach(format!("Failed to open {save_path:?} to save parquet data"))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, data.first().ok_or_report()?.schema(), Some(props))
        .attach("Failed to create parquet writer for arrow data")?;
    data.iter()
        .try_for_each(|d| writer.write(d).attach("Failed to write parquet file"))?;
    writer
        .close()
        .attach("Failed to close the parquet writer")?;
    Ok(())
}

#[instrument(skip(uri))] // We skip the uri, to avoid logging any secrets
fn run_query(
    uri: String,
    query: String,
    partition_on: Option<String>,
    num_partitions: Option<i32>,
) -> Result<Vec<RecordBatch>, Report> {
    let source_conn = parse_source(uri.as_str(), None).attach("Failed to parse the URI")?;

    let (origin_query, partition_queries) =
        if let (Some(p), Some(n)) = (partition_on, num_partitions) {
            let partition_query =
                PartitionQuery::new(query.as_str(), p.as_str(), None, None, n as usize);
            let queries = partition(&partition_query, &source_conn)
                .attach("Failed to partition the query")?;
            (Some(query), queries)
        } else {
            (None, vec![CXQuery::from(&query)])
        };

    Ok(
        get_arrow(&source_conn, origin_query, &partition_queries, None)
            .attach("Failed to run the query")?
            .arrow()
            .attach("Failed to retrieve arrow data from CX")?,
    )
}

#[instrument(skip(values), fields(values_len = values.len()))]
fn set(values: &mut [Value], row_idx: usize, col_idx: usize, value: Value) {
    match values.get_mut(row_idx).unwrap() {
        val @ Value::Nothing { .. } => {
            *val = value;
        }
        Value::Record { val, .. } => {
            *(val.to_mut().get_index_mut(col_idx).unwrap().1) = value;
        }
        _ => unreachable!(),
    }
}

macro_rules! convert {
    ($column:ident, $col_idx: ident, $records:ident, $span:ident, $type:ty, $v:ident => $func:expr) => {
        let column = $column.as_any().downcast_ref::<$type>().unwrap();
        for (row_idx, value) in column.iter().enumerate() {
            if let Some($v) = value {
                set($records, row_idx, $col_idx, $func);
            }
        }
    };
}

macro_rules! convert_list {
    ($column:ident, $col_idx:ident, $records:ident, $span:ident, $type:ty) => {
        let column = $column.as_any().downcast_ref::<$type>().unwrap();
        let values = column.values();
        let offsets = column.offsets().as_ref();
        let mut list: Vec<Value> = iter::repeat_n(Value::nothing($span), values.len()).collect();
        convert_column_to_nu(values, $col_idx, &mut list, $span)?;
        (offsets[1..]).iter().enumerate().zip(column.iter()).fold(
            offsets[0],
            |start, ((row_idx, &offset), is_null)| {
                if is_null.is_some() {
                    let tmp = &list[start as usize..offset as usize];
                    let list = Value::list(tmp.to_vec(), $span);
                    set($records, row_idx, $col_idx, list);
                }
                offset
            },
        );
    };
}

macro_rules! convert_datetime {
    ($column:ident, $col_idx:ident, $records:ident, $span:ident, $type:ty, $tz:ident, $v:ident => $func:expr) => {
        let column = $column.as_any().downcast_ref::<$type>().unwrap();
        for (row_idx, value) in column.iter().enumerate() {
            if let Some($v) = value {
                let datetime = $func;
                set(
                    $records,
                    row_idx,
                    $col_idx,
                    if let Some(tz) = $tz {
                        Value::date(datetime.with_timezone(&tz).fixed_offset(), $span)
                    } else {
                        Value::date(datetime.fixed_offset(), $span)
                    },
                );
            }
        }
    };
}

macro_rules! convert_date {
    ($column:ident, $col_idx:ident, $records:ident, $span:ident, $type:ty, $v:ident => $func:expr) => {
        let column = $column.as_any().downcast_ref::<$type>().unwrap();
        for (row_idx, value) in column.iter().enumerate() {
            if let Some($v) = value {
                let datetime = $func.unwrap().and_hms_opt(0, 0, 0).unwrap();
                let datetime = Utc.from_local_datetime(&datetime).unwrap().fixed_offset();
                set($records, row_idx, $col_idx, Value::date(datetime, $span));
            }
        }
    };
}

#[instrument(skip(column, records, span), fields(column_dtype = %column.data_type(), records_len = records.len()))]
fn convert_column_to_nu(
    column: &std::sync::Arc<dyn Array>,
    col_idx: usize,
    records: &mut [Value],
    span: Span,
) -> Result<(), Report> {
    match column.data_type() {
        DataType::Null => (),
        DataType::Boolean => {
            convert!(column, col_idx, records, span, BooleanArray, v => Value::bool(v, span));
        }
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => {
            let column = cast(column, &DataType::Int64).unwrap();
            convert!(column, col_idx, records, span, Int64Array, v => Value::int(v, span));
        }
        DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _)
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64 => {
            let column = cast(column, &DataType::Float64).unwrap();
            convert!(column, col_idx, records, span, Float64Array, v => Value::float(v, span));
        }
        DataType::Timestamp(time_unit, time_zone) => {
            let tz: Option<Tz> = time_zone
                .as_ref()
                .map(|tz| {
                    tz.parse()
                        .attach(format!("Failed to parse the timezone info: {tz}"))
                })
                .transpose()?;
            match time_unit {
                TimeUnit::Second => {
                    convert_datetime!(column, col_idx, records, span, TimestampSecondArray, tz, v => DateTime::from_timestamp_secs(v).unwrap());
                }
                TimeUnit::Millisecond => {
                    convert_datetime!(column, col_idx, records, span, TimestampMillisecondArray, tz, v => DateTime::from_timestamp_millis(v).unwrap());
                }
                TimeUnit::Microsecond => {
                    convert_datetime!(column, col_idx, records, span, TimestampMicrosecondArray, tz, v => DateTime::from_timestamp_micros(v).unwrap());
                }
                TimeUnit::Nanosecond => {
                    convert_datetime!(column, col_idx, records, span, TimestampNanosecondArray, tz, v => DateTime::from_timestamp_nanos(v));
                }
            }
        }
        DataType::Date32 => {
            convert_date!(column, col_idx, records, span, Date32Array, v => NaiveDate::from_epoch_days(v));
        }
        DataType::Date64 => {
            convert_date!(column, col_idx, records, span, Date64Array, v => NaiveDate::from_epoch_days((v / 86_400_000) as i32));
        }
        DataType::Time32(time_unit) => match time_unit {
            TimeUnit::Second => {
                convert!(column, col_idx, records, span, Time32SecondArray, v => Value::duration((v as i64) * 1_000_000_000, span));
            }
            TimeUnit::Millisecond => {
                convert!(column, col_idx, records, span, Time32MillisecondArray, v => Value::duration((v as i64) * 1_000_000, span));
            }
            _ => unreachable!(),
        },
        DataType::Time64(time_unit) => match time_unit {
            TimeUnit::Microsecond => {
                convert!(column, col_idx, records, span, Time64MicrosecondArray, v => Value::duration(v * 1_000, span));
            }
            TimeUnit::Nanosecond => {
                convert!(column, col_idx, records, span, Time64NanosecondArray, v => Value::duration(v, span));
            }
            _ => unreachable!(),
        },
        DataType::Duration(time_unit) => match time_unit {
            TimeUnit::Second => {
                convert!(column, col_idx, records, span, DurationSecondArray, v => Value::duration(v * 1_000_000_000, span));
            }
            TimeUnit::Millisecond => {
                convert!(column, col_idx, records, span, DurationMillisecondArray, v => Value::duration(v * 1_000_000, span));
            }
            TimeUnit::Microsecond => {
                convert!(column, col_idx, records, span, DurationMillisecondArray, v => Value::duration(v * 1000, span));
            }
            TimeUnit::Nanosecond => {
                convert!(column, col_idx, records, span, DurationNanosecondArray, v => Value::duration(v, span));
            }
        },
        DataType::Interval(_) => {
            let array_fmt = ArrayFormatter::try_new(column, &Default::default()).unwrap();
            for row_idx in 0..column.len() {
                let interval_str = array_fmt.value(row_idx).to_string();
                set(records, row_idx, col_idx, Value::string(interval_str, span));
            }
        }
        DataType::Binary => {
            convert!(column, col_idx, records, span, BinaryArray, v => Value::binary(v, span));
        }
        DataType::FixedSizeBinary(_) => {
            convert!(column, col_idx, records, span, FixedSizeBinaryArray, v => Value::binary(v, span));
        }
        DataType::LargeBinary => {
            convert!(column, col_idx, records, span, LargeBinaryArray, v => Value::binary(v, span));
        }
        DataType::BinaryView => {
            convert!(column, col_idx, records, span, BinaryViewArray, v => Value::binary(v, span));
        }
        DataType::Utf8 => {
            convert!(column, col_idx, records, span, StringArray, v => Value::string(v, span));
        }
        DataType::LargeUtf8 => {
            convert!(column, col_idx, records, span, LargeStringArray, v => Value::string(v, span));
        }
        DataType::Utf8View => {
            convert!(column, col_idx, records, span, StringViewArray, v => Value::string(v, span));
        }
        DataType::List(_) => {
            convert_list!(column, col_idx, records, span, ListArray);
        }
        DataType::ListView(_) => {
            convert_list!(column, col_idx, records, span, ListViewArray);
        }
        DataType::FixedSizeList(_, _) => {
            let column = column
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .unwrap();
            let values = column.values();
            let fixed_len = column.value_length();
            let mut list: Vec<Value> = iter::repeat_n(Value::nothing(span), values.len()).collect();
            convert_column_to_nu(values, col_idx, &mut list, span)?;
            list.chunks_exact(fixed_len as usize)
                .enumerate()
                .zip(column.iter())
                .for_each(|((row_idx, chunk), is_null)| {
                    if is_null.is_some() {
                        let list = Value::list(chunk.to_vec(), span);
                        set(records, row_idx, col_idx, list);
                    }
                });
        }
        DataType::LargeList(_) => {
            convert_list!(column, col_idx, records, span, LargeListArray);
        }
        DataType::LargeListView(_) => {
            convert_list!(column, col_idx, records, span, LargeListViewArray);
        }
        DataType::Struct(fields) => {
            let column = column.as_any().downcast_ref::<StructArray>().unwrap();
            let inner_record: Record = fields
                .into_iter()
                .map(|f| f.name().clone())
                .zip(iter::repeat(Value::nothing(span)))
                .collect();
            let mut inner_records: Vec<Value> =
                iter::repeat_n(Value::record(inner_record, span), column.len()).collect();
            for (inner_col_idx, inner_column) in column.columns().iter().enumerate() {
                convert_column_to_nu(inner_column, inner_col_idx, &mut inner_records, span)?;
            }
            for (row_idx, inner_record) in inner_records.into_iter().enumerate() {
                set(records, row_idx, col_idx, inner_record);
            }
        }
        DataType::Dictionary(key_type, value_type) => {
            fn convert_dict_value<T: arrow::datatypes::ArrowDictionaryKeyType>(
                column: &std::sync::Arc<dyn Array + 'static>,
                col_idx: usize,
                records: &mut [Value],
                span: Span,
                value_type: &DataType,
            ) {
                let column = column
                    .as_any()
                    .downcast_ref::<DictionaryArray<T>>()
                    .unwrap();
                match value_type {
                    DataType::Utf8 => {
                        convert!(column, col_idx, records, span, StringArray, v => Value::string(v, span));
                    }
                    DataType::LargeUtf8 => {
                        convert!(column, col_idx, records, span, LargeStringArray, v => Value::string(v, span));
                    }
                    DataType::Utf8View => {
                        convert!(column, col_idx, records, span, StringViewArray, v => Value::string(v, span));
                    }
                    datatype => unimplemented!(
                        "{datatype} not currently supported as dictionary value type"
                    ),
                }
            }
            match &**key_type {
                DataType::Int8 => {
                    convert_dict_value::<Int8Type>(column, col_idx, records, span, value_type);
                }
                DataType::Int16 => {
                    convert_dict_value::<Int16Type>(column, col_idx, records, span, value_type);
                }
                DataType::Int32 => {
                    convert_dict_value::<Int32Type>(column, col_idx, records, span, value_type);
                }
                DataType::Int64 => {
                    convert_dict_value::<Int64Type>(column, col_idx, records, span, value_type);
                }
                DataType::UInt8 => {
                    convert_dict_value::<UInt8Type>(column, col_idx, records, span, value_type);
                }
                DataType::UInt16 => {
                    convert_dict_value::<UInt16Type>(column, col_idx, records, span, value_type);
                }
                DataType::UInt32 => {
                    convert_dict_value::<UInt32Type>(column, col_idx, records, span, value_type);
                }
                DataType::UInt64 => {
                    convert_dict_value::<UInt64Type>(column, col_idx, records, span, value_type);
                }
                _ => unreachable!(),
            }
        }
        datatype => unimplemented!("{datatype} is not supported"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow::{
        array::{ArrayRef, Int32Array, IntervalDayTimeArray, PrimitiveArray, record_batch},
        datatypes::{Date32Type, IntervalDayTime, Time32SecondType},
    };
    use arrow_schema::{Field, Schema};
    use chrono::FixedOffset;
    use nu_protocol::record;

    #[test]
    fn simple_arrow_types() {
        let batch = vec![
            record_batch!(
                ("a", Int16, [1, 2, 3]),
                ("b", Float32, [Some(4.0), None, Some(5.0)]),
                ("c", Utf8View, ["alpha", "beta", "gamma"])
            )
            .unwrap(),
        ];
        let output = convert_arrow_to_nu(batch, Span::test_data()).unwrap();

        let expected: Vec<_> = [Value::test_int(1), Value::test_int(2), Value::test_int(3)]
            .into_iter()
            .zip([
                Value::test_float(4.0),
                Value::test_nothing(),
                Value::test_float(5.0),
            ])
            .zip([
                Value::test_string("alpha"),
                Value::test_string("beta"),
                Value::test_string("gamma"),
            ])
            .map(|((a, b), c)| Value::test_record(record! {"a" => a, "b" => b, "c" => c}))
            .collect();

        assert_eq!(output, expected);
    }

    #[test]
    fn date_arrow_types() {
        // PrimitiveArray<Date32>[2003-10-31,  2007-02-04,  1969-01-01]
        let date32_array: PrimitiveArray<Date32Type> = vec![12356, 13548, -365].into();
        let time32_array: PrimitiveArray<Time32SecondType> = vec![7201, 60054, 60].into();
        let timestamp_array = TimestampSecondArray::from(vec![Some(11111111), None, None])
            .with_timezone("+10:00".to_string());
        let interval_array = IntervalDayTimeArray::from(vec![
            IntervalDayTime::new(1, 1000), // 1 day, 1000 milliseconds
            IntervalDayTime::new(33, 0),   // 33 days, 0 milliseconds
            IntervalDayTime::new(0, 12 * 60 * 60 * 1000), // 0 days, 12 hours
        ]);
        let schema = Schema::new(vec![
            Field::new("d", date32_array.data_type().clone(), true),
            Field::new("e", time32_array.data_type().clone(), true),
            Field::new("f", timestamp_array.data_type().clone(), true),
            Field::new("g", interval_array.data_type().clone(), true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(date32_array),
                Arc::new(time32_array),
                Arc::new(timestamp_array),
                Arc::new(interval_array),
            ],
        )
        .unwrap();
        let output = convert_arrow_to_nu(vec![batch], Span::test_data()).unwrap();

        let expected: Vec<_> = [
            Value::test_date(
                Utc.with_ymd_and_hms(2003, 10, 31, 0, 0, 0)
                    .unwrap()
                    .fixed_offset(),
            ),
            Value::test_date(
                Utc.with_ymd_and_hms(2007, 2, 4, 0, 0, 0)
                    .unwrap()
                    .fixed_offset(),
            ),
            Value::test_date(
                Utc.with_ymd_and_hms(1969, 1, 1, 0, 0, 0)
                    .unwrap()
                    .fixed_offset(),
            ),
        ]
        .into_iter()
        .zip([
            Value::test_duration(7201 * 1_000_000_000),
            Value::test_duration(60054 * 1_000_000_000),
            Value::test_duration(60 * 1_000_000_000),
        ])
        .zip([
            Value::test_date(
                "1970-05-10T00:25:11+10:00"
                    .parse::<DateTime<FixedOffset>>()
                    .unwrap(),
            ),
            Value::test_nothing(),
            Value::test_nothing(),
        ])
        .zip([
            Value::test_string("1 days 1.000 secs"),
            Value::test_string("33 days"),
            Value::test_string("12 hours"),
        ])
        .map(|(((d, e), f), g)| {
            Value::test_record(record! {"d" => d, "e" => e, "f" => f, "g" => g})
        })
        .collect();

        assert_eq!(output, expected);
    }

    #[test]
    fn complex_arrow_types() {
        let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(0), None, Some(2)]),
            None,
            Some(vec![]),
        ]);

        let fixed_list_array = FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
            vec![
                Some(vec![Some(0), Some(1), Some(2)]),
                None,
                Some(vec![Some(3), None, Some(5)]),
            ],
            3,
        );

        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("a", DataType::Boolean, true)),
                Arc::new(BooleanArray::from(vec![Some(false), None, Some(true)])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("b", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![Some(42), None, Some(19)])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("c", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![None, None, None])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("d", list_array.data_type().clone(), true)),
                Arc::new(list_array.clone()) as ArrayRef,
            ),
        ]);

        let schema = Schema::new(vec![
            Field::new("g", list_array.data_type().clone(), true),
            Field::new("h", fixed_list_array.data_type().clone(), true),
            Field::new("i", struct_array.data_type().clone(), true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(list_array),
                Arc::new(fixed_list_array),
                Arc::new(struct_array),
            ],
        )
        .unwrap();

        let output = convert_arrow_to_nu(vec![batch], Span::test_data()).unwrap();

        let expected: Vec<_> = [
            Value::test_list(vec![
                Value::test_int(0),
                Value::test_nothing(),
                Value::test_int(2),
            ]),
            Value::test_nothing(),
            Value::test_list(vec![]),
        ]
        .into_iter()
        .zip([
            Value::test_list(vec![
                Value::test_int(0),
                Value::test_int(1),
                Value::test_int(2),
            ]),
            Value::test_nothing(),
            Value::test_list(vec![
                Value::test_int(3),
                Value::test_nothing(),
                Value::test_int(5),
            ]),
        ])
        .zip([
            Value::test_record(
                record! {
                    "a" => Value::test_bool(false),
                    "b" => Value::test_int(42),
                    "c" => Value::test_nothing(),
                    "d" => Value::test_list(vec![Value::test_int(0), Value::test_nothing(), Value::test_int(2)]),
                },
            ),
            Value::test_record(
                record! {
                    "a" => Value::test_nothing(),
                    "b" => Value::test_nothing(),
                    "c" => Value::test_nothing(),
                    "d" => Value::test_nothing(),
                },
            ),
            Value::test_record(
                record! {
                    "a" => Value::test_bool(true),
                    "b" => Value::test_int(19),
                    "c" => Value::test_nothing(),
                    "d" => Value::test_list(vec![]),
                },
            ),
        ])
        .map(|((d, e), f)| Value::test_record(record! {"g" => d, "h" => e, "i" => f}))
        .collect();

        assert_eq!(output, expected);
    }
}
