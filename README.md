# nu_plugin_connectorx

| [crates.io](https://crates.io/crates/nu_plugin_connectorx) |

> [!WARNING]
> This tool might be rough around the edges, it is not widely tested.

<details>
  <summary>`cx --help` content</summary>
  
  Load data from DBs using ConnectorX

  Usage:
    > cx {flags} <uri>

  Flags:
    -h, --help: Display the help message for this command
    -q, --query <string>: SQL Query to run
    -f, --sql-file <string>: File with SQL Query to run
    -p, --partition-on <string>: Column to partition the data on
    -n, --num-partitions <int>: Number of partitions to make
    -o, --output-parquet <path>: Parquet file save location, if provided no pipeline output will be produced

  Parameters:
    uri <string>: ConnectorX connection URI

  Input/output types:
    ╭───┬─────────┬─────────╮
    │ # │  input  │ output  │
    ├───┼─────────┼─────────┤
    │ 0 │ nothing │ nothing │
    │ 1 │ nothing │ table   │
    ╰───┴─────────┴─────────╯
</details>

# Installation

```sh
cargo install --locked nu_plugin_connectorx
```

# Example Usage

Create dummy table in sqlite db,
```nu
[[a b c]; [1 2 3] [4 5 6]] | into sqlite test.db
```

Run query against the DB,
```nu
cx $'sqlite://("./test.db" | path expand)' -q 'select * from main'
```

Output,
```sh
╭───┬───┬───┬───╮
│ # │ a │ b │ c │
├───┼───┼───┼───┤
│ 0 │ 1 │ 2 │ 3 │
│ 1 │ 4 │ 5 │ 6 │
╰───┴───┴───┴───╯
```
