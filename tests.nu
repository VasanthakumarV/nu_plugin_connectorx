use std/assert
use std/log

def "test plugin" [] {
  mut db_file = (mktemp --suffix .db) | path expand

  let data = [
    [a b];
    [0 0]
    [1 1]
    [2 1]
    [3 2]
    [4 3]
    [5 5]
    [6 8]
    [7 13]
  ]
  $data | into sqlite $db_file

  mut output = cx $"sqlite://($db_file)" -p a -n 3 -q "select * from main"
  $output = $output | sort-by "a"
  assert equal $data $output

  $output = cx $"sqlite://($db_file)" -q "select * from main"
  $output = $output | sort-by "a"
  assert equal $data $output

  let query_file = mktemp --suffix .sql
  "select * from main" | save -f $query_file
  $output = cx $"sqlite://($db_file)" -f $query_file
  $output = $output | sort-by "a"
  assert equal $data $output
  rm $query_file

  let parquet_output = mktemp --suffix .parquet
  cx $"sqlite://($db_file)" -p a -n 3 -q "select * from main" -o $parquet_output
  $output = polars open $parquet_output | polars collect | polars into-nu | sort-by "a"
  assert equal $data $output
  rm $parquet_output

  rm $db_file
}

def main [] {
  log info "Importing plugin..."
  plugin use connectorx
  plugin use polars

  let test_commands = (
    scope commands
    | where ($it.type == "custom")
    and ($it.name | str starts-with "test ")
    and not ($it.description | str starts-with "ignore")
    | get name
    | each {|test| [$"log info 'Running test: ($test)'" $test] } | flatten
    | str join "; "
  )

  nu --commands $"source ($env.CURRENT_FILE); ($test_commands)"
  log info "Tests completed successfully"
}
