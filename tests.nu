use std/assert
use std/log

def "test plugin" [] {
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

  mut db_file = "./test.db" | path expand

  log info $"Creating sqlite db at: ($db_file)"
  $data | into sqlite $db_file

  mut output = cx $"sqlite://($db_file)" -p a -n 3 -q "select * from main"
  $output = $output | sort-by "a"

  assert equal $data $output

  rm $db_file
}

def main [] {
  log info "Importing plugin..."
  plugin use connectorx

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
