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

  let db_file = mktemp --suffix db

  log info $"Creating sqlite db at: ($db_file)"
  $data | into sqlite $db_file

  mut output = cx $"sqlite://($db_file)" -p a -n 3 -q "select * from main"
  $output = $output | sort-by "a"

  assert equal $data $output

  rm $db_file
}

def main [] {
  log info "Building binary..."
  ^pixi r cargo build
  let bin_dir = ($env | get -o CARGO_TARGET_DIR | default "./target")
    | path join "debug" "nu_plugin_connectorx"

  log info "Adding plugin..."
  const plugin_reg = "connectorx.msgpackz"
  plugin add --plugin-config $plugin_reg $bin_dir
  plugin use --plugin-config $plugin_reg connectorx

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
