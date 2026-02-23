# nu_plugin_connectorx

| [crates.io](https://crates.io/crates/nu_plugin_connectorx) |

> [!WARNING]
> This tool might be rough around the edges, it is not widely tested.

# Installation

```sh
cargo install nu_plugin_connectorx
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
