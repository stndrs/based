# based

[![Package Version](https://img.shields.io/hexpm/v/based)](https://hex.pm/packages/based)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/based/)

This package provides a unified interface for interacting SQL databases. With
`based`, gleam code can remain mostly database-agnostic. `based/db` provides a
set of generic functions that supports compatible adapter packages.

Packages for different database backends can conform to the type and function
definitions in this package.

```sh
gleam add based
```

```gleam
import based/db
import dynamic
import database
import database/value

const sql = "SELECT name FROM users WHERE id=$1;"

pub type User {
  User(id: Int, name: String)
}

pub fn main() {
  let assert Ok(config) = load_config()
  let assert Ok(db) = database.start(config)

  let user_decoder = fn() {
    use id <- decode.field("id", decode.int)
    use name <- decode.field("name", decode.string)

    decode.success(User(id:, name:))
  }

  use conn <- database.with_connection(db)

  "SELECT id, name FROM users WHERE id=$1"
  |> db.sql
  |> db.values([value.int(1)])
  |> db.all(conn, database.query)
}
```

Further documentation can be found at <https://hexdocs.pm/based>.

## Development

```sh
gleam run   # Run the project
gleam test  # Run the tests
gleam shell # Run an Erlang shell
```
