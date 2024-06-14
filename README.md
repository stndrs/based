# based

[![Package Version](https://img.shields.io/hexpm/v/based)](https://hex.pm/packages/based)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/based/)

This package provides a unified interface for interacting SQL databases. With
`based`, gleam code can remain mostly database-agnostic. SQL queries will need
to be written for your chosen database (postgres, sqlite, etc), but execution
of those queries isn't tied to your chosen database.

Packages for different database backends can conform to the type and function
definitions in this package. This can allow developers to quickly get started
writing gleam programs using a sqlite backend, but change to postgres when
needed without a large refactor.

## Example packages that can be used with `based`

- [`based_pg`](https://github.com/stndrs/based_pg)
- [`based_sqlite`](https://github.com/stndrs/based_sqlite)

```sh
gleam add based
```

```gleam
import based
import dynamic
import based_pg
// import based_sqlite

const sql = "SELECT name FROM users WHERE id=$1;"

pub type User {
  User(name: String)
}

pub fn main() {
  let config = load_config()

  use db <- based.register(based_pg.adapter(config))
  // use db <- based.register(based_sqlite.adapter(config))

  let decoder = dynamic.decode1(User, dynamic.element(0, dynamic.string))

  // Swapping out the backend doesn't require rewriting the existing queries
  based.new_query(sql)
  |> based.with_values([based.int(1)])
  |> based.one(db, decoder)
}
```

Further documentation can be found at <https://hexdocs.pm/based>.

## Development

```sh
gleam run   # Run the project
gleam test  # Run the tests
gleam shell # Run an Erlang shell
```
