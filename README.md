# based

[![Package Version](https://img.shields.io/hexpm/v/based)](https://hex.pm/packages/based)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/based/)

Database-agnostic types and a composable SQL query builder for Gleam.

`based` provides a unified interface for interacting with SQL databases.
Adapter packages for specific database backends (e.g. PostgreSQL, MariaDB)
conform to the types and functions defined here.

```sh
gleam add based
```

## Query Builder

Build type-safe SQL queries using `based/sql`:

```gleam
import database as db
import based/sql

let query =
  sql.from(sql.table("users"))
  |> sql.select([sql.column("id"), sql.column("name")])
  |> sql.where([sql.column("id") |> sql.eq(db.int(1), of: sql.val)])
  |> sql.to_query(adapter)

// query.sql == "SELECT id, name FROM users WHERE id = ?"
// query.values == [db.Int(1)]
```

### Insert

```gleam
import based/sql

let inserter =
  sql.rows([#("John", "john@example.com")])
  |> sql.value("name", fn(user) { db.text(user.0) })
  |> sql.value("email", fn(user) { db.text(user.1) })

let query =
  sql.insert(into: sql.table("users"))
  |> sql.values(inserter)
  |> sql.to_query(adapter)

// query.sql == "INSERT INTO users (name, email) VALUES (?, ?)"
```

### Update

```gleam
import based/sql

let query =
  sql.table("users")
  |> sql.update([sql.set("name", db.text("Jane"), of: sql.val)])
  |> sql.where([sql.column("id") |> sql.eq(db.int(1), of: sql.val)])
  |> sql.to_query(adapter)

// query.sql == "UPDATE users SET name = ? WHERE id = ?"
```

### Delete

```gleam
import based/sql

let query =
  sql.from(sql.table("users"))
  |> sql.delete()
  |> sql.where([sql.column("id") |> sql.eq(db.int(1), of: sql.val)])
  |> sql.to_query(adapter)

// query.sql == "DELETE FROM users WHERE id = ?"
```

## Running Queries

Use `based.query`, `based.all`, or `based.one` with a configured `Db` from an
adapter package:

```gleam
import based
import gleam/dynamic/decode

let user_decoder = {
  use id <- decode.field("id", decode.int)
  use name <- decode.field("name", decode.string)

  decode.success(User(id:, name:))
}

// `database` is provided by an adapter package
let assert Ok(users) = based.all(query, database, user_decoder)
```

## Custom Adapters

Adapter packages configure an `sql.Adapter` to control placeholder formatting,
identifier escaping, and value type mapping:

```gleam
import based/sql
import gleam/int

let mysql_adapter =
  sql.adapter()
  |> sql.on_placeholder(with: fn(_) { "?" })
  |> sql.on_identifier(with: fn(name) { "`" <> name <> "`" })
  |> sql.on_value(with: my_value_to_string)
  |> sql.on_null(with: fn() { MyNull })
  |> sql.on_int(with: fn(i) { MyInt(i) })
  |> sql.on_text(with: fn(s) { MyText(s) })
```

Further documentation can be found at <https://hexdocs.pm/based>.

## Development

```sh
gleam test  # Run the tests
```
