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
import based/sql

let adapter = sql.adapter()

let query =
  sql.from(sql.table("users"))
  |> sql.select([sql.col("id"), sql.col("name")])
  |> sql.where([sql.col("id") |> sql.eq(sql.int(1), of: sql.value)])
  |> sql.to_query(adapter)

// query.sql == "SELECT id, name FROM users WHERE id = ?"
// query.values == [sql.Int(1)]
```

### Insert

```gleam
import based/sql

let query =
  sql.insert(into: sql.table("users"))
  |> sql.values([
    {
      use <- sql.field(column: "name", value: sql.text("John"))
      sql.final(column: "email", value: sql.text("john@example.com"))
    },
  ])
  |> sql.to_query(sql.adapter())

// query.sql == "INSERT INTO users (name, email) VALUES (?, ?)"
```

### Update

```gleam
import based/sql

let query =
  sql.update(table: sql.table("users"))
  |> sql.set("name", sql.text("Jane"), of: sql.value)
  |> sql.where([sql.col("id") |> sql.eq(sql.int(1), of: sql.value)])
  |> sql.to_query(sql.adapter())

// query.sql == "UPDATE users SET name = ? WHERE id = ?"
```

### Delete

```gleam
import based/sql

let query =
  sql.from(sql.table("users"))
  |> sql.delete()
  |> sql.where([sql.col("id") |> sql.eq(sql.int(1), of: sql.value)])
  |> sql.to_query(sql.adapter())

// query.sql == "DELETE FROM users WHERE id = ?"
```

## Running Queries

Use `db.query`, `db.all`, or `db.one` with a configured `Db` from an adapter package:

```gleam
import based/db
import gleam/dynamic/decode

let user_decoder = {
  use id <- decode.field("id", decode.int)
  use name <- decode.field("name", decode.string)

  decode.success(User(id:, name:))
}

// `database` is provided by an adapter package
let assert Ok(users) = db.all(query, database, user_decoder)
```

## Custom Adapters

Adapter packages configure an `sql.Adapter` to control placeholder formatting,
identifier escaping, and value type mapping:

```gleam
import based/sql
import gleam/int

let mysql_adapter =
  sql.new_adapter()
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
