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

Build type-safe SQL queries using the `based/sql` modules:

```gleam
import based/db
import based/repo
import based/sql
import based/sql/select

let users = sql.table("users")

let query =
  repo.default()
  |> select.from(users)
  |> select.columns([sql.column("id"), sql.column("name")])
  |> select.where([sql.eq(sql.column("id"), db.int(1), of: sql.val)])
  |> select.to_query

// query.sql == "SELECT id, name FROM users WHERE id = ?"
// query.values == [db.int(1)]
```

### Insert

```gleam
import based/db
import based/repo
import based/sql
import based/sql/insert

let users = sql.table("users")

let query =
  repo.default()
  |> insert.into(users)
  |> insert.values([
    {
      use <- insert.value("name", db.text("John"))
      insert.final("email", db.text("john@example.com"))
    },
  ])
  |> insert.to_query

// query.sql == "INSERT INTO users (name, email) VALUES (?, ?)"
```

### Update

```gleam
import based/db
import based/repo
import based/sql
import based/sql/update

let users = sql.table("users")

let query =
  repo.default()
  |> update.table(users)
  |> update.set("name", db.text("Jane"), of: sql.val)
  |> update.where([sql.eq(sql.column("id"), db.int(1), of: sql.val)])
  |> update.to_query

// query.sql == "UPDATE users SET name = ? WHERE id = ?"
```

### Delete

```gleam
import based/db
import based/repo
import based/sql
import based/sql/delete

let users = sql.table("users")

let query =
  repo.default()
  |> delete.from(users)
  |> delete.where([sql.eq(sql.column("id"), db.int(1), of: sql.val)])
  |> delete.to_query

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

// `database` is a hypothetical adapter package
let assert Ok(users) = db.all(query, database, user_decoder)
```

## Adapter Configuration

Adapter packages configure a `Repo` to control placeholder formatting,
identifier escaping, and value serialization:

```gleam
import based/repo
import gleam/int

// PostgreSQL-style
let pg_repo =
  repo.new()
  |> repo.on_placeholder(fn(index) { "$" <> int.to_string(index) })
  |> repo.on_identifier(fn(ident) { "\"" <> ident <> "\"" })
  |> repo.on_value(value_to_string)
  |> repo.on_text(db.text)
  |> repo.on_null(fn() { db.null })
```

Further documentation can be found at <https://hexdocs.pm/based>.

## Development

```sh
gleam test  # Run the tests
```
