import based/sql
import based/sql/insert
import based/value
import gleeunit/should

pub fn basic_insert_test() {
  let expected = "INSERT INTO users (name, email) VALUES (?, ?)"
  let users = sql.identifier("users") |> sql.table

  let query =
    value.sql()
    |> insert.into(users)
    |> insert.columns(["name", "email"])
    |> insert.values([
      [value.text("John"), value.text("john@example.com")],
    ])
    |> insert.to_query

  query.sql |> should.equal(expected)
  query.values
  |> should.equal([value.text("John"), value.text("john@example.com")])
}

pub fn insert_multiple_columns_test() {
  let expected =
    "INSERT INTO users (name, email, age, active) VALUES (?, ?, ?, ?)"
  let users = sql.identifier("users") |> sql.table

  let query =
    value.sql()
    |> insert.into(users)
    |> insert.columns(["name", "email", "age", "active"])
    |> insert.values([
      [
        value.text("John"),
        value.text("john@example.com"),
        value.int(30),
        value.bool(True),
      ],
    ])
    |> insert.to_query

  query.sql |> should.equal(expected)
  query.values
  |> should.equal([
    value.text("John"),
    value.text("john@example.com"),
    value.int(30),
    value.true,
  ])
}

pub fn insert_returning_test() {
  let expected = "INSERT INTO users (name) VALUES (?) RETURNING id, name"
  let users = sql.identifier("users") |> sql.table

  let query =
    value.sql()
    |> insert.into(users)
    |> insert.columns(["name"])
    |> insert.values([[value.text("John")]])
    |> insert.returning(["id", "name"])
    |> insert.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("John")])
}

pub fn insert_to_string_test() {
  let expected =
    "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')"
  let users = sql.identifier("users") |> sql.table

  let query =
    value.sql()
    |> insert.into(users)
    |> insert.columns(["name", "email"])
    |> insert.values([
      [
        value.text("John"),
        value.text("john@example.com"),
      ],
    ])

  insert.to_string(query) |> should.equal(expected)
}

pub fn insert_multiple_rows_test() {
  let expected = "INSERT INTO users (name, email) VALUES (?, ?), (?, ?)"
  let users = sql.identifier("users") |> sql.table

  let query =
    value.sql()
    |> insert.into(users)
    |> insert.columns(["name", "email"])
    |> insert.values([
      [
        value.text("John"),
        value.text("john@example.com"),
      ],
      [
        value.text("Jane"),
        value.text("jane@example.com"),
      ],
    ])
    |> insert.to_query

  query.sql |> should.equal(expected)
  query.values
  |> should.equal([
    value.text("John"),
    value.text("john@example.com"),
    value.text("Jane"),
    value.text("jane@example.com"),
  ])
}

pub fn insert_with_null_test() {
  let expected = "INSERT INTO users (name, middle_name) VALUES (?, ?)"
  let users = sql.identifier("users") |> sql.table

  let query =
    value.sql()
    |> insert.into(users)
    |> insert.columns(["name", "middle_name"])
    |> insert.values([
      [value.text("John"), value.null],
    ])
    |> insert.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("John"), value.null])
}

pub fn insert_with_different_value_types_test() {
  let expected =
    "INSERT INTO products (id, price, is_active, description) VALUES (?, ?, ?, ?)"
  let products = sql.identifier("products") |> sql.table

  let query =
    value.sql()
    |> insert.into(products)
    |> insert.columns(["id", "price", "is_active", "description"])
    |> insert.values([
      [
        value.int(123),
        value.float(19.99),
        value.true,
        value.null,
      ],
    ])
    |> insert.to_query

  query.sql
  |> should.equal(expected)

  query.values
  |> should.equal([
    value.int(123),
    value.float(19.99),
    value.true,
    value.null,
  ])
}
