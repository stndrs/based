import based/sql
import based/sql/insert
import based/value
import gleeunit/should

pub fn basic_insert_test() {
  let expected = "INSERT INTO users (name, email) VALUES (?, ?)"
  let users = sql.name("users") |> sql.table

  let query =
    insert.into(users)
    |> insert.columns(["name", "email"])
    |> insert.values([
      [
        sql.value("John", of: value.text),
        sql.value("john@example.com", of: value.text),
      ],
    ])
    |> insert.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values
  |> should.equal([value.text("John"), value.text("john@example.com")])
}

pub fn insert_multiple_columns_test() {
  let expected =
    "INSERT INTO users (name, email, age, active) VALUES (?, ?, ?, ?)"
  let users = sql.name("users") |> sql.table

  let query =
    insert.into(users)
    |> insert.columns(["name", "email", "age", "active"])
    |> insert.values([
      [
        sql.value("John", of: value.text),
        sql.value("john@example.com", of: value.text),
        sql.value(30, of: value.int),
        sql.value(True, of: value.bool),
      ],
    ])
    |> insert.to_query(value.format())

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
  let users = sql.name("users") |> sql.table

  let query =
    insert.into(users)
    |> insert.columns(["name"])
    |> insert.values([[sql.value("John", of: value.text)]])
    |> insert.returning(["id", "name"])
    |> insert.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("John")])
}

pub fn insert_to_string_test() {
  let expected =
    "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')"
  let users = sql.name("users") |> sql.table

  let query =
    insert.into(users)
    |> insert.columns(["name", "email"])
    |> insert.values([
      [
        sql.value("John", of: value.text),
        sql.value("john@example.com", of: value.text),
      ],
    ])

  insert.to_string(query, value.format()) |> should.equal(expected)
}

pub fn insert_multiple_rows_test() {
  let expected = "INSERT INTO users (name, email) VALUES (?, ?), (?, ?)"
  let users = sql.name("users") |> sql.table

  let query =
    insert.into(users)
    |> insert.columns(["name", "email"])
    |> insert.values([
      [
        sql.value("John", of: value.text),
        sql.value("john@example.com", of: value.text),
      ],
      [
        sql.value("Jane", of: value.text),
        sql.value("jane@example.com", of: value.text),
      ],
    ])
    |> insert.to_query(value.format())

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
  let users = sql.name("users") |> sql.table

  let query =
    insert.into(users)
    |> insert.columns(["name", "middle_name"])
    |> insert.values([
      [sql.value("John", of: value.text), sql.value(Nil, value.null)],
    ])
    |> insert.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("John"), value.null(Nil)])
}

pub fn insert_with_different_value_types_test() {
  let expected =
    "INSERT INTO products (id, price, is_active, description) VALUES (?, ?, ?, ?)"
  let products = sql.name("products") |> sql.table

  let query =
    insert.into(products)
    |> insert.columns(["id", "price", "is_active", "description"])
    |> insert.values([
      [
        sql.value(123, of: value.int),
        sql.value(19.99, of: value.float),
        sql.value(True, value.bool),
        sql.value(Nil, of: value.null),
      ],
    ])
    |> insert.to_query(value.format())

  query.sql
  |> should.equal(expected)

  query.values
  |> should.equal([
    value.int(123),
    value.float(19.99),
    value.true,
    value.null(Nil),
  ])
}
