import based/sql
import based/sql/insert
import based/sql/table
import based/value
import gleeunit/should

pub fn basic_insert_test() {
  let expected = "INSERT INTO users (name, email) VALUES (?, ?)"
  let users = table.new("users")

  let query =
    sql.insert(users)
    |> insert.columns(["name", "email"])
    |> insert.values([[sql.text("John"), sql.text("john@example.com")]])
    |> insert.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values
  |> should.equal([value.text("John"), value.text("john@example.com")])
}

pub fn insert_multiple_columns_test() {
  let expected =
    "INSERT INTO users (name, email, age, active) VALUES (?, ?, ?, ?)"
  let users = table.new("users")

  let query =
    sql.insert(users)
    |> insert.columns(["name", "email", "age", "active"])
    |> insert.values([
      [sql.text("John"), sql.text("john@example.com"), sql.int(30), sql.true],
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
  let users = table.new("users")

  let query =
    sql.insert(users)
    |> insert.columns(["name"])
    |> insert.values([[sql.text("John")]])
    |> insert.returning(["id", "name"])
    |> insert.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("John")])
}

pub fn insert_to_string_test() {
  let expected =
    "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')"
  let users = table.new("users")

  let query =
    sql.insert(users)
    |> insert.columns(["name", "email"])
    |> insert.values([[sql.text("John"), sql.text("john@example.com")]])

  insert.to_string(query, value.format()) |> should.equal(expected)
}

pub fn insert_multiple_rows_test() {
  let expected = "INSERT INTO users (name, email) VALUES (?, ?), (?, ?)"
  let users = table.new("users")

  let query =
    sql.insert(users)
    |> insert.columns(["name", "email"])
    |> insert.values([
      [sql.text("John"), sql.text("john@example.com")],
      [sql.text("Jane"), sql.text("jane@example.com")],
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
  let users = table.new("users")

  let query =
    sql.insert(users)
    |> insert.columns(["name", "middle_name"])
    |> insert.values([[sql.text("John"), sql.null]])
    |> insert.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("John"), value.null])
}

pub fn insert_with_different_value_types_test() {
  let expected =
    "INSERT INTO products (id, price, is_active, description) VALUES (?, ?, ?, ?)"
  let products = table.new("products")

  let query =
    sql.insert(products)
    |> insert.columns(["id", "price", "is_active", "description"])
    |> insert.values([[sql.int(123), sql.float(19.99), sql.true, sql.null]])
    |> insert.to_query(value.format())

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
