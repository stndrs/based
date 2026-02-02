import based
import based/db
import based/sql
import based/sql/insert
import gleeunit/should

pub fn basic_insert_test() {
  let expected = "INSERT INTO users (name, email) VALUES (?, ?)"
  let users = sql.table("users")

  let query =
    based.default()
    |> insert.into(users)
    |> insert.columns(["name", "email"])
    |> insert.values([
      [db.text("John"), db.text("john@example.com")],
    ])
    |> insert.to_query

  query.sql |> should.equal(expected)
  query.values
  |> should.equal([db.text("John"), db.text("john@example.com")])
}

pub fn insert_multiple_columns_test() {
  let expected =
    "INSERT INTO users (name, email, age, active) VALUES (?, ?, ?, ?)"
  let users = sql.table("users")

  let query =
    based.default()
    |> insert.into(users)
    |> insert.columns(["name", "email", "age", "active"])
    |> insert.values([
      [
        db.text("John"),
        db.text("john@example.com"),
        db.int(30),
        db.bool(True),
      ],
    ])
    |> insert.to_query

  query.sql |> should.equal(expected)
  query.values
  |> should.equal([
    db.text("John"),
    db.text("john@example.com"),
    db.int(30),
    db.true,
  ])
}

pub fn insert_returning_test() {
  let expected = "INSERT INTO users (name) VALUES (?) RETURNING id, name"
  let users = sql.table("users")

  let query =
    based.default()
    |> insert.into(users)
    |> insert.columns(["name"])
    |> insert.values([[db.text("John")]])
    |> insert.returning([sql.column("id"), sql.column("name")])
    |> insert.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([db.text("John")])
}

pub fn insert_to_string_test() {
  let expected =
    "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')"
  let users = sql.table("users")

  let query =
    based.default()
    |> insert.into(users)
    |> insert.columns(["name", "email"])
    |> insert.values([
      [
        db.text("John"),
        db.text("john@example.com"),
      ],
    ])

  insert.to_string(query) |> should.equal(expected)
}

pub fn insert_multiple_rows_test() {
  let expected = "INSERT INTO users (name, email) VALUES (?, ?), (?, ?)"
  let users = sql.table("users")

  let query =
    based.default()
    |> insert.into(users)
    |> insert.columns(["name", "email"])
    |> insert.values([
      [
        db.text("John"),
        db.text("john@example.com"),
      ],
      [
        db.text("Jane"),
        db.text("jane@example.com"),
      ],
    ])
    |> insert.to_query

  query.sql |> should.equal(expected)
  query.values
  |> should.equal([
    db.text("John"),
    db.text("john@example.com"),
    db.text("Jane"),
    db.text("jane@example.com"),
  ])
}

pub fn insert_with_null_test() {
  let expected = "INSERT INTO users (name, middle_name) VALUES (?, ?)"
  let users = sql.table("users")

  let query =
    based.default()
    |> insert.into(users)
    |> insert.columns(["name", "middle_name"])
    |> insert.values([
      [db.text("John"), db.null],
    ])
    |> insert.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([db.text("John"), db.null])
}

pub fn insert_with_different_value_types_test() {
  let expected =
    "INSERT INTO products (id, price, is_active, description) VALUES (?, ?, ?, ?)"
  let products = sql.table("products")

  let query =
    based.default()
    |> insert.into(products)
    |> insert.columns(["id", "price", "is_active", "description"])
    |> insert.values([
      [
        db.int(123),
        db.float(19.99),
        db.true,
        db.null,
      ],
    ])
    |> insert.to_query

  query.sql
  |> should.equal(expected)

  query.values
  |> should.equal([
    db.int(123),
    db.float(19.99),
    db.true,
    db.null,
  ])
}
