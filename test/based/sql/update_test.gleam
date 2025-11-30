import based/sql
import based/sql/select
import based/sql/update
import based/value
import gleeunit/should

pub fn basic_update_test() {
  let expected = "UPDATE users SET name = ? WHERE id = ?"
  let users = sql.name("users") |> sql.table

  let query =
    update.table(users)
    |> update.set("name", sql.value("John", of: value.text))
    |> update.where([
      sql.name("id")
      |> sql.column
      |> sql.eq(sql.value(1, of: value.int)),
    ])
    |> update.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("John"), value.int(1)])
}

pub fn update_multiple_columns_test() {
  let expected = "UPDATE users SET name = ?, email = ? WHERE id = ?"
  let users = sql.name("users") |> sql.table

  let query =
    update.table(users)
    |> update.set("name", sql.value("John", of: value.text))
    |> update.set("email", sql.value("john@example.com", of: value.text))
    |> update.where([
      sql.name("id")
      |> sql.column
      |> sql.eq(sql.value(1, of: value.int)),
    ])
    |> update.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values
  |> should.equal([
    value.text("John"),
    value.text("john@example.com"),
    value.int(1),
  ])
}

pub fn update_with_where_not_test() {
  let expected = "UPDATE users SET active = ? WHERE NOT id = ?"
  let users = sql.name("users") |> sql.table

  let query =
    update.table(users)
    |> update.set("active", sql.value(True, value.bool))
    |> update.where_not([
      sql.name("id")
      |> sql.column
      |> sql.eq(sql.value(1, of: value.int)),
    ])
    |> update.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.true, value.int(1)])
}

pub fn update_returning_test() {
  let expected = "UPDATE users SET name = ? WHERE id = ? RETURNING id, name"
  let users = sql.name("users") |> sql.table

  let query =
    update.table(users)
    |> update.set("name", sql.value("John", of: value.text))
    |> update.where([
      sql.name("id")
      |> sql.column
      |> sql.eq(sql.value(1, of: value.int)),
    ])
    |> update.returning(["id", "name"])
    |> update.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("John"), value.int(1)])
}

pub fn update_with_is_test() {
  let expected = "UPDATE products SET price = ? WHERE is_deleted IS ?"
  let products = sql.name("products") |> sql.table

  let query =
    update.table(products)
    |> update.set("price", sql.value(19.99, of: value.float))
    |> update.where([
      sql.name("is_deleted")
      |> sql.column
      |> sql.is(sql.value(False, value.bool)),
    ])
    |> update.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.float(19.99), value.false])
}

pub fn update_set_from_subquery_test() {
  let expected =
    "UPDATE products SET price = (SELECT price FROM prices WHERE id = ?)"
  let products = sql.name("products") |> sql.table
  let prices = sql.name("prices") |> sql.table

  let price_id =
    select.from(prices)
    |> select.columns(["price"])
    |> select.where([
      sql.name("id")
      |> sql.column
      |> sql.eq(sql.value(1, of: value.int)),
    ])
    |> select.to_subquery(value.format())

  let query =
    update.table(products)
    |> update.set("price", price_id)
    |> update.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(1)])
}

pub fn update_with_is_to_string_test() {
  let expected = "UPDATE products SET price = 19.99 WHERE is_deleted IS FALSE"
  let products = sql.name("products") |> sql.table

  let query =
    update.table(products)
    |> update.set("price", sql.value(19.99, of: value.float))
    |> update.where([
      sql.name("is_deleted")
      |> sql.column
      |> sql.is(sql.value(False, value.bool)),
    ])

  update.to_string(query, value.format())
  |> should.equal(expected)
}
