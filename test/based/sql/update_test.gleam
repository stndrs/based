import based/sql
import based/sql/select
import based/sql/update
import based/value
import gleeunit/should

pub fn basic_update_test() {
  let expected = "UPDATE users SET name = ? WHERE id = ?"
  let users = sql.identifier("users")

  let query =
    update.table(sql.table(users))
    |> update.set("name", sql.value(value.text("John")))
    |> update.where([
      sql.identifier("id")
      |> sql.column
      |> sql.eq(sql.value(value.int(1))),
    ])
    |> update.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("John"), value.int(1)])
}

pub fn update_multiple_columns_test() {
  let expected = "UPDATE users SET name = ?, email = ? WHERE id = ?"
  let users = sql.identifier("users")

  let query =
    update.table(sql.table(users))
    |> update.set("name", sql.value(value.text("John")))
    |> update.set("email", sql.value(value.text("john@example.com")))
    |> update.where([
      sql.identifier("id")
      |> sql.column
      |> sql.eq(sql.value(value.int(1))),
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
  let users = sql.identifier("users")

  let query =
    update.table(sql.table(users))
    |> update.set("active", sql.value(value.true))
    |> update.where_not([
      sql.identifier("id")
      |> sql.column
      |> sql.eq(sql.value(value.int(1))),
    ])
    |> update.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.true, value.int(1)])
}

pub fn update_returning_test() {
  let expected = "UPDATE users SET name = ? WHERE id = ? RETURNING id, name"
  let users = sql.identifier("users")

  let query =
    update.table(sql.table(users))
    |> update.set("name", sql.value(value.text("John")))
    |> update.where([
      sql.identifier("id")
      |> sql.column
      |> sql.eq(sql.value(value.int(1))),
    ])
    |> update.returning(["id", "name"])
    |> update.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("John"), value.int(1)])
}

pub fn update_with_is_test() {
  let expected = "UPDATE products SET price = ? WHERE is_deleted IS FALSE"
  let products = sql.identifier("products")

  let query =
    update.table(sql.table(products))
    |> update.set("price", sql.value(value.float(19.99)))
    |> update.where([
      sql.identifier("is_deleted")
      |> sql.column
      |> sql.is(False),
    ])
    |> update.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.float(19.99)])
}

pub fn update_set_from_subquery_test() {
  let expected =
    "UPDATE products SET price = (SELECT price FROM prices WHERE id = ?)"
  let products = sql.identifier("products")
  let prices = sql.identifier("prices")

  let price_id =
    select.from(prices, of: sql.table)
    |> select.columns(["price"])
    |> select.where([
      sql.identifier("id")
      |> sql.column
      |> sql.eq(sql.value(value.int(1))),
    ])
    |> select.to_subquery(value.format())

  let query =
    update.table(sql.table(products))
    |> update.set("price", price_id)
    |> update.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(1)])
}

pub fn update_with_is_to_string_test() {
  let expected = "UPDATE products SET price = 19.99 WHERE is_deleted IS FALSE"
  let products = sql.identifier("products")

  let query =
    update.table(sql.table(products))
    |> update.set("price", sql.value(value.float(19.99)))
    |> update.where([
      sql.identifier("is_deleted")
      |> sql.column
      |> sql.is(False),
    ])

  update.to_string(query, value.format())
  |> should.equal(expected)
}
