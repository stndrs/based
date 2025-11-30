import based/sql
import based/sql/select
import based/sql/table
import based/sql/update
import based/value
import gleeunit/should

pub fn basic_update_test() {
  let expected = "UPDATE users SET name = ? WHERE id = ?"
  let users = table.new("users")

  let query =
    sql.update(users)
    |> update.set("name", sql.text("John"))
    |> update.where([sql.column("id") |> sql.eq(sql.int(1))])
    |> update.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("John"), value.int(1)])
}

pub fn update_multiple_columns_test() {
  let expected = "UPDATE users SET name = ?, email = ? WHERE id = ?"
  let users = table.new("users")

  let query =
    sql.update(users)
    |> update.set("name", sql.text("John"))
    |> update.set("email", sql.text("john@example.com"))
    |> update.where([sql.column("id") |> sql.eq(sql.int(1))])
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
  let users = table.new("users")

  let query =
    sql.update(users)
    |> update.set("active", sql.true)
    |> update.where_not([sql.column("id") |> sql.eq(sql.int(1))])
    |> update.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.true, value.int(1)])
}

pub fn update_returning_test() {
  let expected = "UPDATE users SET name = ? WHERE id = ? RETURNING id, name"
  let users = table.new("users")

  let query =
    sql.update(users)
    |> update.set("name", sql.text("John"))
    |> update.where([sql.column("id") |> sql.eq(sql.int(1))])
    |> update.returning(["id", "name"])
    |> update.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("John"), value.int(1)])
}

pub fn update_with_is_test() {
  let expected = "UPDATE products SET price = ? WHERE is_deleted IS ?"
  let products = table.new("products")

  let query =
    sql.update(products)
    |> update.set("price", sql.float(19.99))
    |> update.where([sql.column("is_deleted") |> sql.is(sql.false)])
    |> update.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.float(19.99), value.false])
}

pub fn update_set_from_subquery_test() {
  let expected =
    "UPDATE products SET price = (SELECT price FROM prices WHERE id = ?)"
  let products = table.new("products")
  let prices = table.new("prices")

  let price_id =
    sql.select(["price"])
    |> select.from(prices)
    |> select.where([sql.column("id") |> sql.eq(sql.int(1))])
    |> select.to_subquery(value.format())

  let query =
    sql.update(products)
    |> update.set("price", price_id)
    |> update.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(1)])
}

pub fn update_with_is_to_string_test() {
  let expected = "UPDATE products SET price = 19.99 WHERE is_deleted IS FALSE"
  let products = table.new("products")

  let query =
    sql.update(products)
    |> update.set("price", sql.float(19.99))
    |> update.where([sql.column("is_deleted") |> sql.is(sql.false)])

  update.to_string(query, value.format())
  |> should.equal(expected)
}
