import based/db
import based/repo
import based/sql
import based/sql/column
import based/sql/select
import based/sql/update

pub fn basic_update_test() {
  let expected = "UPDATE users SET name = ? WHERE id = ?"
  let users = sql.table("users")

  let query =
    repo.default()
    |> update.table(users)
    |> update.set("name", db.text("John"), of: sql.val)
    |> update.where([
      sql.column("id")
      |> sql.eq(db.int(1), of: sql.val),
    ])
    |> update.to_query

  assert expected == query.sql
  assert [db.text("John"), db.int(1)] == query.values
}

pub fn update_multiple_columns_test() {
  let expected = "UPDATE users SET name = ?, email = ? WHERE id = ?"
  let users = sql.table("users")

  let query =
    repo.default()
    |> update.table(users)
    |> update.set("name", db.text("John"), of: sql.val)
    |> update.set("email", db.text("john@example.com"), of: sql.val)
    |> update.where([
      sql.column("id")
      |> sql.eq(db.int(1), of: sql.val),
    ])
    |> update.to_query

  assert expected == query.sql
  assert [db.text("John"), db.text("john@example.com"), db.int(1)]
    == query.values
}

pub fn update_with_where_not_test() {
  let expected = "UPDATE users SET active = ? WHERE NOT id = ?"
  let users = sql.table("users")

  let query =
    repo.default()
    |> update.table(users)
    |> update.set("active", db.true, of: sql.val)
    |> update.where_not([
      sql.column("id")
      |> sql.eq(db.int(1), of: sql.val),
    ])
    |> update.to_query

  assert expected == query.sql
  assert [db.true, db.int(1)] == query.values
}

pub fn update_returning_test() {
  let expected = "UPDATE users SET name = ? WHERE id = ? RETURNING id, name"
  let users = sql.table("users")

  let query =
    repo.default()
    |> update.table(users)
    |> update.set("name", db.text("John"), of: sql.val)
    |> update.where([
      sql.column("id")
      |> sql.eq(db.int(1), of: sql.val),
    ])
    |> update.returning([sql.column("id"), sql.column("name")])
    |> update.to_query

  assert expected == query.sql
  assert [db.text("John"), db.int(1)] == query.values
}

pub fn update_with_is_test() {
  let expected = "UPDATE products SET price = ? WHERE is_deleted IS FALSE"
  let products = sql.table("products")

  let query =
    repo.default()
    |> update.table(products)
    |> update.set("price", db.float(19.99), of: sql.val)
    |> update.where([
      sql.column("is_deleted")
      |> column.is(False),
    ])
    |> update.to_query

  assert expected == query.sql
  assert [db.float(19.99)] == query.values
}

pub fn update_set_from_subquery_test() {
  let expected =
    "UPDATE products SET price = (SELECT price FROM prices WHERE id = ?)"
  let products = sql.table("products")
  let prices = sql.table("prices")

  let price_id =
    repo.default()
    |> select.from(prices)
    |> select.columns([sql.column("price")])
    |> select.where([
      sql.column("id")
      |> sql.eq(db.int(1), of: sql.val),
    ])

  let query =
    repo.default()
    |> update.table(products)
    |> update.set("price", price_id, of: select.subquery)
    |> update.to_query

  assert expected == query.sql
  assert [db.int(1)] == query.values
}

pub fn update_with_is_to_string_test() {
  let expected = "UPDATE products SET price = 19.99 WHERE is_deleted IS FALSE"
  let products = sql.table("products")

  let query =
    repo.default()
    |> update.table(products)
    |> update.set("price", db.float(19.99), of: sql.val)
    |> update.where([
      sql.column("is_deleted")
      |> column.is(False),
    ])

  assert expected == update.to_string(query)
}
