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

pub fn update_with_like_test() {
  let expected = "UPDATE users SET active = ? WHERE name LIKE ?"
  let users = sql.table("users")

  let query =
    repo.default()
    |> update.table(users)
    |> update.set("active", db.true, of: sql.val)
    |> update.where([sql.column("name") |> sql.like("%John%")])
    |> update.to_query

  assert expected == query.sql
  assert [db.true, db.text("%John%")] == query.values
}

pub fn update_with_not_like_test() {
  let expected = "UPDATE users SET active = ? WHERE name NOT LIKE ?"
  let users = sql.table("users")

  let query =
    repo.default()
    |> update.table(users)
    |> update.set("active", db.true, of: sql.val)
    |> update.where([sql.column("name") |> sql.not_like("%admin%")])
    |> update.to_query

  assert expected == query.sql
  assert [db.true, db.text("%admin%")] == query.values
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

pub fn update_with_chained_where_test() {
  let expected = "UPDATE users SET name = ? WHERE id = ? AND active IS TRUE"
  let users = sql.table("users")

  let query =
    repo.default()
    |> update.table(users)
    |> update.set("name", db.text("John"), of: sql.val)
    |> update.where([
      sql.column("id")
      |> sql.eq(db.int(1), of: sql.val),
    ])
    |> update.where([
      sql.column("active")
      |> column.is(True),
    ])
    |> update.to_query

  assert expected == query.sql
  assert [db.text("John"), db.int(1)] == query.values
}

pub fn update_with_chained_where_to_string_test() {
  let expected =
    "UPDATE users SET name = 'John' WHERE id = 1 AND active IS TRUE"
  let users = sql.table("users")

  let query =
    repo.default()
    |> update.table(users)
    |> update.set("name", db.text("John"), of: sql.val)
    |> update.where([
      sql.column("id")
      |> sql.eq(db.int(1), of: sql.val),
    ])
    |> update.where([
      sql.column("active")
      |> column.is(True),
    ])

  assert expected == update.to_string(query)
}

pub fn update_with_order_by_asc_test() {
  let expected =
    "UPDATE users SET active = ? WHERE active IS TRUE ORDER BY id ASC"
  let users = sql.table("users")

  let query =
    repo.default()
    |> update.table(users)
    |> update.set("active", db.false, of: sql.val)
    |> update.where([sql.column("active") |> column.is(True)])
    |> update.order_by(["id"])
    |> update.asc
    |> update.to_query

  assert expected == query.sql
  assert [db.false] == query.values
}

pub fn update_with_order_by_desc_test() {
  let expected =
    "UPDATE users SET active = ? WHERE active IS TRUE ORDER BY created_at DESC"
  let users = sql.table("users")

  let query =
    repo.default()
    |> update.table(users)
    |> update.set("active", db.false, of: sql.val)
    |> update.where([sql.column("active") |> column.is(True)])
    |> update.order_by(["created_at"])
    |> update.desc
    |> update.to_query

  assert expected == query.sql
  assert [db.false] == query.values
}

pub fn update_with_limit_test() {
  let expected = "UPDATE users SET active = ? WHERE active IS TRUE LIMIT ?"
  let users = sql.table("users")

  let query =
    repo.default()
    |> update.table(users)
    |> update.set("active", db.false, of: sql.val)
    |> update.where([sql.column("active") |> column.is(True)])
    |> update.limit(10)
    |> update.to_query

  assert expected == query.sql
  assert [db.false, db.int(10)] == query.values
}

pub fn update_with_order_by_and_limit_test() {
  let expected =
    "UPDATE users SET active = ? WHERE active IS TRUE ORDER BY id ASC LIMIT ?"
  let users = sql.table("users")

  let query =
    repo.default()
    |> update.table(users)
    |> update.set("active", db.false, of: sql.val)
    |> update.where([sql.column("active") |> column.is(True)])
    |> update.order_by(["id"])
    |> update.asc
    |> update.limit(100)
    |> update.to_query

  assert expected == query.sql
  assert [db.false, db.int(100)] == query.values
}

pub fn update_with_limit_and_offset_test() {
  let expected =
    "UPDATE users SET active = ? WHERE active IS TRUE LIMIT ? OFFSET ?"
  let users = sql.table("users")

  let query =
    repo.default()
    |> update.table(users)
    |> update.set("active", db.false, of: sql.val)
    |> update.where([sql.column("active") |> column.is(True)])
    |> update.limit(10)
    |> update.offset(20)
    |> update.to_query

  assert expected == query.sql
  assert [db.false, db.int(10), db.int(20)] == query.values
}

pub fn update_with_order_by_limit_returning_test() {
  let expected =
    "UPDATE users SET active = ? WHERE active IS TRUE ORDER BY id ASC LIMIT ? RETURNING id"
  let users = sql.table("users")

  let query =
    repo.default()
    |> update.table(users)
    |> update.set("active", db.false, of: sql.val)
    |> update.where([sql.column("active") |> column.is(True)])
    |> update.order_by(["id"])
    |> update.asc
    |> update.limit(5)
    |> update.returning([sql.column("id")])
    |> update.to_query

  assert expected == query.sql
  assert [db.false, db.int(5)] == query.values
}

pub fn update_with_limit_to_string_test() {
  let expected =
    "UPDATE users SET active = FALSE WHERE active IS TRUE ORDER BY id DESC LIMIT 10"
  let users = sql.table("users")

  let result =
    repo.default()
    |> update.table(users)
    |> update.set("active", db.false, of: sql.val)
    |> update.where([sql.column("active") |> column.is(True)])
    |> update.order_by(["id"])
    |> update.desc
    |> update.limit(10)
    |> update.to_string

  assert expected == result
}

pub fn update_offset_without_limit_test() {
  let expected = "UPDATE users SET active = ? WHERE active IS TRUE OFFSET ?"
  let users = sql.table("users")

  let query =
    repo.default()
    |> update.table(users)
    |> update.set("active", db.false, of: sql.val)
    |> update.where([sql.column("active") |> column.is(True)])
    |> update.offset(10)
    |> update.to_query

  assert expected == query.sql
  assert [db.false, db.int(10)] == query.values
}

pub fn update_offset_before_limit_test() {
  let expected =
    "UPDATE users SET active = ? WHERE active IS TRUE LIMIT ? OFFSET ?"
  let users = sql.table("users")

  let query =
    repo.default()
    |> update.table(users)
    |> update.set("active", db.false, of: sql.val)
    |> update.where([sql.column("active") |> column.is(True)])
    |> update.offset(50)
    |> update.limit(10)
    |> update.to_query

  assert expected == query.sql
  assert [db.false, db.int(10), db.int(50)] == query.values
}
