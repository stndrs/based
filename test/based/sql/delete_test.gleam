import based/db
import based/repo
import based/sql
import based/sql/delete
import gleeunit/should

pub fn basic_delete_test() {
  let expected = "DELETE FROM users WHERE id = ?"
  let users = sql.table("users")

  let query =
    repo.default()
    |> delete.from(users)
    |> delete.where([
      sql.column("id")
      |> sql.eq(db.int(1), of: sql.val),
    ])
    |> delete.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([db.int(1)])
}

pub fn delete_with_where_not_test() {
  let expected = "DELETE FROM users WHERE NOT id = ?"
  let users = sql.table("users")

  let query =
    repo.default()
    |> delete.from(users)
    |> delete.where_not([
      sql.column("id")
      |> sql.eq(db.int(1), of: sql.val),
    ])
    |> delete.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([db.int(1)])
}

pub fn delete_with_multiple_conditions_test() {
  let expected = "DELETE FROM users WHERE id = ? AND created_at < ?"
  let users = sql.table("users")

  let query =
    repo.default()
    |> delete.from(users)
    |> delete.where([
      sql.column("id")
        |> sql.eq(db.int(1), of: sql.val),
      sql.column("created_at")
        |> sql.lt(db.text("2024-01-01"), of: sql.val),
    ])
    |> delete.to_query

  query.sql |> should.equal(expected)
  query.values
  |> should.equal([db.int(1), db.text("2024-01-01")])
}

pub fn delete_returning_test() {
  let expected = "DELETE FROM users WHERE id = ? RETURNING id, name"
  let users = sql.table("users")

  let query =
    repo.default()
    |> delete.from(users)
    |> delete.where([
      sql.column("id")
      |> sql.eq(db.int(1), of: sql.val),
    ])
    |> delete.returning([sql.column("id"), sql.column("name")])
    |> delete.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([db.int(1)])
}

pub fn delete_with_like_test() {
  let expected = "DELETE FROM users WHERE name LIKE ?"
  let users = sql.table("users")

  let query =
    repo.default()
    |> delete.from(users)
    |> delete.where([sql.column("name") |> sql.like("%John%")])
    |> delete.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([db.text("%John%")])
}

pub fn delete_with_not_like_test() {
  let expected = "DELETE FROM users WHERE name NOT LIKE ?"
  let users = sql.table("users")

  let query =
    repo.default()
    |> delete.from(users)
    |> delete.where([sql.column("name") |> sql.not_like("%admin%")])
    |> delete.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([db.text("%admin%")])
}

pub fn delete_to_string_test() {
  let expected = "DELETE FROM users WHERE id = 1 AND created_at < '2024-01-01'"
  let users = sql.table("users")

  let query =
    repo.default()
    |> delete.from(users)
    |> delete.where([
      sql.column("id")
        |> sql.eq(db.int(1), of: sql.val),
      sql.column("created_at")
        |> sql.lt(db.text("2024-01-01"), of: sql.val),
    ])

  delete.to_string(query) |> should.equal(expected)
}
