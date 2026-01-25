import based/sql
import based/sql/delete
import based/value
import gleeunit/should

pub fn basic_delete_test() {
  let expected = "DELETE FROM users WHERE id = ?"
  let users = sql.table("users")

  let query =
    value.repo()
    |> delete.from(users)
    |> delete.where([
      sql.column("id")
      |> sql.eq(value.int(1), of: sql.value),
    ])
    |> delete.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(1)])
}

pub fn delete_with_where_not_test() {
  let expected = "DELETE FROM users WHERE NOT id = ?"
  let users = sql.table("users")

  let query =
    value.repo()
    |> delete.from(users)
    |> delete.where_not([
      sql.column("id")
      |> sql.eq(value.int(1), of: sql.value),
    ])
    |> delete.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(1)])
}

pub fn delete_with_multiple_conditions_test() {
  let expected = "DELETE FROM users WHERE id = ? AND created_at < ?"
  let users = sql.table("users")

  let query =
    value.repo()
    |> delete.from(users)
    |> delete.where([
      sql.column("id")
        |> sql.eq(value.int(1), of: sql.value),
      sql.column("created_at")
        |> sql.lt(value.text("2024-01-01"), of: sql.value),
    ])
    |> delete.to_query

  query.sql |> should.equal(expected)
  query.values
  |> should.equal([value.int(1), value.text("2024-01-01")])
}

pub fn delete_returning_test() {
  let expected = "DELETE FROM users WHERE id = ? RETURNING id, name"
  let users = sql.table("users")

  let query =
    value.repo()
    |> delete.from(users)
    |> delete.where([
      sql.column("id")
      |> sql.eq(value.int(1), of: sql.value),
    ])
    |> delete.returning(["id", "name"])
    |> delete.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(1)])
}

pub fn delete_to_string_test() {
  let expected = "DELETE FROM users WHERE id = 1 AND created_at < '2024-01-01'"
  let users = sql.table("users")

  let query =
    value.repo()
    |> delete.from(users)
    |> delete.where([
      sql.column("id")
        |> sql.eq(value.int(1), of: sql.value),
      sql.column("created_at")
        |> sql.lt(value.text("2024-01-01"), of: sql.value),
    ])

  delete.to_string(query) |> should.equal(expected)
}
