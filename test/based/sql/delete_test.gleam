import based/sql
import based/sql/delete
import based/sql/table
import based/value
import gleeunit/should

pub fn basic_delete_test() {
  let expected = "DELETE FROM users WHERE id = ?"
  let users = table.new("users")

  let query =
    sql.delete()
    |> delete.from(users)
    |> delete.where([sql.column("id") |> sql.eq(sql.int(1))])
    |> delete.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(1)])
}

pub fn delete_with_where_not_test() {
  let expected = "DELETE FROM users WHERE NOT id = ?"
  let users = table.new("users")

  let query =
    sql.delete()
    |> delete.from(users)
    |> delete.where_not([sql.column("id") |> sql.eq(sql.int(1))])
    |> delete.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(1)])
}

pub fn delete_with_multiple_conditions_test() {
  let expected = "DELETE FROM users WHERE id = ? AND created_at < ?"
  let users = table.new("users")

  let query =
    sql.delete()
    |> delete.from(users)
    |> delete.where([
      sql.column("id") |> sql.eq(sql.int(1)),
      sql.column("created_at") |> sql.lt(sql.text("2024-01-01")),
    ])
    |> delete.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values
  |> should.equal([value.int(1), value.text("2024-01-01")])
}

pub fn delete_returning_test() {
  let expected = "DELETE FROM users WHERE id = ? RETURNING id, name"
  let users = table.new("users")

  let query =
    sql.delete()
    |> delete.from(users)
    |> delete.where([sql.column("id") |> sql.eq(sql.int(1))])
    |> delete.returning(["id", "name"])
    |> delete.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(1)])
}

pub fn delete_to_string_test() {
  let expected = "DELETE FROM users WHERE id = 1 AND created_at < '2024-01-01'"
  let users = table.new("users")

  let query =
    sql.delete()
    |> delete.from(users)
    |> delete.where([
      sql.column("id") |> sql.eq(sql.int(1)),
      sql.column("created_at") |> sql.lt(sql.text("2024-01-01")),
    ])

  delete.to_string(query, value.format()) |> should.equal(expected)
}
