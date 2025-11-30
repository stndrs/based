import based/sql
import based/sql/delete
import based/value
import gleeunit/should

pub fn basic_delete_test() {
  let expected = "DELETE FROM users WHERE id = ?"
  let users = sql.name("users") |> sql.table

  let query =
    delete.from(users)
    |> delete.where([
      sql.name("id")
      |> sql.column
      |> sql.eq(sql.value(1, of: value.int)),
    ])
    |> delete.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(1)])
}

pub fn delete_with_where_not_test() {
  let expected = "DELETE FROM users WHERE NOT id = ?"
  let users = sql.name("users") |> sql.table

  let query =
    delete.from(users)
    |> delete.where_not([
      sql.name("id")
      |> sql.column
      |> sql.eq(sql.value(1, of: value.int)),
    ])
    |> delete.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(1)])
}

pub fn delete_with_multiple_conditions_test() {
  let expected = "DELETE FROM users WHERE id = ? AND created_at < ?"
  let users = sql.name("users") |> sql.table

  let query =
    delete.from(users)
    |> delete.where([
      sql.name("id")
        |> sql.column
        |> sql.eq(sql.value(1, of: value.int)),
      sql.name("created_at")
        |> sql.column
        |> sql.lt(sql.value("2024-01-01", of: value.text)),
    ])
    |> delete.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values
  |> should.equal([value.int(1), value.text("2024-01-01")])
}

pub fn delete_returning_test() {
  let expected = "DELETE FROM users WHERE id = ? RETURNING id, name"
  let users = sql.name("users") |> sql.table

  let query =
    delete.from(users)
    |> delete.where([
      sql.name("id")
      |> sql.column
      |> sql.eq(sql.value(1, of: value.int)),
    ])
    |> delete.returning(["id", "name"])
    |> delete.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(1)])
}

pub fn delete_to_string_test() {
  let expected = "DELETE FROM users WHERE id = 1 AND created_at < '2024-01-01'"
  let users = sql.name("users") |> sql.table

  let query =
    delete.from(users)
    |> delete.where([
      sql.name("id")
        |> sql.column
        |> sql.eq(sql.value(1, of: value.int)),
      sql.name("created_at")
        |> sql.column
        |> sql.lt(sql.value("2024-01-01", of: value.text)),
    ])

  delete.to_string(query, value.format()) |> should.equal(expected)
}
