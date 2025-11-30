import based/sql/column
import based/sql/table
import based/value
import gleam/string_tree
import gleeunit/should

pub fn new_test() {
  let col = column.new("id")

  let expected = "id"
  column.to_string_tree(col, value.format())
  |> string_tree.to_string
  |> should.equal(expected)
}

pub fn for_test() {
  let users = table.new("users")

  let col = column.new("id") |> column.for(users)

  let expected = "users.id"
  column.to_string_tree(col, value.format())
  |> string_tree.to_string
  |> should.equal(expected)
}

pub fn alias_test() {
  let col = column.new("user_id") |> column.alias("id")

  let expected = "user_id AS id"
  column.to_string_tree(col, value.format())
  |> string_tree.to_string
  |> should.equal(expected)
}

pub fn table_and_alias_test() {
  let users = table.new("users")

  let col =
    column.new("user_id")
    |> column.for(users)
    |> column.alias("id")

  let expected = "users.user_id AS id"
  column.to_string_tree(col, value.format())
  |> string_tree.to_string
  |> should.equal(expected)
}
