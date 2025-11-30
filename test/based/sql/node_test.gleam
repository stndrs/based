import based/sql
import based/sql/column
import based/sql/node
import based/sql/table
import based/value
import gleam/string_tree
import gleeunit/should

pub fn column_test() {
  let col = column.new("id")
  let col_node = node.column(col)

  let expected = "id"
  node.to_string_tree(col_node, value.format())
  |> string_tree.to_string
  |> should.equal(expected)
}

pub fn column_with_table_test() {
  let users = table.new("users")
  let col = column.new("id") |> column.for(users)
  let col_node = node.column(col)

  let expected = "users.id"
  node.to_string_tree(col_node, value.format())
  |> string_tree.to_string
  |> should.equal(expected)
}

pub fn columns_test() {
  let cols = [column.new("id"), column.new("name"), column.new("email")]
  let cols_node = node.columns(cols)

  let expected = "(id, name, email)"
  node.to_string_tree(cols_node, value.format())
  |> string_tree.to_string
  |> should.equal(expected)
}

pub fn literal_test() {
  let lit = node.literal(value.int(42))

  let expected = ":param"
  node.to_string_tree(lit, value.format())
  |> string_tree.to_string
  |> should.equal(expected)

  node.unwrap(lit) |> should.equal([value.int(42)])
}

pub fn literals_test() {
  let lits = node.literals([value.int(1), value.int(2), value.int(3)])

  let expected = "(:param, :param, :param)"
  node.to_string_tree(lits, value.format())
  |> string_tree.to_string
  |> should.equal(expected)

  node.unwrap(lits)
  |> should.equal([value.int(1), value.int(2), value.int(3)])
}

pub fn tuples_test() {
  let tuples =
    node.tuples([
      [value.int(1), value.text("John")],
      [value.int(2), value.text("Jane")],
    ])

  let expected = "((:param, :param), (:param, :param))"
  node.to_string_tree(tuples, value.format())
  |> string_tree.to_string
  |> should.equal(expected)

  node.unwrap(tuples)
  |> should.equal([
    value.int(1),
    value.text("John"),
    value.int(2),
    value.text("Jane"),
  ])
}

pub fn special_literals_test() {
  let true_node = sql.true
  let false_node = sql.false
  let null_node = sql.null

  node.to_string_tree(true_node, value.format())
  |> string_tree.to_string
  |> should.equal(":param")
  node.to_string_tree(false_node, value.format())
  |> string_tree.to_string
  |> should.equal(":param")
  node.to_string_tree(null_node, value.format())
  |> string_tree.to_string
  |> should.equal(":param")

  node.unwrap(true_node) |> should.equal([value.true])
  node.unwrap(false_node) |> should.equal([value.false])
  node.unwrap(null_node) |> should.equal([value.null])
}
