import based/sql/column
import based/sql/expression.{type Expression}
import based/sql/node.{type Node}
import based/sql/table
import gleam/list
import gleam/option.{type Option, None, Some}

pub fn table(name: String) -> table.Table {
  table.new(name)
}

pub fn column(name: String) -> column.Column {
  column.new(name)
}

pub fn or(left: Expression(v), right: Expression(v)) -> Expression(v) {
  expression.logical(left, right, expression.Or)
}

pub fn and(left: Expression(v), right: Expression(v)) -> Expression(v) {
  expression.logical(left, right, expression.And)
}

pub fn not(expr: Expression(v)) -> Expression(v) {
  expression.not(expr)
}

pub fn raw(sql: String) -> Expression(v) {
  expression.raw(sql)
}

pub type Order {
  Asc
  Desc
}

pub fn list(vals: List(a), of kind: fn(a) -> v) -> Node(v) {
  vals
  |> list.map(fn(val) {
    val
    |> kind
    |> node.value
  })
  |> node.list
}

pub fn value(value: v) -> Node(v) {
  node.value(value)
}

pub fn nullable(value: Option(a), inner_type: fn(a) -> Node(v)) -> Node(v) {
  case value {
    Some(term) -> inner_type(term)
    None -> node.null(True)
  }
}
