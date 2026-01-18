import based/sql/column
import based/sql/internal/expr
import based/sql/internal/node.{type Node}
import based/sql/table
import gleam/list
import gleam/option.{type Option, None, Some}

pub fn table(name: String) -> table.Table {
  table.new(name)
}

pub fn column(name: String) -> column.Column {
  column.new(name)
}

pub fn or(left: Expr(v), right: Expr(v)) -> Expr(v) {
  expr.logical(left, right, expr.Or)
}

pub fn and(left: Expr(v), right: Expr(v)) -> Expr(v) {
  expr.logical(left, right, expr.And)
}

pub fn not(expr: Expr(v)) -> Expr(v) {
  expr.not(expr)
}

pub fn raw(sql: String) -> Expr(v) {
  expr.raw(sql)
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
    |> node.Value
  })
  |> node.List
}

pub fn value(value: v) -> Node(v) {
  node.Value(value)
}

pub fn nullable(value: Option(a), inner_type: fn(a) -> Node(v)) -> Node(v) {
  case value {
    Some(term) -> inner_type(term)
    None -> node.Null(True)
  }
}

pub type Expr(v) =
  expr.Expr(v)
