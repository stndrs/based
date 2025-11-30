import based/sql/column
import based/sql/delete.{type Delete}
import based/sql/expr.{type Expr}
import based/sql/insert.{type Insert}
import based/sql/node.{type Node}
import based/sql/select.{type Select}
import based/sql/table.{type Table}
import based/sql/union.{type Union}
import based/sql/update.{type Update}
import based/sql/with.{type With}
import gleam/list
import gleam/option.{type Option, None, Some}

// Queries

pub fn select(columns: List(String)) -> Select(v) {
  select.new(columns)
}

pub fn update(table: Table(v)) -> Update(v) {
  update.new(table)
}

pub fn insert(into table: Table(v)) -> Insert(v) {
  insert.new(table)
}

pub fn delete() -> Delete(v) {
  delete.new()
}

pub fn union(selects: List(Select(v))) -> Union(v) {
  union.new(selects)
}

pub fn union_all(selects: List(Select(v))) -> Union(v) {
  union.all(selects)
}

pub fn with(ctes: List(with.Cte(v))) -> With(v) {
  with.new(ctes)
}

// Table

pub fn table(name: String) -> Table(v) {
  table.new(name)
}

// Nodes

pub fn tuples(vals: List(List(Node(v)))) -> Node(v) {
  vals
  |> list.map(list.flat_map(_, node.unwrap))
  |> node.tuples
}

pub fn list(vals: List(a), of inner_type: fn(a) -> Node(v)) -> Node(v) {
  list.map(vals, inner_type)
  |> list.flat_map(node.unwrap)
  |> node.values
}

pub fn column(name: String) -> Node(v) {
  column.new(name) |> node.column
}

pub fn columns(names: List(String)) -> Node(v) {
  list.map(names, column.new) |> node.columns
}

pub fn value(value: a, of kind: fn(a) -> v) -> Node(v) {
  value
  |> kind
  |> node.value
}

pub fn nullable(
  value: Option(a),
  inner_type: fn(a) -> Node(v),
  or_else null: fn(Nil) -> Node(v),
) -> Node(v) {
  case value {
    Some(term) -> inner_type(term)
    None -> null(Nil)
  }
}

// Exprs

pub fn eq(left: Node(v), right: Node(v)) -> Expr(v) {
  expr.eq(left, right)
}

pub fn gt(left: Node(v), right: Node(v)) -> Expr(v) {
  expr.gt(left, right)
}

pub fn lt(left: Node(v), right: Node(v)) -> Expr(v) {
  expr.lt(left, right)
}

pub fn gt_eq(left: Node(v), right: Node(v)) -> Expr(v) {
  expr.gt_eq(left, right)
}

pub fn lt_eq(left: Node(v), right: Node(v)) -> Expr(v) {
  expr.lt_eq(left, right)
}

pub fn not_eq(left: Node(v), right: Node(v)) -> Expr(v) {
  expr.not_eq(left, right)
}

pub fn between(left: Node(v), start: Node(v), end: Node(v)) -> Expr(v) {
  expr.between(left, start, end)
}

pub fn like(
  left: Node(v),
  value: String,
  of outer: fn(String) -> Node(v),
) -> Expr(v) {
  expr.like(left, outer(value))
}

pub fn in(left: Node(v), right: Node(v)) -> Expr(v) {
  expr.in(left, right)
}

pub fn is(left: Node(v), right: Node(v)) -> Expr(v) {
  expr.is(left, right)
}

pub fn or(expr: Expr(v), expr1: Expr(v)) -> Expr(v) {
  expr.or(expr, expr1)
}

pub fn not(expr: Expr(v)) -> Expr(v) {
  expr.not(expr)
}

pub fn not_like(
  left: Node(v),
  right: String,
  of outer: fn(String) -> Node(v),
) -> Expr(v) {
  expr.not_like(left, outer(right))
}
