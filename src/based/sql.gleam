import based/sql/internal/expr
import based/sql/internal/node
import based/sql/internal/table
import gleam/list
import gleam/option.{type Option, None, Some}

pub type Node(v) =
  node.Node(v)

// Exprs

pub fn eq(left: Node(v), right: Node(v)) -> Expr(v) {
  expr.compare(left, right, expr.Eq)
}

pub fn gt(left: Node(v), right: Node(v)) -> Expr(v) {
  expr.compare(left, right, expr.Gt)
}

pub fn lt(left: Node(v), right: Node(v)) -> Expr(v) {
  expr.compare(left, right, expr.Lt)
}

pub fn gt_eq(left: Node(v), right: Node(v)) -> Expr(v) {
  expr.compare(left, right, expr.GtEq)
}

pub fn lt_eq(left: Node(v), right: Node(v)) -> Expr(v) {
  expr.compare(left, right, expr.LtEq)
}

pub fn not_eq(left: Node(v), right: Node(v)) -> Expr(v) {
  expr.compare(left, right, expr.NotEq)
}

pub fn between(left: Node(v), start: Node(v), end: Node(v)) -> Expr(v) {
  expr.compare(left, end, expr.Between(start))
}

pub fn like(left: Node(v), val: String, of outer: fn(String) -> v) -> Expr(v) {
  let right = outer(val) |> value

  expr.compare(left, right, expr.Like)
}

pub fn in(left: Node(v), right: Node(v)) -> Expr(v) {
  expr.compare(left, right, expr.In)
}

pub fn is(left: Node(v), right: Bool) -> Expr(v) {
  expr.is(left, right)
}

pub fn is_null(left: Node(v)) -> Expr(v) {
  expr.is_null(left, True)
}

pub fn is_not_null(left: Node(v)) -> Expr(v) {
  expr.is_null(left, False)
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

pub fn not_like(
  left: Node(v),
  val: String,
  of outer: fn(String) -> v,
) -> Expr(v) {
  let right = outer(val) |> value

  expr.compare(left, right, expr.NotLike)
}

pub fn raw(sql: String) -> Expr(v) {
  expr.raw(sql)
}

pub type Order {
  Asc
  Desc
}

pub fn tuples(vals: List(List(Node(v)))) -> Node(v) {
  vals
  |> list.map(list.flat_map(_, node.unwrap))
  |> node.Tuples
}

pub fn list(vals: List(a), of inner_type: fn(a) -> v) -> Node(v) {
  list.map(vals, inner_type)
  |> node.Values
}

pub fn columns(names: List(String)) -> Node(v) {
  node.Columns(names)
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

pub fn values(values: List(v)) -> Node(v) {
  node.Values(values)
}

pub type Expr(v) =
  expr.Expr(v)

pub type Table(v) =
  table.Table(v)

pub type Identifier =
  table.Identifier

pub fn identifier(name: String) -> Identifier {
  table.identifier(name)
}

pub fn alias(identifier: Identifier, alias: String) -> Identifier {
  table.alias(identifier, alias)
}

pub fn attr(identifier: Identifier, attr: String) -> Identifier {
  table.attr(identifier, attr)
}

pub fn column(identifier: Identifier) -> Node(v) {
  node.Column(identifier)
}

pub fn table(identifier: Identifier) -> Table(v) {
  table.new(identifier)
}
