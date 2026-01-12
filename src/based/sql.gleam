import based/sql/internal/expr
import based/sql/internal/fmt
import based/sql/internal/node
import based/sql/internal/table
import gleam/list
import gleam/option.{type Option, None, Some}

/// Sql must be configured by adapter packages.
///
/// Example:
///
/// A PostgreSQL adapter might configure `Sql` like this:
///
/// ```gleam
/// let sql = sql.new()
///   |> sql.on_placeholder(fn(index) { "$" <> int.to_string(index) })
///   |> sql.on_identifier(function.identifier)
///   |> sql.on_value(value.to_string)
/// ```
/// A MariaDB adapter might configure `Sql` like this:
///
/// ```gleam
/// let sql = sql.new()
///   |> sql.on_placeholder(fn(_index) { "?" })
///   |> sql.on_identifier(fn(ident) { "`" <> ident <> "`" })
///   |> sql.on_value(value.to_string)
/// ```
///
pub type Sql(v) {
  Sql(fmt: fmt.Fmt(v))
}

/// Returns a `Sql(v)` record with handlers that does not apply any
/// formatting to identifiers, and returns `?` as placeholders. The value
/// handler's default behaviour is to panic since it handles a generic type.
pub fn new() -> Sql(v) {
  Sql(fmt: fmt.new())
}

/// Sets the placeholder formatting function.
pub fn on_placeholder(
  sql: Sql(v),
  handle_placeholder: fn(Int) -> String,
) -> Sql(v) {
  let fmt = fmt.on_placeholder(sql.fmt, handle_placeholder)

  Sql(fmt:)
}

/// Set the identifier formatting function.
pub fn on_identifier(
  sql: Sql(v),
  handle_identifier: fn(String) -> String,
) -> Sql(v) {
  let fmt = fmt.on_identifier(sql.fmt, handle_identifier)

  Sql(fmt:)
}

/// Set the value formatting function.
pub fn on_value(sql: Sql(v), handle_value: fn(v) -> String) -> Sql(v) {
  let fmt = fmt.on_value(sql.fmt, handle_value)

  Sql(fmt:)
}

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
