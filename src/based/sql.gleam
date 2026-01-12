import based/db
import based/sql/internal/expr
import based/sql/internal/node
import based/sql/internal/table
import gleam/function
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
pub opaque type Sql(v) {
  Sql(
    handle_identifier: fn(String) -> String,
    handle_placeholder: fn(Int) -> String,
    handle_value: fn(v) -> String,
  )
}

/// Returns a `Sql(v)` record with handlers that does not apply any
/// formatting to identifiers, and returns `?` as placeholders. The value
/// handler's default behaviour is to panic since it handles a generic type.
pub fn new() -> Sql(v) {
  Sql(
    handle_identifier: function.identity,
    handle_placeholder: fn(_) { "?" },
    handle_value: fn(_) { panic as "based/sql.Sql not configured" },
  )
}

/// Apply the configured identifier format function to the provided identifier.
@internal
pub fn to_identifier(sql: Sql(v), identifier: String) -> String {
  sql.handle_identifier(identifier)
}

/// Apply the configured value format function to the provided value.
@internal
pub fn to_string(sql: Sql(v), value: v) -> String {
  sql.handle_value(value)
}

/// Apply the configured placeholder format function to the provided
/// placeholder index.
@internal
pub fn to_placeholder(sql: Sql(v), value: Int) -> String {
  sql.handle_placeholder(value)
}

/// Sets the placeholder formatting function.
pub fn on_placeholder(
  sql: Sql(v),
  handle_placeholder: fn(Int) -> String,
) -> Sql(v) {
  Sql(..sql, handle_placeholder:)
}

/// Set the identifier formatting function.
pub fn on_identifier(
  sql: Sql(v),
  handle_identifier: fn(String) -> String,
) -> Sql(v) {
  Sql(..sql, handle_identifier:)
}

/// Set the value formatting function.
pub fn on_value(sql: Sql(v), handle_value: fn(v) -> String) -> Sql(v) {
  Sql(..sql, handle_value:)
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

// Node

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

// Expr

pub type Expr(v) =
  expr.Expr(v)

// Identifier

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

// Table

pub fn subquery(query: db.Query(v)) -> Table(v) {
  table.subquery(query)
}
