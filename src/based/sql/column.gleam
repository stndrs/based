import based
import based/sql/internal/expr.{type Expr}
import based/sql/internal/fmt
import based/sql/internal/node.{type Node}
import based/sql/table
import gleam/option.{type Option, None, Some}
import gleam/string

pub type Column {
  Column(name: String, alias: Option(String), table: Option(String))
}

pub fn new(name: String) -> Column {
  Column(name:, alias: None, table: None)
}

pub fn alias(column: Column, alias: String) -> Column {
  Column(..column, alias: Some(alias))
}

pub fn for(column: Column, table: table.Table) -> Column {
  Column(..column, table: Some(table.name))
}

@internal
pub fn to_string(column: Column, repo: based.Repo(v)) -> String {
  let Column(name:, alias:, table:) = column

  let ident = case table {
    Some(table) -> {
      table
      |> fmt.to_identifier(repo.fmt, _)
      |> string.append(".")
      |> string.append(fmt.to_identifier(repo.fmt, name))
    }
    None -> fmt.to_identifier(repo.fmt, name)
  }

  case alias {
    Some(a) -> fmt.alias(ident, a)
    None -> ident
  }
}

pub fn node(column: Column) -> Node(v) {
  node.Column(name: column.name, alias: column.alias, table: column.table)
}

// Expressions

pub fn eq(column: Column, right: a, of kind: fn(a) -> Node(v)) -> Expr(v) {
  let right = kind(right)

  column
  |> node
  |> expr.compare(right, expr.Eq)
}

pub fn gt(column: Column, right: a, of kind: fn(a) -> Node(v)) -> Expr(v) {
  let right = kind(right)

  column
  |> node
  |> expr.compare(right, expr.Gt)
}

pub fn lt(column: Column, right: a, of kind: fn(a) -> Node(v)) -> Expr(v) {
  let right = kind(right)

  column
  |> node
  |> expr.compare(right, expr.Lt)
}

pub fn gt_eq(column: Column, right: a, of kind: fn(a) -> Node(v)) -> Expr(v) {
  let right = kind(right)

  column
  |> node
  |> expr.compare(right, expr.GtEq)
}

pub fn lt_eq(column: Column, right: a, of kind: fn(a) -> Node(v)) -> Expr(v) {
  let right = kind(right)

  column
  |> node
  |> expr.compare(right, expr.LtEq)
}

pub fn not_eq(column: Column, right: a, of kind: fn(a) -> Node(v)) -> Expr(v) {
  let right = kind(right)

  column
  |> node
  |> expr.compare(right, expr.NotEq)
}

pub fn between(
  column: Column,
  start: a,
  end: a,
  of kind: fn(a) -> Node(v),
) -> Expr(v) {
  let end = kind(end)
  let start = kind(start)

  column
  |> node
  |> expr.compare(end, expr.Between(start))
}

pub fn like(column: Column, val: String) -> Expr(v) {
  let right = node.Text(val)

  column
  |> node
  |> expr.compare(right, expr.Like)
}

pub fn in(column: Column, right: a, of kind: fn(a) -> Node(v)) -> Expr(v) {
  let right = kind(right)

  column
  |> node
  |> expr.compare(right, expr.In)
}

pub fn is(column: Column, right: Bool) -> Expr(v) {
  column
  |> node
  |> expr.is(right)
}

pub fn is_null(column: Column) -> Expr(v) {
  column
  |> node
  |> expr.is_null(True)
}

pub fn is_not_null(column: Column) -> Expr(v) {
  column
  |> node
  |> expr.is_null(False)
}

pub fn not_like(column: Column, val: String) -> Expr(v) {
  let right = node.Text(val)

  column
  |> node
  |> expr.compare(right, expr.NotLike)
}
