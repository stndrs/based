import based
import based/sql/expression.{type Expression}
import based/sql/internal/fmt
import based/sql/node.{type Node}
import based/sql/table
import gleam/option.{type Option, None, Some}
import gleam/string

pub opaque type Column {
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
  node.column(column.name, column.alias, column.table)
}

// Expressions

pub fn eq(column: Column, right: a, of kind: fn(a) -> Node(v)) -> Expression(v) {
  let right = kind(right)

  column
  |> node
  |> expression.compare(right, expression.Eq)
}

pub fn gt(column: Column, right: a, of kind: fn(a) -> Node(v)) -> Expression(v) {
  let right = kind(right)

  column
  |> node
  |> expression.compare(right, expression.Gt)
}

pub fn lt(column: Column, right: a, of kind: fn(a) -> Node(v)) -> Expression(v) {
  let right = kind(right)

  column
  |> node
  |> expression.compare(right, expression.Lt)
}

pub fn gt_eq(
  column: Column,
  right: a,
  of kind: fn(a) -> Node(v),
) -> Expression(v) {
  let right = kind(right)

  column
  |> node
  |> expression.compare(right, expression.GtEq)
}

pub fn lt_eq(
  column: Column,
  right: a,
  of kind: fn(a) -> Node(v),
) -> Expression(v) {
  let right = kind(right)

  column
  |> node
  |> expression.compare(right, expression.LtEq)
}

pub fn not_eq(
  column: Column,
  right: a,
  of kind: fn(a) -> Node(v),
) -> Expression(v) {
  let right = kind(right)

  column
  |> node
  |> expression.compare(right, expression.NotEq)
}

pub fn between(
  column: Column,
  start: a,
  end: a,
  of kind: fn(a) -> Node(v),
) -> Expression(v) {
  let end = kind(end)
  let start = kind(start)

  column
  |> node
  |> expression.compare(end, expression.Between(start))
}

pub fn like(column: Column, val: String) -> Expression(v) {
  let right = node.text(val)

  column
  |> node
  |> expression.compare(right, expression.Like)
}

pub fn in(column: Column, right: a, of kind: fn(a) -> Node(v)) -> Expression(v) {
  let right = kind(right)

  column
  |> node
  |> expression.compare(right, expression.In)
}

pub fn is(column: Column, right: Bool) -> Expression(v) {
  column
  |> node
  |> expression.is(right)
}

pub fn is_null(column: Column) -> Expression(v) {
  column
  |> node
  |> expression.is_null(True)
}

pub fn is_not_null(column: Column) -> Expression(v) {
  column
  |> node
  |> expression.is_null(False)
}

pub fn not_like(column: Column, val: String) -> Expression(v) {
  let right = node.text(val)

  column
  |> node
  |> expression.compare(right, expression.NotLike)
}
