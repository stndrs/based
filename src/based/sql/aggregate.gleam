import based
import based/sql/column.{type Column}
import based/sql/expression.{type Expression}
import based/sql/internal/fmt
import based/sql/node.{type Node}

// ---------- Aggregate Functions ---------- //

pub opaque type Aggregate {
  Avg(col: Column)
  Count(col: Column)
  Max(col: Column)
  Min(col: Column)
  Sum(col: Column)
}

pub fn avg(col: Column) -> Aggregate {
  Avg(col:)
}

pub fn count(col: Column) -> Aggregate {
  Count(col:)
}

pub fn max(col: Column) -> Aggregate {
  Max(col:)
}

pub fn min(col: Column) -> Aggregate {
  Min(col:)
}

pub fn sum(col: Column) -> Aggregate {
  Sum(col:)
}

fn to_node(aggregate: Aggregate, repo: based.Repo(v)) -> Node(v) {
  todo
}

// Expressions

pub fn eq(
  aggregate: Aggregate,
  right: a,
  of kind: fn(a) -> Node(v),
) -> Expression(v) {
  let right = kind(right)

  aggregate
  |> to_node(todo)
  |> expression.compare(right, expression.Eq)
}

pub fn gt(
  aggregate: Aggregate,
  right: a,
  of kind: fn(a) -> Node(v),
) -> Expression(v) {
  let right = kind(right)

  aggregate
  |> to_node(todo)
  |> expression.compare(right, expression.Gt)
}

pub fn lt(
  aggregate: Aggregate,
  right: a,
  of kind: fn(a) -> Node(v),
) -> Expression(v) {
  let right = kind(right)

  aggregate
  |> to_node(todo)
  |> expression.compare(right, expression.Lt)
}

pub fn gt_eq(
  aggregate: Aggregate,
  right: a,
  of kind: fn(a) -> Node(v),
) -> Expression(v) {
  let right = kind(right)

  aggregate
  |> to_node(todo)
  |> expression.compare(right, expression.GtEq)
}

pub fn lt_eq(
  aggregate: Aggregate,
  right: a,
  of kind: fn(a) -> Node(v),
) -> Expression(v) {
  let right = kind(right)

  aggregate
  |> to_node(todo)
  |> expression.compare(right, expression.LtEq)
}

pub fn not_eq(
  aggregate: Aggregate,
  right: a,
  of kind: fn(a) -> Node(v),
) -> Expression(v) {
  let right = kind(right)

  aggregate
  |> to_node(todo)
  |> expression.compare(right, expression.NotEq)
}

@internal
pub fn to_string(aggregate: Aggregate, repo: based.Repo(v)) -> String {
  case aggregate {
    Avg(col:) ->
      col
      |> column.to_string(repo)
      |> fmt.avg
    Count(col:) ->
      col
      |> column.to_string(repo)
      |> fmt.count
    Max(col:) ->
      col
      |> column.to_string(repo)
      |> fmt.max
    Min(col:) ->
      col
      |> column.to_string(repo)
      |> fmt.min
    Sum(col:) ->
      col
      |> column.to_string(repo)
      |> fmt.sum
  }
}
