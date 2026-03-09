import based/sql/column.{type Column}
import based/sql/condition.{type Condition}
import based/sql/table
import gleam/list
import gleam/option.{type Option, None, Some}

// ---------- Table ---------- //

pub fn table(name: String) -> table.Table {
  table.new(name)
}

// ---------- Column ---------- //

pub fn column(name: String) -> Column {
  column.new(name)
}

pub const all = column.all

pub fn avg(name: String) -> Column {
  column.avg(name)
}

pub fn count(name: String) -> Column {
  column.count(name)
}

pub fn max(name: String) -> Column {
  column.max(name)
}

pub fn min(name: String) -> Column {
  column.min(name)
}

pub fn sum(name: String) -> Column {
  column.sum(name)
}

// ---------- Conditions ---------- //

pub type Kind(a, v) {
  Kind(comparable: fn() -> condition.Comparable(a, v))
}

pub const col = Kind(comparable: column_comparable)

pub const val = Kind(comparable: value_comparable)

fn column_comparable() -> condition.Comparable(Column, v) {
  condition.comparable(fn(col) {
    let node = column.value(col)

    #(node, [])
  })
}

fn value_comparable() -> condition.Comparable(v, v) {
  condition.comparable(fn(val) {
    let node = condition.value

    #(node, [val])
  })
}

pub fn eq(
  column: Column,
  right: a,
  of kind: Kind(a, v),
) -> #(Condition, List(v)) {
  column.eq(column, right, of: kind.comparable)
}

pub fn gt(
  column: Column,
  right: a,
  of kind: Kind(a, v),
) -> #(Condition, List(v)) {
  column.gt(column, right, of: kind.comparable)
}

pub fn lt(
  column: Column,
  right: a,
  of kind: Kind(a, v),
) -> #(Condition, List(v)) {
  column.lt(column, right, of: kind.comparable)
}

pub fn gt_eq(
  column: Column,
  right: a,
  of kind: Kind(a, v),
) -> #(Condition, List(v)) {
  column.gt_eq(column, right, of: kind.comparable)
}

pub fn lt_eq(
  column: Column,
  right: a,
  of kind: Kind(a, v),
) -> #(Condition, List(v)) {
  column.lt_eq(column, right, of: kind.comparable)
}

pub fn not_eq(
  column: Column,
  right: a,
  of kind: Kind(a, v),
) -> #(Condition, List(v)) {
  column.not_eq(column, right, of: kind.comparable)
}

pub fn between(
  column: Column,
  start: a,
  end: a,
  of kind: Kind(a, v),
) -> #(Condition, List(v)) {
  column.between(column, start, end, of: kind.comparable)
}

pub fn like(column: Column, val: String) -> #(Condition, List(v)) {
  column.like(column, val)
}

pub fn not_like(column: Column, val: String) -> #(Condition, List(v)) {
  column.not_like(column, val)
}

pub fn in(
  column: Column,
  right: a,
  of kind: Kind(a, v),
) -> #(Condition, List(v)) {
  column.in(column, right, of: kind.comparable)
}

pub fn is(column: Column, right: Bool) -> Condition {
  column
  |> column.value
  |> condition.is(right)
}

pub fn is_null(column: Column) -> #(Condition, List(v)) {
  column.is_null(column)
}

pub fn is_not_null(column: Column) -> Condition {
  column
  |> column.value
  |> condition.is_null(False)
}

pub fn or(
  left: #(Condition, List(v)),
  right: #(Condition, List(v)),
) -> #(Condition, List(v)) {
  let condition = condition.or(left.0, right.0)
  let values = list.flatten([left.1, right.1])

  #(condition, values)
}

pub fn not(condition: #(Condition, List(v))) -> #(Condition, List(v)) {
  #(condition.not(condition.0), condition.1)
}

/// Creates a raw SQL condition from an arbitrary SQL string. The SQL is
/// included verbatim in the generated query with no parameterization.
pub fn raw(sql: String) -> #(Condition, List(v)) {
  #(condition.raw(sql), [])
}

// ---------- Join ---------- //

pub type JoinType {
  InnerJoin
  LeftJoin
  RightJoin
  FullJoin
}

pub type Join {
  Join(type_: JoinType, table: table.Table, conditions: List(Condition))
}

pub fn inner_join(table: table.Table, conditions: List(Condition)) -> Join {
  Join(InnerJoin, table, conditions)
}

pub fn left_join(table: table.Table, conditions: List(Condition)) -> Join {
  Join(LeftJoin, table, conditions)
}

pub fn right_join(table: table.Table, conditions: List(Condition)) -> Join {
  Join(RightJoin, table, conditions)
}

pub fn full_join(table: table.Table, conditions: List(Condition)) -> Join {
  Join(FullJoin, table, conditions)
}

// -------------------------- //

pub type Order {
  Asc
  Desc
}

pub fn list(of kind: fn(a) -> v) -> Kind(List(a), v) {
  Kind(comparable: fn() {
    condition.comparable(fn(vals: List(a)) {
      let node =
        vals
        |> list.length
        |> condition.values

      let vals = list.map(vals, kind)

      #(node, vals)
    })
  })
}

pub fn nullable(
  value: Option(a),
  inner_type: fn(a) -> condition.Node,
) -> condition.Node {
  case value {
    Some(term) -> inner_type(term)
    None -> condition.null
  }
}
