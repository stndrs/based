import based/sql/column.{type Column}
import based/sql/condition.{type Condition}
import based/sql/table
import gleam/option.{type Option, None, Some}

pub fn table(name: String) -> table.Table {
  table.new(name)
}

pub fn column(name: String) -> Column {
  column.new(name)
}

pub fn or(left: Condition(v), right: Condition(v)) -> Condition(v) {
  condition.or(left, right)
}

pub fn not(condition: Condition(v)) -> Condition(v) {
  condition.not(condition)
}

pub fn raw(sql: String) -> Condition(v) {
  condition.raw(sql)
}

// ---------- Join ---------- //

pub type JoinType {
  InnerJoin
  LeftJoin
  RightJoin
  FullJoin
}

pub type Join(v) {
  Join(type_: JoinType, table: table.Table, exprs: List(Condition(v)))
}

pub fn inner_join(table: table.Table, exprs: List(Condition(v))) -> Join(v) {
  Join(InnerJoin, table, exprs)
}

pub fn left_join(table: table.Table, exprs: List(Condition(v))) -> Join(v) {
  Join(LeftJoin, table, exprs)
}

pub fn right_join(table: table.Table, exprs: List(Condition(v))) -> Join(v) {
  Join(RightJoin, table, exprs)
}

pub fn full_join(table: table.Table, exprs: List(Condition(v))) -> Join(v) {
  Join(FullJoin, table, exprs)
}

// -------------------------- //

pub type Order {
  Asc
  Desc
}

// pub fn list(vals: List(a), of kind: fn(a) -> v) -> Node {
//   vals
//   |> list.map(fn(val) {
//     val
//     |> kind
//     |> node.value
//   })
//   |> node.list
// }
// 

pub fn value(value: v) -> condition.Node(v) {
  condition.value(value)
}

pub fn nullable(
  value: Option(a),
  inner_type: fn(a) -> condition.Node(v),
) -> condition.Node(v) {
  case value {
    Some(term) -> inner_type(term)
    None -> condition.null
  }
}
