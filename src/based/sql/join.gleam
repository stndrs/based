import based/sql/expr.{type Expr}
import based/sql/table.{type Table}

pub type JoinType {
  InnerJoin
  LeftJoin
  RightJoin
  FullJoin
}

pub type Join(v) {
  Join(type_: JoinType, table: Table(v), exprs: List(Expr(v)))
}

pub fn inner(table: Table(v), exprs: List(Expr(v))) -> Join(v) {
  Join(InnerJoin, table, exprs)
}

pub fn left(table: Table(v), exprs: List(Expr(v))) -> Join(v) {
  Join(LeftJoin, table, exprs)
}

pub fn right(table: Table(v), exprs: List(Expr(v))) -> Join(v) {
  Join(RightJoin, table, exprs)
}

pub fn full(table: Table(v), exprs: List(Expr(v))) -> Join(v) {
  Join(FullJoin, table, exprs)
}
