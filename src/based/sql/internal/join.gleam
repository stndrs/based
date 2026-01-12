import based/sql/internal/expr
import based/sql/internal/table

pub type JoinType {
  InnerJoin
  LeftJoin
  RightJoin
  FullJoin
}

pub type Join(v) {
  Join(type_: JoinType, table: table.Table(v), exprs: List(expr.Expr(v)))
}

pub fn inner(table: table.Table(v), exprs: List(expr.Expr(v))) -> Join(v) {
  Join(InnerJoin, table, exprs)
}

pub fn left(table: table.Table(v), exprs: List(expr.Expr(v))) -> Join(v) {
  Join(LeftJoin, table, exprs)
}

pub fn right(table: table.Table(v), exprs: List(expr.Expr(v))) -> Join(v) {
  Join(RightJoin, table, exprs)
}

pub fn full(table: table.Table(v), exprs: List(expr.Expr(v))) -> Join(v) {
  Join(FullJoin, table, exprs)
}
