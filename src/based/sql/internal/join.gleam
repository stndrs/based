import based/sql

pub type JoinType {
  InnerJoin
  LeftJoin
  RightJoin
  FullJoin
}

pub type Join(v) {
  Join(type_: JoinType, table: sql.Table(v), exprs: List(sql.Expr(v)))
}

pub fn inner(table: sql.Table(v), exprs: List(sql.Expr(v))) -> Join(v) {
  Join(InnerJoin, table, exprs)
}

pub fn left(table: sql.Table(v), exprs: List(sql.Expr(v))) -> Join(v) {
  Join(LeftJoin, table, exprs)
}

pub fn right(table: sql.Table(v), exprs: List(sql.Expr(v))) -> Join(v) {
  Join(RightJoin, table, exprs)
}

pub fn full(table: sql.Table(v), exprs: List(sql.Expr(v))) -> Join(v) {
  Join(FullJoin, table, exprs)
}
