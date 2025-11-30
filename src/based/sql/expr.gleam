import based/format.{type Format}
import based/sql/internal/fmt
import based/sql/node.{type Node}
import gleam/list
import gleam/string_tree.{type StringTree}

pub opaque type Expr(v) {
  Compare(left: Node(v), right: Node(v), operator: ComparisonOperator(v))
  Logical(left: Expr(v), right: Expr(v), operator: LogicalOperator)
  Not(expr: Expr(v))
}

pub fn eq(left: Node(v), right: Node(v)) -> Expr(v) {
  Compare(left:, right:, operator: Eq)
}

pub fn gt(left: Node(v), right: Node(v)) -> Expr(v) {
  Compare(left:, right:, operator: Gt)
}

pub fn lt(left: Node(v), right: Node(v)) -> Expr(v) {
  Compare(left:, right:, operator: Lt)
}

pub fn gt_eq(left: Node(v), right: Node(v)) -> Expr(v) {
  Compare(left:, right:, operator: GtEq)
}

pub fn lt_eq(left: Node(v), right: Node(v)) -> Expr(v) {
  Compare(left:, right:, operator: LtEq)
}

pub fn not_eq(left: Node(v), right: Node(v)) -> Expr(v) {
  Compare(left:, right:, operator: NotEq)
}

pub fn between(left: Node(v), start: Node(v), end: Node(v)) -> Expr(v) {
  Compare(left:, right: end, operator: Between(start))
}

pub fn like(left: Node(v), right: Node(v)) -> Expr(v) {
  Compare(left:, right:, operator: Like)
}

pub fn in(left: Node(v), right: Node(v)) -> Expr(v) {
  Compare(left:, right:, operator: In)
}

pub fn is(left: Node(v), right: Node(v)) -> Expr(v) {
  Compare(left:, right:, operator: Is)
}

pub fn not_like(left: Node(v), right: Node(v)) -> Expr(v) {
  Compare(left:, right:, operator: NotLike)
}

pub fn and(left: Expr(v), right: Expr(v)) -> Expr(v) {
  Logical(left:, right:, operator: And)
}

pub fn or(left: Expr(v), right: Expr(v)) -> Expr(v) {
  Logical(left:, right:, operator: Or)
}

pub fn not(expr: Expr(v)) -> Expr(v) {
  Not(expr:)
}

pub fn to_values(expr: Expr(v)) -> List(v) {
  case expr {
    Compare(left, right, op) -> {
      list.flatten([unwrap(left), operator_values(op), unwrap(right)])
    }
    Logical(left, right, _) -> {
      list.flatten([to_values(left), to_values(right)])
    }
    Not(expr) -> to_values(expr)
  }
}

pub fn unwrap(node: Node(v)) -> List(v) {
  case node {
    node.Literal(val) -> [val]
    node.Literals(vals) -> vals
    node.Tuples(vals) -> list.flatten(vals)
    node.Subquery(query) -> query.values
    _ -> []
  }
}

fn operator_values(op: ComparisonOperator(v)) -> List(v) {
  case op {
    Between(val) -> unwrap(val)
    _ -> []
  }
}

type ComparisonOperator(v) {
  Eq
  Gt
  Lt
  GtEq
  LtEq
  NotEq
  Between(Node(v))
  In
  Is
  IsNot
  IsNull
  IsNotNull
  Like
  NotLike
}

type LogicalOperator {
  And
  Or
}

pub fn to_string_tree(expr: Expr(v), format: Format(v)) -> StringTree {
  case expr {
    Compare(left, right, op) -> {
      let left = node.to_string_tree(left, format)
      let right = node.to_string_tree(right, format)

      let fmt = to_comp_fmt(op)
      fmt(left, right)
    }
    Logical(left, right, logical) -> {
      let left = to_string_tree(left, format)
      let right = to_string_tree(right, format)

      let fmt = to_logical_fmt(logical)
      fmt(left, right)
    }
    Not(expr) -> {
      string_tree.from_string("NOT ")
      |> string_tree.append_tree(to_string_tree(expr, format))
    }
  }
}

fn to_comp_fmt(
  operator: ComparisonOperator(v),
) -> fn(StringTree, StringTree) -> StringTree {
  case operator {
    Eq -> fmt.eq
    Gt -> fmt.gt
    Lt -> fmt.lt
    GtEq -> fmt.gt_eq
    LtEq -> fmt.lt_eq
    NotEq -> fmt.not_eq
    Like -> fmt.like
    In -> fmt.in
    Is -> fmt.is
    Between(_start) -> fn(left, _end) {
      let ph = string_tree.from_string(fmt.placeholder)

      fmt.between(left, ph, ph)
    }
    IsNot -> fmt.is_not
    IsNull -> fmt.is_null
    IsNotNull -> fmt.is_not_null
    NotLike -> fmt.not_like
  }
}

fn to_logical_fmt(
  operator: LogicalOperator,
) -> fn(StringTree, StringTree) -> StringTree {
  case operator {
    And -> fmt.and
    Or -> fmt.or
  }
}
