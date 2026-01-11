import based/sql/internal/fmt
import based/sql/internal/node.{type Node}
import gleam/list

// Expr

pub opaque type Expr(v) {
  Compare(left: Node(v), right: Node(v), operator: ComparisonOperator(v))
  Logical(left: Expr(v), right: Expr(v), operator: LogicalOperator)
  Not(expr: Expr(v))
  Is(left: Node(v), right: Bool)
  IsNull(left: Node(v), right: Bool)
}

pub fn compare(
  left: Node(v),
  right: Node(v),
  operator: ComparisonOperator(v),
) -> Expr(v) {
  Compare(left:, right:, operator:)
}

pub fn logical(
  left: Expr(v),
  right: Expr(v),
  operator: LogicalOperator,
) -> Expr(v) {
  Logical(left:, right:, operator:)
}

pub fn not(expr: Expr(v)) -> Expr(v) {
  Not(expr:)
}

pub fn is(left: Node(v), right: Bool) -> Expr(v) {
  Is(left:, right:)
}

pub fn is_null(left: Node(v), right: Bool) -> Expr(v) {
  IsNull(left:, right:)
}

pub fn to_values(expr: Expr(v)) -> List(v) {
  case expr {
    Compare(left, right, op) -> {
      let op_vals = case op {
        Between(val) -> node.unwrap(val)
        _ -> []
      }

      list.flatten([node.unwrap(left), op_vals, node.unwrap(right)])
    }
    Logical(left, right, _) -> {
      list.flatten([to_values(left), to_values(right)])
    }
    Not(expr) -> to_values(expr)
    Is(..) -> []
    IsNull(..) -> []
  }
}

pub type ComparisonOperator(v) {
  Eq
  Gt
  Lt
  GtEq
  LtEq
  NotEq
  Between(Node(v))
  In
  Like
  NotLike
}

pub type LogicalOperator {
  And
  Or
}

pub fn to_string(
  expr: Expr(v),
  with handle_identifier: fn(String) -> String,
) -> String {
  case expr {
    Compare(left, right, op) -> {
      let left = node.to_string(left, handle_identifier)
      let right = node.to_string(right, handle_identifier)

      let fmt = to_comp_fmt(op)
      fmt(left, right)
    }
    Logical(left, right, logical) -> {
      let left = to_string(left, handle_identifier)
      let right = to_string(right, handle_identifier)

      let fmt = to_logical_fmt(logical)
      fmt(left, right)
    }
    Not(expr) -> {
      expr
      |> to_string(handle_identifier)
      |> fmt.not
    }
    Is(left:, right:) -> {
      let left = node.to_string(left, handle_identifier)

      let fmt = case right {
        True -> fmt.is(_, fmt.true)
        False -> fmt.is(_, fmt.false)
      }

      fmt(left)
    }
    IsNull(left:, right:) -> {
      let left = node.to_string(left, handle_identifier)

      let fmt = case right {
        True -> fmt.is
        False -> fmt.is_not
      }

      fmt(left, fmt.null)
    }
  }
}

fn to_comp_fmt(operator: ComparisonOperator(v)) -> fn(String, String) -> String {
  case operator {
    Eq -> fmt.eq
    Gt -> fmt.gt
    Lt -> fmt.lt
    GtEq -> fmt.gt_eq
    LtEq -> fmt.lt_eq
    NotEq -> fmt.not_eq
    Like -> fmt.like
    In -> fmt.in
    Between(_start) -> fn(left, _end) {
      let ph = fmt.placeholder

      fmt.between(left, ph, ph)
    }
    NotLike -> fmt.not_like
  }
}

fn to_logical_fmt(operator: LogicalOperator) -> fn(String, String) -> String {
  case operator {
    And -> fmt.and
    Or -> fmt.or
  }
}
