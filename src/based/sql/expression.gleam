import based/sql/internal/fmt
import based/sql/node.{type Node}
import gleam/list

// Expression

pub opaque type Expression(v) {
  Compare(left: Node(v), right: Node(v), operator: ComparisonOperator(v))
  Logical(left: Expression(v), right: Expression(v), operator: LogicalOperator)
  Not(expr: Expression(v))
  Is(left: Node(v), right: Bool)
  IsNull(left: Node(v), right: Bool)
  Raw(sql: String)
}

@internal
pub fn compare(
  left: Node(v),
  right: Node(v),
  operator: ComparisonOperator(v),
) -> Expression(v) {
  Compare(left:, right:, operator:)
}

@internal
pub fn logical(
  left: Expression(v),
  right: Expression(v),
  operator: LogicalOperator,
) -> Expression(v) {
  Logical(left:, right:, operator:)
}

@internal
pub fn not(expr: Expression(v)) -> Expression(v) {
  Not(expr:)
}

@internal
pub fn is(left: Node(v), right: Bool) -> Expression(v) {
  Is(left:, right:)
}

@internal
pub fn is_null(left: Node(v), right: Bool) -> Expression(v) {
  IsNull(left:, right:)
}

@internal
pub fn raw(sql: String) -> Expression(v) {
  Raw(sql)
}

@internal
pub fn to_values(
  expr: Expression(v),
  with handle_text: fn(String) -> v,
) -> List(v) {
  case expr {
    Compare(left, right, op) -> {
      let op_vals = case op {
        Between(val) -> node.to_values(val, handle_text)
        _ -> []
      }

      list.flatten([
        node.to_values(left, handle_text),
        op_vals,
        node.to_values(right, handle_text),
      ])
    }
    Logical(left, right, _) -> {
      list.flatten([to_values(left, handle_text), to_values(right, handle_text)])
    }
    Not(expr) -> to_values(expr, handle_text)
    Is(..) -> []
    IsNull(..) -> []
    Raw(..) -> []
  }
}

@internal
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

@internal
pub type LogicalOperator {
  And
  Or
}

@internal
pub fn to_string(
  expr: Expression(v),
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
    Raw(sql) -> sql
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
