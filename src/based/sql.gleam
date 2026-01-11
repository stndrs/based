import based/db
import based/sql/internal/fmt
import based/sql/internal/node.{type Node}
import gleam/function
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/string

/// SqlFmt must be configured by adapter packages.
///
/// Example:
///
/// A PostgreSQL adapter might configure `SqlFmt` like this:
///
/// ```gleam
/// let fmt = format.new()
///   |> format.on_placeholder(fn(index) { "$" <> int.to_string(index) })
///   |> format.on_identifier(function.identifier)
///   |> format.on_value(value.to_string)
/// ```
/// A MariaDB adapter might configure `SqlFmt` like this:
///
/// ```gleam
/// let fmt = format.new()
///   |> format.on_placeholder(fn(_index) { "?" })
///   |> format.on_identifier(fn(ident) { "`" <> ident <> "`" })
///   |> format.on_value(value.to_string)
/// ```
///
pub opaque type SqlFmt(v) {
  SqlFmt(
    handle_identifier: fn(String) -> String,
    handle_placeholder: fn(Int) -> String,
    handle_value: fn(v) -> String,
  )
}

/// Returns a `SqlFmt(v)` record with handlers that does not apply any
/// formatting to identifiers, and returns `?` as placeholders. The value
/// handler's default behaviour is to panic since it handles a generic type.
pub fn format() -> SqlFmt(v) {
  SqlFmt(
    handle_identifier: function.identity,
    handle_placeholder: fn(_) { "?" },
    handle_value: fn(_) { panic as "based/format.SqlFmt not configured" },
  )
}

/// Apply the configured identifier format function to the provided identifier.
pub fn to_identifier(fmt: SqlFmt(v), identifier: String) -> String {
  fmt.handle_identifier(identifier)
}

/// Apply the configured value format function to the provided value.
pub fn to_string(fmt: SqlFmt(v), value: v) -> String {
  fmt.handle_value(value)
}

/// Apply the configured placeholder format function to the provided
/// placeholder index.
pub fn to_placeholder(fmt: SqlFmt(v), value: Int) -> String {
  fmt.handle_placeholder(value)
}

/// Sets the placeholder formatting function.
pub fn on_placeholder(
  fmt: SqlFmt(v),
  handle_placeholder: fn(Int) -> String,
) -> SqlFmt(v) {
  SqlFmt(..fmt, handle_placeholder:)
}

/// Set the identifier formatting function.
pub fn on_identifier(
  fmt: SqlFmt(v),
  handle_identifier: fn(String) -> String,
) -> SqlFmt(v) {
  SqlFmt(..fmt, handle_identifier:)
}

/// Set the value formatting function.
pub fn on_value(fmt: SqlFmt(v), handle_value: fn(v) -> String) -> SqlFmt(v) {
  SqlFmt(..fmt, handle_value:)
}

// Exprs

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

pub fn like(left: Node(v), val: String, of outer: fn(String) -> v) -> Expr(v) {
  let right = outer(val) |> value

  Compare(left:, right:, operator: Like)
}

pub fn in(left: Node(v), right: Node(v)) -> Expr(v) {
  Compare(left:, right:, operator: In)
}

pub fn is(left: Node(v), right: Bool) -> Expr(v) {
  Is(left:, right:)
}

pub fn is_null(left: Node(v)) -> Expr(v) {
  IsNull(left:, right: True)
}

pub fn is_not_null(left: Node(v)) -> Expr(v) {
  IsNull(left:, right: False)
}

pub fn or(left: Expr(v), right: Expr(v)) -> Expr(v) {
  Logical(left:, right:, operator: Or)
}

pub fn and(left: Expr(v), right: Expr(v)) -> Expr(v) {
  Logical(left:, right:, operator: And)
}

pub fn not(expr: Expr(v)) -> Expr(v) {
  Not(expr:)
}

pub fn not_like(
  left: Node(v),
  val: String,
  of outer: fn(String) -> v,
) -> Expr(v) {
  let right = outer(val) |> value

  Compare(left:, right:, operator: NotLike)
}

// Node

pub type Order {
  Asc
  Desc
}

@internal
pub fn table_to_node(table: Table(v)) -> Node(v) {
  case table {
    Table(identifier:) -> {
      identifier
      |> identifier_to_string
      |> node.TableRef
    }
    Subquery(query:, alias:) -> node.Query(query:, alias:)
  }
}

pub fn tuples(vals: List(List(Node(v)))) -> Node(v) {
  vals
  |> list.map(list.flat_map(_, node.unwrap))
  |> node.Tuples
}

pub fn list(vals: List(a), of inner_type: fn(a) -> v) -> Node(v) {
  list.map(vals, inner_type)
  |> node.Values
}

pub fn columns(names: List(String)) -> Node(v) {
  node.Columns(names)
}

pub fn value(value: v) -> Node(v) {
  node.Value(value)
}

pub fn nullable(value: Option(a), inner_type: fn(a) -> Node(v)) -> Node(v) {
  case value {
    Some(term) -> inner_type(term)
    None -> node.Null(True)
  }
}

@internal
pub fn subquery(query: db.Query(v)) -> Node(v) {
  node.Query(query:, alias: None)
}

pub fn values(values: List(v)) -> Node(v) {
  node.Values(values)
}

// @internal
// pub fn node_to_string(node: Node(v), format: SqlFmt(v)) -> String {
//   case node {
//     TableRef(identifier) -> {
//       let ident =
//         format
//         |> to_identifier(identifier.name)
// 
//       case identifier.alias {
//         Some(alias) -> fmt.alias(ident, alias)
//         None -> ident
//       }
//     }
//     ColumnRef(identifier) -> {
//       identifier_to_string(identifier, format)
//       |> to_identifier(format, _)
//     }
//     Columns(identifiers) ->
//       list.map(identifiers, fn(ident) {
//         ident
//         |> identifier_to_string(format)
//         |> to_identifier(format, _)
//       })
//       |> string.join(", ")
//       |> fmt.enclose
//     Value(_val) -> fmt.placeholder
//     Values(values) -> {
//       values
//       |> list.map(fn(_value) { fmt.placeholder })
//       |> string.join(", ")
//       |> fmt.enclose
//     }
//     Tuples(tuples) -> {
//       tuples
//       |> list.map(fn(vals) {
//         list.map(vals, fn(_) { fmt.placeholder })
//         |> string.join(", ")
//         |> fmt.enclose
//       })
//       |> string.join(", ")
//       |> fmt.enclose
//     }
//     Query(query:, alias: _) -> fmt.enclose(query.sql)
//     Null(val) -> {
//       case val {
//         True -> fmt.null
//         False -> "NOT NULL"
//       }
//     }
//   }
// }

// Expr

pub opaque type Expr(v) {
  Compare(left: Node(v), right: Node(v), operator: ComparisonOperator(v))
  Logical(left: Expr(v), right: Expr(v), operator: LogicalOperator)
  Not(expr: Expr(v))
  Is(left: Node(v), right: Bool)
  IsNull(left: Node(v), right: Bool)
}

@internal
pub fn expr_to_values(expr: Expr(v)) -> List(v) {
  case expr {
    Compare(left, right, op) -> {
      list.flatten([node.unwrap(left), operator_values(op), node.unwrap(right)])
    }
    Logical(left, right, _) -> {
      list.flatten([expr_to_values(left), expr_to_values(right)])
    }
    Not(expr) -> expr_to_values(expr)
    Is(..) -> []
    IsNull(..) -> []
  }
}

fn operator_values(op: ComparisonOperator(v)) -> List(v) {
  case op {
    Between(val) -> node.unwrap(val)
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
  Like
  NotLike
}

type LogicalOperator {
  And
  Or
}

@internal
pub fn expr_to_string(expr: Expr(v), format: SqlFmt(v)) -> String {
  case expr {
    Compare(left, right, op) -> {
      let left = node.to_string(left, format.handle_identifier)
      let right = node.to_string(right, format.handle_identifier)

      let fmt = to_comp_fmt(op)
      fmt(left, right)
    }
    Logical(left, right, logical) -> {
      let left = expr_to_string(left, format)
      let right = expr_to_string(right, format)

      let fmt = to_logical_fmt(logical)
      fmt(left, right)
    }
    Not(expr) -> {
      expr
      |> expr_to_string(format)
      |> fmt.not
    }
    Is(left:, right:) -> {
      let left = node.to_string(left, format.handle_identifier)

      let fmt = case right {
        True -> fmt.is(_, fmt.true)
        False -> fmt.is(_, fmt.false)
      }

      fmt(left)
    }
    IsNull(left:, right:) -> {
      let left = node.to_string(left, format.handle_identifier)

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

// Identifier

pub opaque type Identifier {
  Identifier(name: String, alias: Option(String), attr: Option(String))
}

pub fn identifier(name: String) -> Identifier {
  Identifier(name:, alias: None, attr: None)
}

pub fn alias(identifier: Identifier, alias: String) -> Identifier {
  Identifier(..identifier, alias: Some(alias))
}

pub fn attr(identifier: Identifier, attr: String) -> Identifier {
  Identifier(..identifier, attr: Some(attr))
}

pub fn column(identifier: Identifier) -> Node(v) {
  identifier
  |> identifier_to_string
  |> node.ColumnRef
}

pub fn table(identifier: Identifier) -> Table(v) {
  Table(identifier:)
}

fn identifier_to_string(identifier: Identifier) -> String {
  let ident = case identifier.attr {
    Some(other) -> {
      let attr =
        Identifier(name: other, alias: None, attr: None)
        |> table
        |> table_to_node
        |> node.to_string(function.identity)

      identifier.name
      |> string.append(".")
      |> string.append(attr)
    }
    None -> identifier.name
  }

  case identifier.alias {
    Some(a) -> fmt.alias(ident, a)
    None -> ident
  }
}

// Table

pub opaque type Table(v) {
  Table(identifier: Identifier)
  Subquery(query: db.Query(v), alias: Option(String))
}

@internal
pub fn from_query(query: db.Query(v)) -> Table(v) {
  Subquery(query, alias: None)
}

@internal
pub fn table_to_values(table: Table(v)) -> List(v) {
  case table {
    Table(..) -> []
    Subquery(query, _) -> query.values
  }
}
