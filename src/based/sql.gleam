import based/db
import based/sql/internal/fmt
import gleam/function
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/string_tree.{type StringTree}

/// SqlFmt must be configured by adapter packages.
///
/// Example:
///
/// A PostgreSQL adapter might configure `SqlFmt` like this:
///
/// ```gleam
/// let fmt = format.new()
///   |> format.on_placeholder(fn(index) {
///     string_tree.from_string("$")
///     |> string_tree.append(int.to_string(index))
///     "$" <> int.to_string(index)
///   })
///   |> format.on_identifier(function.identifier)
///   |> format.on_value(value.to_string_tree)
/// ```
/// A MariaDB adapter might configure `SqlFmt` like this:
///
/// ```gleam
/// let fmt = format.new()
///   |> format.on_placeholder(fn(_index) { "?" })
///   |> format.on_identifier(fn(ident) { "`" <> ident <> "`" })
///   |> format.on_value(value.to_string_tree)
/// ```
///
pub opaque type SqlFmt(v) {
  SqlFmt(
    handle_identifier: fn(StringTree) -> StringTree,
    handle_placeholder: fn(Int) -> StringTree,
    handle_value: fn(v) -> StringTree,
  )
}

/// Returns a `SqlFmt(v)` record with handlers that does not apply any
/// formatting to identifiers, and returns `?` as placeholders. The value
/// handler's default behaviour is to panic since it handles a generic type.
pub fn format() -> SqlFmt(v) {
  SqlFmt(
    handle_identifier: function.identity,
    handle_placeholder: fn(_) { string_tree.from_string("?") },
    handle_value: fn(_) { panic as "based/format.SqlFmt not configured" },
  )
}

/// Apply the configured identifier format function to the provided identifier.
pub fn to_identifier(fmt: SqlFmt(v), identifier: StringTree) -> StringTree {
  fmt.handle_identifier(identifier)
}

/// Apply the configured value format function to the provided value.
pub fn to_string(fmt: SqlFmt(v), value: v) -> StringTree {
  fmt.handle_value(value)
}

/// Apply the configured placeholder format function to the provided
/// placeholder index.
pub fn to_placeholder(fmt: SqlFmt(v), value: Int) -> StringTree {
  fmt.handle_placeholder(value)
}

/// Sets the placeholder formatting function.
pub fn on_placeholder(
  fmt: SqlFmt(v),
  handle_placeholder: fn(Int) -> StringTree,
) -> SqlFmt(v) {
  SqlFmt(..fmt, handle_placeholder:)
}

/// Set the identifier formatting function.
pub fn on_identifier(
  fmt: SqlFmt(v),
  handle_identifier: fn(StringTree) -> StringTree,
) -> SqlFmt(v) {
  SqlFmt(..fmt, handle_identifier:)
}

/// Set the value formatting function.
pub fn on_value(fmt: SqlFmt(v), handle_value: fn(v) -> StringTree) -> SqlFmt(v) {
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

pub fn like(
  left: Node(v),
  value: String,
  of outer: fn(String) -> Node(v),
) -> Expr(v) {
  let right = outer(value)

  Compare(left:, right:, operator: Like)
}

pub fn in(left: Node(v), right: Node(v)) -> Expr(v) {
  Compare(left:, right:, operator: In)
}

pub fn is(left: Node(v), right: Node(v)) -> Expr(v) {
  Compare(left:, right:, operator: Is)
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
  value: String,
  of outer: fn(String) -> Node(v),
) -> Expr(v) {
  let right = outer(value)

  Compare(left:, right:, operator: NotLike)
}

// Node

pub type Order {
  Asc
  Desc
}

pub opaque type Node(v) {
  TableRef(Identifier)
  ColumnRef(Identifier)
  Columns(List(Identifier))
  Value(v)
  Values(List(v))
  Tuples(List(List(v)))
  Query(query: db.Query(v), alias: Option(String))
  Null
}

pub fn table_to_node(table: Table(v)) -> Node(v) {
  case table {
    Table(identifier:) -> TableRef(identifier)
    Subquery(query:, alias:) -> Query(query:, alias:)
  }
}

pub fn tuples(vals: List(List(Node(v)))) -> Node(v) {
  vals
  |> list.map(list.flat_map(_, unwrap))
  |> Tuples
}

pub fn list(vals: List(a), of inner_type: fn(a) -> Node(v)) -> Node(v) {
  list.map(vals, inner_type)
  |> list.flat_map(unwrap)
  |> Values
}

pub fn columns(names: List(String)) -> Node(v) {
  {
    use name <- list.map(names)

    name
    |> string_tree.from_string
    |> Identifier(None, None)
  }
  |> Columns
}

pub fn value(value: a, of kind: fn(a) -> v) -> Node(v) {
  Value(kind(value))
}

pub fn nullable(value: Option(a), inner_type: fn(a) -> Node(v)) -> Node(v) {
  case value {
    Some(term) -> inner_type(term)
    None -> Null
  }
}

pub fn subquery(query: db.Query(v)) -> Node(v) {
  Query(query:, alias: None)
}

pub fn values(values: List(v)) -> Node(v) {
  Values(values)
}

pub fn unwrap(node: Node(v)) -> List(v) {
  case node {
    Value(val) -> [val]
    Values(vals) -> vals
    Tuples(vals) -> list.flatten(vals)
    Query(query:, alias: _) -> query.values
    _ -> []
  }
}

pub fn node_to_string_tree(node: Node(v), format: SqlFmt(v)) -> StringTree {
  case node {
    TableRef(identifier) -> {
      let ident =
        format
        |> to_identifier(identifier.name)

      case identifier.alias {
        Some(alias) -> fmt.alias(ident, alias)
        None -> ident
      }
    }
    ColumnRef(identifier) -> {
      identifier_to_string_tree(identifier, format)
      |> to_identifier(format, _)
    }
    Columns(identifiers) ->
      list.map(identifiers, fn(ident) {
        ident
        |> identifier_to_string_tree(format)
        |> to_identifier(format, _)
      })
      |> string_tree.join(", ")
      |> fmt.enclose_tree
    Value(_val) -> fmt.placeholder()
    Values(values) -> {
      values
      |> list.map(fn(_value) { fmt.placeholder() })
      |> string_tree.join(", ")
      |> fmt.enclose_tree
    }
    Tuples(tuples) -> {
      tuples
      |> list.map(fn(vals) {
        list.map(vals, fn(_) { fmt.placeholder() })
        |> string_tree.join(", ")
        |> fmt.enclose_tree
      })
      |> string_tree.join(", ")
      |> fmt.enclose_tree
    }
    Query(query:, alias: _) -> fmt.enclose(query.sql)
    Null -> fmt.null()
  }
}

// Expr

pub opaque type Expr(v) {
  Compare(left: Node(v), right: Node(v), operator: ComparisonOperator(v))
  Logical(left: Expr(v), right: Expr(v), operator: LogicalOperator)
  Not(expr: Expr(v))
}

pub fn expr_to_values(expr: Expr(v)) -> List(v) {
  case expr {
    Compare(left, right, op) -> {
      list.flatten([unwrap(left), operator_values(op), unwrap(right)])
    }
    Logical(left, right, _) -> {
      list.flatten([expr_to_values(left), expr_to_values(right)])
    }
    Not(expr) -> expr_to_values(expr)
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

pub fn expr_to_string_tree(expr: Expr(v), format: SqlFmt(v)) -> StringTree {
  case expr {
    Compare(left, right, op) -> {
      let left = node_to_string_tree(left, format)
      let right = node_to_string_tree(right, format)

      let fmt = to_comp_fmt(op)
      fmt(left, right)
    }
    Logical(left, right, logical) -> {
      let left = expr_to_string_tree(left, format)
      let right = expr_to_string_tree(right, format)

      let fmt = to_logical_fmt(logical)
      fmt(left, right)
    }
    Not(expr) -> {
      string_tree.from_string("NOT ")
      |> string_tree.append_tree(expr_to_string_tree(expr, format))
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
      let ph = fmt.placeholder()

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

// Join

pub type JoinType {
  InnerJoin
  LeftJoin
  RightJoin
  FullJoin
}

pub type Join(v) {
  Join(type_: JoinType, table: Node(v), exprs: List(Expr(v)))
}

pub fn inner(table: Table(v), exprs: List(Expr(v))) -> Join(v) {
  let table = table_to_node(table)

  Join(InnerJoin, table, exprs)
}

pub fn left(table: Table(v), exprs: List(Expr(v))) -> Join(v) {
  let table = table_to_node(table)

  Join(LeftJoin, table, exprs)
}

pub fn right(table: Table(v), exprs: List(Expr(v))) -> Join(v) {
  let table = table_to_node(table)

  Join(RightJoin, table, exprs)
}

pub fn full(table: Table(v), exprs: List(Expr(v))) -> Join(v) {
  let table = table_to_node(table)

  Join(FullJoin, table, exprs)
}

// Identifier

pub opaque type Identifier {
  Identifier(
    name: StringTree,
    alias: Option(StringTree),
    other: Option(Identifier),
  )
}

pub fn name(name: String) -> Identifier {
  Identifier(name: string_tree.from_string(name), alias: None, other: None)
}

pub fn alias(identifier: Identifier, alias: String) -> Identifier {
  Identifier(..identifier, alias: Some(string_tree.from_string(alias)))
}

pub fn column(identifier: Identifier) -> Node(v) {
  ColumnRef(identifier)
}

pub fn table(identifier: Identifier) -> Table(v) {
  Table(identifier:)
}

fn identifier_to_string_tree(
  identifier: Identifier,
  fmt: SqlFmt(v),
) -> StringTree {
  let ident = case identifier.other {
    Some(other) -> {
      table(other)
      |> table_to_node
      |> node_to_string_tree(fmt)
      |> string_tree.append(".")
      |> string_tree.append_tree(identifier.name)
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

pub fn attribute(table: Table(v), name: String) -> Identifier {
  case table {
    Table(Identifier(table_name, alias, _other)) -> {
      let table_name = alias |> option.unwrap(table_name)
      let other = Identifier(name: table_name, alias: None, other: None) |> Some

      Identifier(name: string_tree.from_string(name), alias: None, other:)
    }
    Subquery(_, alias:) -> {
      let table_name = alias |> option.unwrap("") |> string_tree.from_string
      let other = Identifier(name: table_name, alias: None, other: None) |> Some

      Identifier(name: string_tree.from_string(name), alias: None, other:)
    }
  }
}

pub fn from_query(query: db.Query(v)) -> Table(v) {
  Subquery(query, alias: None)
}

pub fn table_to_values(table: Table(v)) -> List(v) {
  case table {
    Table(..) -> []
    Subquery(query, _) -> query.values
  }
}
