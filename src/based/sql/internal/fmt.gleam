import gleam/function
import gleam/string

pub opaque type Fmt(v) {
  Fmt(
    handle_identifier: fn(String) -> String,
    handle_placeholder: fn(Int) -> String,
    handle_value: fn(v) -> String,
  )
}

/// Returns a `Fmt(v)` record with handlers that does not apply any
/// formatting to identifiers, and returns `?` as placeholders. The value
/// handler's default behaviour is to panic since it handles a generic type.
pub fn new() -> Fmt(v) {
  Fmt(
    handle_identifier: function.identity,
    handle_placeholder: fn(_) { "?" },
    handle_value: fn(_) { panic as "based/format.Fmt not configured" },
  )
}

/// Sets the placeholder formatting function.
pub fn on_placeholder(
  fmt: Fmt(v),
  handle_placeholder: fn(Int) -> String,
) -> Fmt(v) {
  Fmt(..fmt, handle_placeholder:)
}

/// Set the identifier formatting function.
pub fn on_identifier(
  fmt: Fmt(v),
  handle_identifier: fn(String) -> String,
) -> Fmt(v) {
  Fmt(..fmt, handle_identifier:)
}

/// Set the value formatting function.
pub fn on_value(fmt: Fmt(v), handle_value: fn(v) -> String) -> Fmt(v) {
  Fmt(..fmt, handle_value:)
}

/// Apply the configured identifier format function to the provided identifier.
pub fn to_identifier(fmt: Fmt(v), identifier: String) -> String {
  fmt.handle_identifier(identifier)
}

/// Apply the configured value format function to the provided value.
pub fn to_string(fmt: Fmt(v), value: v) -> String {
  fmt.handle_value(value)
}

/// Apply the configured placeholder format function to the provided
/// placeholder index.
pub fn to_placeholder(fmt: Fmt(v), value: Int) -> String {
  fmt.handle_placeholder(value)
}

pub const placeholder = ":param"

pub const column = ":col:"

pub const value = ":val:"

pub const null = "NULL"

pub const true = "TRUE"

pub const false = "FALSE"

// Statements

pub fn insert(
  columns: List(String),
  into table: String,
  values values: List(String),
) -> String {
  "INSERT INTO "
  |> string.append(table)
  |> string.append(" ")
  |> string.append(
    columns
    |> string.join(with: ", ")
    |> enclose,
  )
  |> string.append(" VALUES ")
  |> string.append(values |> string.join(", "))
}

pub fn update(table: String) -> String {
  "UPDATE " <> table
}

pub fn set(st: String, updates: List(String)) -> String {
  st
  |> string.append(" SET ")
  |> string.append(string.join(updates, ", "))
}

pub const delete = "DELETE"

pub fn select(values: List(String)) -> String {
  let values =
    values
    |> string.join(with: ", ")

  "SELECT "
  |> string.append(values)
}

pub fn select_distinct(values: List(String)) -> String {
  let values =
    values
    |> string.join(with: ", ")

  "SELECT DISTINCT "
  |> string.append(values)
}

pub fn from(st: String, value: String) -> String {
  append(st, " FROM ", value)
}

// Where Clause

pub fn where(st: String, value: String) -> String {
  append(st, " WHERE ", value)
}

pub fn and(st: String, value: String) -> String {
  append(st, " AND ", value)
}

pub fn or(st: String, value: String) -> String {
  append(st, " OR ", value)
}

// SQL Joins

pub fn inner_join(st: String, value: String) -> String {
  append(st, " INNER JOIN ", value)
}

pub fn left_join(st: String, value: String) -> String {
  append(st, " LEFT JOIN ", value)
}

pub fn right_join(st: String, value: String) -> String {
  append(st, " RIGHT JOIN ", value)
}

pub fn full_outer_join(st: String, value: String) -> String {
  append(st, " FULL OUTER JOIN ", value)
}

pub fn on(st: String, value: String) -> String {
  append(st, " ON ", value)
}

// Operators

pub fn eq(st: String, placeholder: String) -> String {
  append(st, " = ", placeholder)
}

pub fn gt(st: String, placeholder: String) -> String {
  append(st, " > ", placeholder)
}

pub fn lt(st: String, placeholder: String) -> String {
  append(st, " < ", placeholder)
}

pub fn gt_eq(st: String, placeholder: String) -> String {
  append(st, " >= ", placeholder)
}

pub fn lt_eq(st: String, placeholder: String) -> String {
  append(st, " <= ", placeholder)
}

pub fn not_eq(st: String, placeholder: String) -> String {
  append(st, " <> ", placeholder)
}

pub fn not(right: String) -> String {
  string.append("NOT ", right)
}

pub fn is_not(st: String, placeholder: String) -> String {
  append(st, " IS NOT ", placeholder)
}

pub fn not_like(st: String, placeholder: String) -> String {
  append(st, " NOT LIKE ", placeholder)
}

pub fn like(st: String, placeholder: String) -> String {
  append(st, " LIKE ", placeholder)
}

pub fn in(st: String, placeholder: String) -> String {
  append(st, " IN ", placeholder)
}

pub fn is(st: String, placeholder: String) -> String {
  append(st, " IS ", placeholder)
}

pub fn between(st: String, val1: String, val2: String) -> String {
  st
  |> append(" BETWEEN ", val1)
  |> append(" AND ", val2)
}

pub fn any(subquery: String) -> String {
  "ANY " |> string.append(subquery)
}

pub fn all(subquery: String) -> String {
  "ALL " |> string.append(subquery)
}

pub fn some(st: String, subquery: String) -> String {
  st
  |> string.append(" SOME ")
  |> string.append(subquery)
}

pub fn exists(subquery: String) -> String {
  "EXISTS " |> string.append(subquery)
}

pub fn alias(value: String, alias: String) -> String {
  append(value, " AS ", alias)
}

pub fn returning(st: String, columns: List(String)) -> String {
  let columns =
    columns
    |> string.join(", ")

  st
  |> string.append(" RETURNING ")
  |> string.append(columns)
}

pub fn limit(st: String, placeholder: String) -> String {
  append(st, " LIMIT ", placeholder)
}

pub fn offset(st: String, placeholder: String) -> String {
  append(st, " OFFSET ", placeholder)
}

pub fn for_update(st: String) -> String {
  string.append(st, " FOR UPDATE")
}

// SQL Functions

// Aggregate functions

pub fn count(value: String) -> String {
  "COUNT(" <> value <> ")"
}

pub fn group_by(st: String, values: List(String)) -> String {
  append(st, " GROUP BY ", string.join(values, with: ", "))
}

pub fn having(st: String, value: String) -> String {
  append(st, " HAVING ", value)
}

pub fn sum(value: String) -> String {
  wrap("SUM", value)
}

pub fn avg(value: String) -> String {
  wrap("AVG", value)
}

pub fn max(value: String) -> String {
  wrap("MAX", value)
}

pub fn min(value: String) -> String {
  wrap("MIN", value)
}

// Arithmetic functions

// Character functions

// Order By

pub fn order_by(st: String, values: List(String)) -> String {
  append(st, " ORDER BY ", string.join(values, with: ", "))
}

pub fn asc(st: String) -> String {
  string.append(st, " ASC")
}

pub fn desc(st: String) -> String {
  string.append(st, " DESC")
}

// Union

pub fn union(query_1: String, query_2: String, all all: Bool) -> String {
  let keyword = case all {
    True -> " UNION ALL "
    False -> " UNION "
  }

  query_1
  |> string.append(keyword)
  |> string.append(query_2)
}

// CTEs

pub fn with(ctes: List(String)) -> String {
  string.join(ctes, ", ") |> string.append("WITH ", _)
}

pub fn with_recursive(ctes: List(String)) -> String {
  string.join(ctes, ", ") |> string.append("WITH RECURSIVE ", _)
}

// Helpers

pub fn terminate(st: String) -> String {
  string.append(st, ";")
}

pub fn enclose(str: String) -> String {
  "(" <> str <> ")"
}

pub fn wrap(st: String, value: String) -> String {
  st
  |> string.append("(")
  |> string.append(value)
  |> string.append(")")
}

pub fn append(st: String, keyword: String, value: String) -> String {
  st
  |> string.append(keyword)
  |> string.append(value)
}
