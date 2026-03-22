//// Internal module for pure SQL string fragment construction.
//// All functions are simple string concatenation helpers — no state,
//// no callbacks, no awareness of query structure or values.
////
//// This module is NOT part of the public API.

import gleam/int
import gleam/string

/// Sentinel marker inserted wherever a parameterized value belongs.
/// Replaced by a later pass with `$1`/`?` or literal values.
pub const placeholder = ":param:"

pub fn select(columns: String) -> String {
  "SELECT " <> columns
}

pub fn select_distinct(columns: String) -> String {
  "SELECT DISTINCT " <> columns
}

pub fn insert(
  into table: String,
  columns columns: List(String),
  values values: List(String),
) -> String {
  let columns = string.join(columns, ", ")
  let values = string.join(values, ", ")

  "INSERT INTO " <> table <> " (" <> columns <> ") VALUES " <> values
}

pub fn update(table: String) -> String {
  "UPDATE " <> table
}

pub fn delete(from table: String) -> String {
  "DELETE FROM " <> table
}

pub fn from(st: String, value: String) -> String {
  st <> " FROM " <> value
}

pub fn where(st: String, value: String) -> String {
  st <> " WHERE " <> value
}

pub fn set(st: String, assignments: String) -> String {
  st <> " SET " <> assignments
}

pub fn on(st: String, value: String) -> String {
  st <> " ON " <> value
}

pub fn returning(st: String, columns: String) -> String {
  st <> " RETURNING " <> columns
}

pub fn group_by(st: String, columns: String) -> String {
  st <> " GROUP BY " <> columns
}

pub fn having(st: String, value: String) -> String {
  st <> " HAVING " <> value
}

pub fn order_by(st: String, columns: String) -> String {
  st <> " ORDER BY " <> columns
}

pub fn limit(st: String, n: Int) -> String {
  st <> " LIMIT " <> int.to_string(n)
}

pub fn offset(st: String, n: Int) -> String {
  st <> " OFFSET " <> int.to_string(n)
}

pub fn for_update(st: String) -> String {
  st <> " FOR UPDATE"
}

pub fn on_conflict(st: String, target: String) -> String {
  st <> " ON CONFLICT (" <> target <> ")"
}

pub fn do_nothing(st: String) -> String {
  st <> " DO NOTHING"
}

pub fn do_update(st: String, assignments: List(String)) -> String {
  st <> " DO UPDATE SET " <> string.join(assignments, ", ")
}

pub fn inner_join(st: String, value: String) -> String {
  st <> " INNER JOIN " <> value
}

pub fn left_join(st: String, value: String) -> String {
  st <> " LEFT JOIN " <> value
}

pub fn right_join(st: String, value: String) -> String {
  st <> " RIGHT JOIN " <> value
}

pub fn full_join(st: String, value: String) -> String {
  st <> " FULL JOIN " <> value
}

pub fn eq(left: String, right: String) -> String {
  left <> " = " <> right
}

pub fn not_eq(left: String, right: String) -> String {
  left <> " != " <> right
}

pub fn gt(left: String, right: String) -> String {
  left <> " > " <> right
}

pub fn lt(left: String, right: String) -> String {
  left <> " < " <> right
}

pub fn gt_eq(left: String, right: String) -> String {
  left <> " >= " <> right
}

pub fn lt_eq(left: String, right: String) -> String {
  left <> " <= " <> right
}

pub fn like(left: String, right: String) -> String {
  left <> " LIKE " <> right
}

pub fn not_like(left: String, right: String) -> String {
  left <> " NOT LIKE " <> right
}

pub fn in_(left: String, values: String) -> String {
  left <> " IN (" <> values <> ")"
}

pub fn is_null(operand: String) -> String {
  operand <> " IS NULL"
}

pub fn is_not_null(operand: String) -> String {
  operand <> " IS NOT NULL"
}

pub fn is_true(operand: String) -> String {
  operand <> " IS TRUE"
}

pub fn is_false(operand: String) -> String {
  operand <> " IS FALSE"
}

pub fn between(operand: String, low: String, high: String) -> String {
  operand <> " BETWEEN " <> low <> " AND " <> high
}

pub fn not(value: String) -> String {
  "NOT (" <> value <> ")"
}

pub fn exists(subquery: String) -> String {
  "EXISTS (" <> subquery <> ")"
}

pub fn and_op(left: String, right: String) -> String {
  "(" <> left <> " AND " <> right <> ")"
}

pub fn or_op(left: String, right: String) -> String {
  "(" <> left <> " OR " <> right <> ")"
}

pub fn any(subquery: String) -> String {
  "ANY (" <> subquery <> ")"
}

pub fn all(subquery: String) -> String {
  "ALL (" <> subquery <> ")"
}

pub fn subquery(query: String) -> String {
  "(" <> query <> ")"
}

pub fn count(value: String) -> String {
  "COUNT(" <> value <> ")"
}

pub fn sum(value: String) -> String {
  "SUM(" <> value <> ")"
}

pub fn avg(value: String) -> String {
  "AVG(" <> value <> ")"
}

pub fn max(value: String) -> String {
  "MAX(" <> value <> ")"
}

pub fn min(value: String) -> String {
  "MIN(" <> value <> ")"
}

pub fn with_cte(ctes: String) -> String {
  "WITH " <> ctes
}

pub fn with_recursive(ctes: String) -> String {
  "WITH RECURSIVE " <> ctes
}

pub fn cte(name: String, body: String) -> String {
  name <> " AS (" <> body <> ")"
}

pub fn union(left: String, right: String) -> String {
  left <> " UNION " <> right
}

pub fn union_all(left: String, right: String) -> String {
  left <> " UNION ALL " <> right
}

/// Wrap a string in parentheses.
pub fn enclose(value: String) -> String {
  "(" <> value <> ")"
}

/// Append an alias: `value AS alias`.
pub fn alias_as(value: String, alias: String) -> String {
  value <> " AS " <> alias
}

/// Append ASC direction.
pub fn asc(value: String) -> String {
  value <> " ASC"
}

/// Append DESC direction.
pub fn desc(value: String) -> String {
  value <> " DESC"
}

/// Terminate a SQL statement with a semicolon.
pub fn terminate(value: String) -> String {
  value <> ";"
}

/// Join a list of strings with ", " separator.
pub fn comma_join(items: List(String)) -> String {
  string.join(items, ", ")
}

/// Wrap a value row in parentheses: "(a, b, c)".
pub fn value_row(values: String) -> String {
  "(" <> values <> ")"
}
