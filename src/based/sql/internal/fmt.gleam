import gleam/list
import gleam/string
import gleam/string_tree.{type StringTree}

pub const placeholder = ":param"

pub const null = "NULL"

// Statements

pub fn insert(
  st: StringTree,
  columns: List(String),
  into table: String,
  values values: List(StringTree),
) -> StringTree {
  st
  |> string_tree.append("INSERT INTO ")
  |> string_tree.append(table)
  |> string_tree.append(" ")
  |> string_tree.append_tree(
    columns
    |> list.map(string_tree.from_string)
    |> string_tree.join(with: ", ")
    |> enclose_tree,
  )
  |> string_tree.append(" VALUES ")
  |> string_tree.append_tree(values |> string_tree.join(", "))
}

pub fn update(st: StringTree, table: String) -> StringTree {
  st
  |> string_tree.append("UPDATE ")
  |> string_tree.append(table)
}

pub fn set(st: StringTree, updates: StringTree) -> StringTree {
  st
  |> string_tree.append(" SET ")
  |> string_tree.append_tree(updates)
}

pub fn delete(st: StringTree) -> StringTree {
  string_tree.append(st, "DELETE")
}

pub fn select(st: StringTree, values: List(String)) -> StringTree {
  let values =
    values
    |> list.map(string_tree.from_string)
    |> string_tree.join(with: ", ")

  st
  |> string_tree.append("SELECT ")
  |> string_tree.append_tree(values)
}

pub fn select_distinct(st: StringTree, values: List(String)) -> StringTree {
  let values =
    values
    |> list.map(string_tree.from_string)
    |> string_tree.join(with: ", ")

  st
  |> string_tree.append("SELECT DISTINCT ")
  |> string_tree.append_tree(values)
}

pub fn from(st: StringTree, value: String) -> StringTree {
  append(st, " FROM ", value)
}

// Where Clause

pub fn where(st: StringTree, value: StringTree) -> StringTree {
  append_tree(st, " WHERE ", value)
}

pub fn and(st: StringTree, value: StringTree) -> StringTree {
  append_tree(st, " AND ", value)
}

pub fn or(st: StringTree, value: StringTree) -> StringTree {
  append_tree(st, " OR ", value)
}

// SQL Joins

pub fn inner_join(st: StringTree, value: String) -> StringTree {
  append(st, " INNER JOIN ", value)
}

pub fn left_join(st: StringTree, value: String) -> StringTree {
  append(st, " LEFT JOIN ", value)
}

pub fn right_join(st: StringTree, value: String) -> StringTree {
  append(st, " RIGHT JOIN ", value)
}

pub fn full_outer_join(st: StringTree, value: String) -> StringTree {
  append(st, " FULL OUTER JOIN ", value)
}

pub fn on(st: StringTree, value: StringTree) -> StringTree {
  append_tree(st, " ON ", value)
}

// Operators

pub fn eq(st: StringTree, placeholder: StringTree) -> StringTree {
  append_tree(st, " = ", placeholder)
}

pub fn gt(st: StringTree, placeholder: StringTree) -> StringTree {
  append_tree(st, " > ", placeholder)
}

pub fn lt(st: StringTree, placeholder: StringTree) -> StringTree {
  append_tree(st, " < ", placeholder)
}

pub fn gt_eq(st: StringTree, placeholder: StringTree) -> StringTree {
  append_tree(st, " >= ", placeholder)
}

pub fn lt_eq(st: StringTree, placeholder: StringTree) -> StringTree {
  append_tree(st, " <= ", placeholder)
}

pub fn not_eq(st: StringTree, placeholder: StringTree) -> StringTree {
  append_tree(st, " <> ", placeholder)
}

pub fn not(st: StringTree) -> StringTree {
  string_tree.append(st, " NOT")
}

pub fn is_not(st: StringTree, placeholder: StringTree) -> StringTree {
  append_tree(st, " IS NOT ", placeholder)
}

pub fn is_null(st: StringTree, placeholder: StringTree) -> StringTree {
  append_tree(st, " IS NULL ", placeholder)
}

pub fn is_not_null(st: StringTree, placeholder: StringTree) -> StringTree {
  append_tree(st, " IS NOT NULL ", placeholder)
}

pub fn not_like(st: StringTree, placeholder: StringTree) -> StringTree {
  append_tree(st, " NOT LIKE ", placeholder)
}

pub fn like(st: StringTree, placeholder: StringTree) -> StringTree {
  append_tree(st, " LIKE ", placeholder)
}

pub fn in(st: StringTree, placeholder: StringTree) -> StringTree {
  append_tree(st, " IN ", placeholder)
}

pub fn is(st: StringTree, placeholder: StringTree) -> StringTree {
  append_tree(st, " IS ", placeholder)
}

pub fn between(st: StringTree, val1: StringTree, val2: StringTree) -> StringTree {
  st
  |> append_tree(" BETWEEN ", val1)
  |> append_tree(" AND ", val2)
}

pub fn any(st: StringTree, subquery: StringTree) -> StringTree {
  st
  |> string_tree.append(" ANY ")
  |> string_tree.append_tree(subquery)
}

pub fn all(st: StringTree, subquery: StringTree) -> StringTree {
  st
  |> string_tree.append(" ALL ")
  |> string_tree.append_tree(subquery)
}

pub fn some(st: StringTree, subquery: StringTree) -> StringTree {
  st
  |> string_tree.append(" SOME ")
  |> string_tree.append_tree(subquery)
}

pub fn exists(subquery: StringTree) -> String {
  string_tree.from_string("EXISTS ")
  |> string_tree.append_tree(subquery)
  |> string_tree.to_string
}

pub fn alias(value: String, alias: String) -> String {
  value <> " AS " <> alias
}

pub fn returning(st: StringTree, columns: List(String)) -> StringTree {
  let columns =
    columns
    |> list.map(string_tree.from_string)
    |> string_tree.join(", ")

  st
  |> string_tree.append(" RETURNING ")
  |> string_tree.append_tree(columns)
}

pub fn limit(st: StringTree, placeholder: String) -> StringTree {
  append(st, " LIMIT ", placeholder)
}

pub fn offset(st: StringTree, placeholder: String) -> StringTree {
  append(st, " OFFSET ", placeholder)
}

pub fn for_update(st: StringTree) -> StringTree {
  string_tree.append(st, " FOR UPDATE")
}

// SQL Functions

// Aggregate functions

pub fn count(value: String) -> String {
  "COUNT(" <> value <> ")"
}

pub fn group_by(st: StringTree, values: List(String)) -> StringTree {
  append(st, " GROUP BY ", string.join(values, with: ", "))
}

pub fn having(st: StringTree, value: StringTree) -> StringTree {
  append_tree(st, " HAVING ", value)
}

pub fn sum(value: String) -> String {
  string_tree.new()
  |> wrap("SUM", value)
  |> string_tree.to_string
}

pub fn avg(value: String) -> String {
  string_tree.new()
  |> wrap("AVG", value)
  |> string_tree.to_string
}

pub fn max(value: String) -> String {
  string_tree.new()
  |> wrap("MAX", value)
  |> string_tree.to_string
}

pub fn min(value: String) -> String {
  string_tree.new()
  |> wrap("MIN", value)
  |> string_tree.to_string
}

// Arithmetic functions

// Character functions

// Order By

pub fn order_by(st: StringTree, values: List(String)) -> StringTree {
  append(st, " ORDER BY ", string.join(values, with: ", "))
}

pub fn asc(st: StringTree) -> StringTree {
  string_tree.append(st, " ASC")
}

pub fn desc(st: StringTree) -> StringTree {
  string_tree.append(st, " DESC")
}

// Union

pub fn union(
  query_1: StringTree,
  query_2: StringTree,
  all all: Bool,
) -> StringTree {
  let keyword = case all {
    True -> " UNION ALL "
    False -> " UNION "
  }

  query_1
  |> string_tree.append(keyword)
  |> string_tree.append_tree(query_2)
}

// CTEs

pub fn with(st: StringTree, ctes: List(String)) -> StringTree {
  string.join(ctes, ", ") |> append(st, "WITH ", _)
}

pub fn with_recursive(st: StringTree, ctes: List(String)) -> StringTree {
  string.join(ctes, ", ") |> append(st, "WITH RECURSIVE ", _)
}

// Helpers

pub fn terminate(st: StringTree) -> StringTree {
  string_tree.append(st, ";")
}

pub fn enclose(str: String) -> String {
  "(" <> str <> ")"
}

pub fn enclose_tree(st: StringTree) -> StringTree {
  st
  |> string_tree.prepend("(")
  |> string_tree.append(")")
}

pub fn wrap(st: StringTree, keyword: String, value: String) -> StringTree {
  st
  |> string_tree.append(keyword)
  |> string_tree.append("(")
  |> string_tree.append(value)
  |> string_tree.append(")")
}

pub fn append(st: StringTree, keyword: String, value: String) -> StringTree {
  st
  |> string_tree.append(keyword)
  |> string_tree.append(value)
}

pub fn append_tree(
  st: StringTree,
  keyword: String,
  value: StringTree,
) -> StringTree {
  st
  |> string_tree.append(keyword)
  |> string_tree.append_tree(value)
}
