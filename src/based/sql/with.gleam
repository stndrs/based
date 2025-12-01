//// A builder for constructing Common Table Expressions

import based/db
import based/sql
import based/sql/internal/builder
import based/sql/internal/fmt
import gleam/list
import gleam/string
import gleam/string_tree.{type StringTree}

/// A WITH clause with recursive flag and CTEs.
pub opaque type With(v) {
  With(recursive: Bool, ctes: List(Cte(v)), query: db.Query(v))
}

/// A Common Table Expression (CTE) with name, columns, and query.
pub opaque type Cte(v) {
  Cte(name: String, columns: List(String), query: db.Query(v))
}

/// Create a new WITH clause with the given CTEs.
pub fn new(ctes: List(Cte(v))) -> With(v) {
  With(recursive: False, ctes:, query: db.sql(""))
}

/// Mark the WITH clause as recursive.
pub fn recursive(with: With(v)) -> With(v) {
  With(..with, recursive: True)
}

/// Create a CTE with the given name and query.
pub fn cte(name: String, query: db.Query(v)) {
  Cte(name:, columns: [], query:)
}

/// Add column names to a CTE.
pub fn columns(cte: Cte(v), columns: List(String)) -> Cte(v) {
  Cte(..cte, columns:)
}

/// Set or modify the main query of a WITH clause.
pub fn query(with: With(v), building: fn() -> db.Query(v)) -> With(v) {
  let query = building()

  With(..with, query:)
}

/// Convert a WITH clause to a database query using the given format.
pub fn to_query(with: With(v), format: sql.SqlFmt(v)) -> db.Query(v) {
  let values = list.flat_map(with.ctes, fn(cte) { cte.query.values })
  let values = list.flatten([values, with.query.values])

  let to_placeholder = sql.to_placeholder(format, _)

  build(with)
  |> builder.placeholders(on: fmt.placeholder(), with: to_placeholder)
  |> string_tree.to_string
  |> db.sql
  |> db.values(values)
}

/// Build a SQL string tree for a WITH clause.
fn build(with: With(v)) -> StringTree {
  let ctes = {
    use cte <- list.map(with.ctes)

    let name = case cte.columns {
      [] -> string_tree.from_string(cte.name)
      cols -> {
        let cols =
          string.join(cols, ", ")
          |> fmt.enclose

        cte.name
        |> string_tree.from_string
        |> string_tree.append(" ")
        |> string_tree.append_tree(cols)
      }
    }

    fmt.alias(name, fmt.enclose(cte.query.sql))
  }

  let with_fmt = case with.recursive {
    True -> fmt.with_recursive
    False -> fmt.with
  }

  string_tree.new()
  |> with_fmt(ctes)
  |> string_tree.append(" ")
  |> string_tree.append(with.query.sql)
  |> fmt.terminate
}
