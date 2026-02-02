//// A builder for constructing Common Table Expressions

import based/db
import based/repo.{type Repo}
import based/sql/internal/builder
import based/sql/internal/fmt
import gleam/list
import gleam/string

/// A WITH clause with recursive flag and CTEs.
pub opaque type With(v) {
  With(repo: Repo(v), recursive: Bool, ctes: List(Cte(v)), query: db.Query(v))
}

/// A Common Table Expression (CTE) with name, columns, and query.
pub opaque type Cte(v) {
  Cte(name: String, columns: List(String), query: db.Query(v))
}

/// Create a new WITH clause with the given CTEs.
pub fn new(repo: Repo(v), ctes: List(Cte(v))) -> With(v) {
  With(repo:, recursive: False, ctes:, query: db.sql(""))
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
pub fn query(with: With(v), building: fn(Repo(v)) -> db.Query(v)) -> With(v) {
  let query = building(with.repo)

  With(..with, query:)
}

/// Convert a WITH clause to a database query using the given fmt.
pub fn to_query(with: With(v)) -> db.Query(v) {
  let values = list.flat_map(with.ctes, fn(cte) { cte.query.values })
  let values = list.flatten([values, with.query.values])

  let to_placeholder = fmt.to_placeholder(with.repo.fmt, _)

  build(with)
  |> builder.placeholders(on: fmt.placeholder, with: to_placeholder)
  |> db.sql
  |> db.params(values)
}

/// Build a SQL string tree for a WITH clause.
fn build(with: With(v)) -> String {
  let ctes = {
    use cte <- list.map(with.ctes)

    let name = case cte.columns {
      [] -> cte.name
      cols -> {
        let cols =
          string.join(cols, ", ")
          |> fmt.enclose

        cte.name
        |> string.append(" ")
        |> string.append(cols)
      }
    }

    fmt.alias(name, fmt.enclose(cte.query.sql))
  }

  let with_fmt = case with.recursive {
    True -> fmt.with_recursive
    False -> fmt.with
  }

  with_fmt(ctes)
  |> string.append(" ")
  |> string.append(with.query.sql)
  |> fmt.terminate
}
