import based/db
import based/format.{type Format}
import based/sql/internal/builder
import based/sql/internal/fmt
import gleam/list
import gleam/string
import gleam/string_tree.{type StringTree}

pub opaque type With(v) {
  With(recursive: Bool, ctes: List(Cte(v)), query: db.Query(v))
}

pub opaque type Cte(v) {
  Cte(name: String, columns: List(String), query: db.Query(v))
}

pub fn new(ctes: List(Cte(v))) -> With(v) {
  With(recursive: False, ctes:, query: db.sql(""))
}

pub fn recursive(with: With(v)) -> With(v) {
  With(..with, recursive: True)
}

pub fn cte(name: String, query: db.Query(v)) {
  Cte(name:, columns: [], query:)
}

pub fn columns(cte: Cte(v), columns: List(String)) -> Cte(v) {
  Cte(..cte, columns:)
}

pub fn query(with: With(v), building: fn() -> db.Query(v)) -> With(v) {
  let query = building()

  With(..with, query:)
}

pub fn to_query(with: With(v), format: Format(v)) -> db.Query(v) {
  let values = list.flat_map(with.ctes, fn(cte) { cte.query.values })
  let values = list.flatten([values, with.query.values])

  let to_placeholder = format.to_placeholder(format, _)

  build(with)
  |> builder.placeholders(on: fmt.placeholder, with: to_placeholder)
  |> string_tree.to_string
  |> db.sql
  |> db.values(values)
}

fn build(with: With(v)) -> StringTree {
  let ctes =
    with.ctes
    |> list.map(fn(cte) {
      let name = case cte.columns {
        [] -> cte.name
        cols -> {
          let cols = string.join(cols, ", ") |> fmt.enclose
          cte.name <> " " <> cols
        }
      }

      fmt.alias(name, fmt.enclose(cte.query.sql))
    })

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
