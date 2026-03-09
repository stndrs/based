import based/db
import based/repo.{type Repo}
import based/sql/internal/builder
import based/sql/internal/fmt
import based/sql/select.{type Select}
import gleam/list
import gleam/string

pub opaque type Union(v) {
  Union(repo: Repo(v), selects: List(Select(v)))
  UnionAll(repo: Repo(v), selects: List(Select(v)))
}

pub fn new(repo: Repo(v), selects: List(Select(v))) -> Union(v) {
  Union(repo:, selects:)
}

pub fn all(repo: Repo(v), selects: List(Select(v))) -> Union(v) {
  UnionAll(repo:, selects:)
}

pub fn to_query(union: Union(v)) -> db.Query(v) {
  let operator = case union {
    Union(..) -> " UNION "
    _ -> " UNION ALL "
  }

  union.selects
  |> list.map(select.build_query)
  |> to_union_query(union.repo, operator)
}

pub fn to_string(union: Union(v)) -> String {
  let operator = case union {
    Union(..) -> " UNION "
    _ -> " UNION ALL "
  }

  union.selects
  |> list.map(select.to_string)
  |> string.join(operator)
}

fn to_union_query(
  selects: List(db.Query(v)),
  repo: Repo(v),
  operator: String,
) -> db.Query(v) {
  let #(vals, sql) =
    selects
    |> list.map_fold([], with: fn(vals, select) {
      #(list.prepend(vals, select.values), select.sql)
    })

  let values = list.reverse(vals) |> list.flatten
  let to_placeholder = fn(idx) { fmt.to_placeholder(repo.fmt, idx) }

  sql
  |> string.join(operator)
  |> builder.placeholders(on: fmt.placeholder, with: to_placeholder)
  |> db.sql
  |> db.params(values)
}
