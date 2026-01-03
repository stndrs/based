import based/db
import based/sql
import based/sql/select.{type Select}
import gleam/list
import gleam/string

pub opaque type Union(v) {
  Union(selects: List(Select(v)))
  UnionAll(selects: List(Select(v)))
}

pub fn new(selects: List(Select(v))) -> Union(v) {
  Union(selects)
}

pub fn all(selects: List(Select(v))) -> Union(v) {
  UnionAll(selects)
}

pub fn to_query(union: Union(v), format: sql.SqlFmt(v)) -> db.Query(v) {
  let operator = case union {
    Union(..) -> " UNION "
    _ -> " UNION ALL "
  }

  union.selects
  |> list.map(select.to_query(_, format))
  |> to_union_query(operator)
}

pub fn to_string(union: Union(v), format: sql.SqlFmt(v)) -> String {
  let operator = case union {
    Union(..) -> " UNION "
    _ -> " UNION ALL "
  }

  union.selects
  |> list.map(select.to_string(_, format))
  |> string.join(operator)
}

fn to_union_query(selects: List(db.Query(v)), operator: String) -> db.Query(v) {
  let #(vals, sql) =
    selects
    |> list.map_fold([], with: fn(vals, select) {
      #(list.prepend(vals, select.values), select.sql)
    })

  let values = list.reverse(vals) |> list.flatten

  sql
  |> string.join(operator)
  |> db.sql
  |> db.params(values)
}
