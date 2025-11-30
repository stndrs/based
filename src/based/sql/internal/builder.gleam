import based/sql
import based/sql/internal/fmt
import gleam/dict
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string_tree.{type StringTree}

pub fn to_string(sql: String, values: List(v), format: sql.SqlFmt(v)) -> String {
  let values_by_idx =
    values
    |> list.index_map(fn(val, idx) { #(idx + 1, val) })
    |> dict.from_list

  let with = fn(idx) {
    values_by_idx
    |> dict.get(idx)
    |> result.map(sql.to_string(format, _))
    |> result.unwrap("")
  }

  string_tree.from_string(sql)
  |> placeholders(on: fmt.placeholder, with:)
  |> string_tree.to_string
}

pub fn placeholders(
  for st: StringTree,
  on ph: String,
  with mapper: fn(Int) -> String,
) -> StringTree {
  string_tree.split(st, on: ph)
  |> list.index_fold(from: [], with: fn(acc, st1, idx) {
    case idx {
      0 -> [st1, ..acc]
      idx -> {
        let ph =
          idx
          |> mapper
          |> string_tree.from_string

        [st1, ph, ..acc]
      }
    }
  })
  |> list.reverse
  |> string_tree.join(with: "")
}

pub fn append_where(
  st: StringTree,
  where: List(List(sql.Expr(v))),
  format: sql.SqlFmt(v),
) -> StringTree {
  where
  |> list.reverse
  |> list.flatten
  |> list.index_fold(from: st, with: fn(sql1, expr, idx) {
    let expr_fmt = case idx {
      0 -> fmt.where
      _ -> fmt.and
    }

    expr
    |> sql.expr_to_string_tree(format)
    |> expr_fmt(sql1, _)
  })
}

pub fn append_group_by(st: StringTree, group_by: List(String)) -> StringTree {
  case group_by {
    [] -> st
    columns -> fmt.group_by(st, columns)
  }
}

pub fn append_having(
  st: StringTree,
  having: List(List(sql.Expr(v))),
  format: sql.SqlFmt(v),
) -> StringTree {
  having
  |> list.reverse
  |> list.flatten
  |> list.index_fold(from: st, with: fn(sql1, expr, idx) {
    let expr_fmt = case idx {
      0 -> fmt.having
      _ -> fmt.and
    }

    expr
    |> sql.expr_to_string_tree(format)
    |> expr_fmt(sql1, _)
  })
}

pub fn append_joins(
  st: StringTree,
  joins: List(sql.Join(v)),
  format: sql.SqlFmt(v),
) -> StringTree {
  joins
  |> list.reverse
  |> list.fold(from: st, with: fn(st, join) {
    let join_tree = case join.type_ {
      sql.InnerJoin -> fmt.inner_join
      sql.LeftJoin -> fmt.left_join
      sql.RightJoin -> fmt.right_join
      sql.FullJoin -> fmt.full_outer_join
    }

    st
    |> join_tree(sql.node_to_string(join.table, format))
    |> list.index_fold(over: join.exprs, from: _, with: fn(sql1, expr, idx) {
      let expr_fmt = case idx {
        0 -> fmt.on
        _ -> fmt.and
      }

      expr
      |> sql.expr_to_string_tree(format)
      |> expr_fmt(sql1, _)
    })
  })
}

pub fn append_order_by(
  st: StringTree,
  order_by: List(String),
  order: Option(sql.Order),
) -> StringTree {
  case order_by {
    [] -> st
    columns -> {
      let append_order = fn(st) {
        case order {
          Some(sql.Asc) -> fmt.asc(st)
          Some(sql.Desc) -> fmt.desc(st)
          None -> st
        }
      }

      fmt.order_by(st, columns) |> append_order
    }
  }
}

pub fn append_limit(
  st: StringTree,
  limit: Option(Int),
  offset: Option(Int),
) -> StringTree {
  limit
  |> option.map(fn(_) { fmt.limit(st, fmt.placeholder) })
  |> option.map(fn(lim) {
    offset
    |> option.map(fn(_) { fmt.offset(lim, fmt.placeholder) })
    |> option.unwrap(lim)
  })
  |> option.unwrap(st)
}

pub fn append_optional(
  st: StringTree,
  opt: Option(a),
  inner: fn(a) -> StringTree,
) -> StringTree {
  opt
  |> option.map(inner)
  |> option.unwrap(st)
}

pub fn append_returning(st: StringTree, cols: List(String)) -> StringTree {
  case cols {
    [] -> st
    cols -> fmt.returning(st, cols)
  }
}
