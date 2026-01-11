import based/sql
import based/sql/internal/expr
import based/sql/internal/fmt
import based/sql/internal/join.{type Join}
import based/sql/internal/node
import based/sql/internal/table
import gleam/dict
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string

pub fn to_string(sql: String, values: List(v), format: sql.Sql(v)) -> String {
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

  sql
  |> placeholders(on: fmt.placeholder, with:)
}

pub fn placeholders(
  for st: String,
  on ph: String,
  with mapper: fn(Int) -> String,
) -> String {
  string.split(st, on: ph)
  |> list.index_fold(from: [], with: fn(acc, st1, idx) {
    case idx {
      0 -> [st1, ..acc]
      idx -> {
        let ph = mapper(idx)

        [st1, ph, ..acc]
      }
    }
  })
  |> list.reverse
  |> string.join(with: "")
}

pub fn append_where(
  st: String,
  where: List(List(sql.Expr(v))),
  format: sql.Sql(v),
) -> String {
  where
  |> list.reverse
  |> list.flatten
  |> list.index_fold(from: st, with: fn(sql1, expr, idx) {
    let expr_fmt = case idx {
      0 -> fmt.where
      _ -> fmt.and
    }

    expr
    |> expr.to_string(sql.to_identifier(format, _))
    |> expr_fmt(sql1, _)
  })
}

pub fn append_group_by(st: String, group_by: List(String)) -> String {
  case group_by {
    [] -> st
    columns -> fmt.group_by(st, columns)
  }
}

pub fn append_having(
  st: String,
  having: List(List(sql.Expr(v))),
  format: sql.Sql(v),
) -> String {
  having
  |> list.reverse
  |> list.flatten
  |> list.index_fold(from: st, with: fn(sql1, expr, idx) {
    let expr_fmt = case idx {
      0 -> fmt.having
      _ -> fmt.and
    }

    expr
    |> expr.to_string(sql.to_identifier(format, _))
    |> expr_fmt(sql1, _)
  })
}

pub fn append_joins(
  st: String,
  joins: List(Join(v)),
  format: sql.Sql(v),
) -> String {
  joins
  |> list.reverse
  |> list.fold(from: st, with: fn(st, join) {
    let join_tree = case join.type_ {
      join.InnerJoin -> fmt.inner_join
      join.LeftJoin -> fmt.left_join
      join.RightJoin -> fmt.right_join
      join.FullJoin -> fmt.full_outer_join
    }

    let table_node = table.to_node(join.table)

    st
    |> join_tree(node.to_string(table_node, with: sql.to_identifier(format, _)))
    |> list.index_fold(over: join.exprs, from: _, with: fn(sql1, expr, idx) {
      let expr_fmt = case idx {
        0 -> fmt.on
        _ -> fmt.and
      }

      expr
      |> expr.to_string(sql.to_identifier(format, _))
      |> expr_fmt(sql1, _)
    })
  })
}

pub fn append_order_by(
  st: String,
  order_by: List(String),
  order: Option(sql.Order),
) -> String {
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
  st: String,
  limit: Option(Int),
  offset: Option(Int),
) -> String {
  limit
  |> option.map(fn(_) { fmt.limit(st, fmt.placeholder) })
  |> option.map(fn(lim) {
    offset
    |> option.map(fn(_) { fmt.offset(lim, fmt.placeholder) })
    |> option.unwrap(lim)
  })
  |> option.unwrap(st)
}

pub fn append_returning(st: String, cols: List(String)) -> String {
  case cols {
    [] -> st
    cols -> fmt.returning(st, cols)
  }
}
