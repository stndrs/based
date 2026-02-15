import based/db
import based/repo.{type Repo}
import based/sql/column.{type Column}
import based/sql/condition.{type Condition}
import based/sql/internal/builder
import based/sql/internal/fmt
import based/sql/table
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/string

pub opaque type Insert(v) {
  Insert(
    repo: Repo(v),
    table: table.Table,
    columns: List(String),
    on_conflict: Option(#(OnConflict, List(v))),
    returning: List(Column),
    values: List(v),
  )
}

pub fn into(repo: Repo(v), table: table.Table) -> Insert(v) {
  Insert(
    repo:,
    table:,
    columns: [],
    on_conflict: None,
    returning: [],
    values: [],
  )
}

pub fn columns(insert: Insert(v), cols: List(String)) -> Insert(v) {
  Insert(..insert, columns: cols)
}

pub opaque type Value(v) {
  Value(column: String, value: v, next: Option(fn() -> Value(v)))
}

pub fn value(column: String, value: v, next: fn() -> Value(v)) -> Value(v) {
  Value(column:, value:, next: Some(next))
}

pub fn final(column: String, value: v) -> Value(v) {
  Value(column:, value:, next: None)
}

fn to_columns_and_values(value: Value(v)) -> #(List(String), List(v)) {
  case value.next {
    Some(next) -> {
      let #(columns, values) = next() |> to_columns_and_values

      #([value.column, ..columns], [value.value, ..values])
    }
    None -> #([value.column], [value.value])
  }
}

pub fn values(insert: Insert(v), values: List(Value(v))) -> Insert(v) {
  let #(columns, values) =
    values
    |> list.fold(from: #([], [[]]), with: fn(acc, value) {
      let #(columns, values) = to_columns_and_values(value)

      #(columns, [values, ..acc.1])
    })

  let values =
    values
    |> list.reverse
    |> list.flatten

  Insert(..insert, columns:, values:)
}

pub opaque type OnConflict {
  OnConflict(target: String, action: Action, where: List(Condition))
}

pub opaque type Action {
  Nothing
  Update(sets: List(Set))
}

pub opaque type Set {
  Set(column: String, value: String)
}

pub fn on_conflict(
  insert: Insert(v),
  target: String,
  do action: Action,
  where conditions: List(#(Condition, List(v))),
) -> Insert(v) {
  let #(conditions, values) =
    condition.split(conditions, insert.repo.value_mapper)

  let conflict = OnConflict(target:, action:, where: conditions)

  Insert(..insert, on_conflict: Some(#(conflict, values)))
}

pub const nothing = Nothing

pub fn update(sets: List(Set)) -> Action {
  Update(sets:)
}

pub fn set(column: String, value: String) -> Set {
  Set(column:, value:)
}

pub fn returning(insert: Insert(v), cols: List(Column)) -> Insert(v) {
  Insert(..insert, returning: cols)
}

pub fn to_query(insert: Insert(v)) -> db.Query(v) {
  let to_placeholder = fmt.to_placeholder(insert.repo.fmt, _)

  let values = case insert.on_conflict {
    Some(#(_, values)) -> list.flatten([insert.values, values])
    None -> insert.values
  }

  build(insert)
  |> builder.placeholders(on: fmt.placeholder, with: to_placeholder)
  |> db.sql
  |> db.params(values)
}

pub fn to_string(insert: Insert(v)) -> String {
  build(insert)
  |> builder.to_string(insert.values, insert.repo.fmt)
}

fn build(insert: Insert(v)) -> String {
  let values =
    insert.values
    |> list.map(fn(_) { fmt.placeholder })
    |> list.sized_chunk(into: list.length(insert.columns))
    |> list.map(fn(vals) {
      vals
      |> string.join(with: ", ")
      |> fmt.enclose
    })

  let into =
    insert.table
    |> table.to_string(fmt.to_identifier(insert.repo.fmt, _))

  let returning = list.map(insert.returning, column.to_string(_, insert.repo))

  let on_conflict = fn(sql) {
    insert.on_conflict
    |> option.map(fn(on_conflict) {
      let #(conflict, _values) = on_conflict

      let action = case conflict.action {
        Nothing -> fmt.do_nothing
        Update(sets:) -> fn(st) {
          let sets = list.map(sets, fn(s) { fmt.eq(s.column, s.value) })

          fmt.do_update(st, sets)
        }
      }

      sql
      |> builder.append_on_conflict(
        conflict.target,
        conflict.where,
        action,
        insert.repo.fmt,
      )
    })
    |> option.unwrap(sql)
  }

  fmt.insert(insert.columns, into:, values:)
  |> on_conflict
  |> builder.append_returning(returning)
}
