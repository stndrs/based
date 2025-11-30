import based/format.{type Format}
import based/sql/table.{type Table}
import gleam/option.{type Option, None, Some}
import gleam/string_tree.{type StringTree}

pub opaque type Column(v) {
  Column(name: String, table: Option(Table(v)), alias: Option(String))
}

pub fn new(name: String) -> Column(v) {
  Column(name:, table: None, alias: None)
}

pub fn for(col: Column(v), table: Table(v)) -> Column(v) {
  Column(..col, table: Some(table))
}

pub fn alias(col: Column(v), alias: String) -> Column(v) {
  Column(..col, alias: Some(alias))
}

pub fn to_string(column: Column(v), fmt: Format(v)) -> String {
  let col = case column.table {
    Some(tab) -> table.to_string(tab, fmt) <> "." <> column.name
    None -> column.name
  }

  case column.alias {
    Some(a) -> col <> " AS " <> a
    None -> col
  }
}

pub fn to_string_tree(column: Column(v), fmt: Format(v)) -> StringTree {
  column
  |> to_string(fmt)
  |> string_tree.from_string
}
