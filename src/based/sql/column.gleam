import based/format.{type Format}
import based/sql/table.{type Table}
import gleam/option.{type Option, None, Some}

/// A column in a SQL query with optional table and alias.
pub opaque type Column(v) {
  Column(name: String, table: Option(Table(v)), alias: Option(String))
}

/// Create a new column with the given name.
pub fn new(name: String) -> Column(v) {
  Column(name:, table: None, alias: None)
}

/// Associate a column with a table.
pub fn for(col: Column(v), table: Table(v)) -> Column(v) {
  Column(..col, table: Some(table))
}

/// Set an alias for a column.
pub fn alias(col: Column(v), alias: String) -> Column(v) {
  Column(..col, alias: Some(alias))
}

/// Convert a column to a string representation using the given format.
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
