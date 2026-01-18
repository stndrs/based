import based/sql/internal/fmt
import gleam/option.{type Option, None, Some}

pub type Table {
  Table(name: String, alias: Option(String))
}

pub fn new(name: String) -> Table {
  Table(name:, alias: None)
}

pub fn alias(identifier: Table, alias: String) -> Table {
  Table(..identifier, alias: Some(alias))
}

@internal
pub fn to_string(
  table: Table,
  with handle_identifier: fn(String) -> String,
) -> String {
  let ident = handle_identifier(table.name)

  case table.alias {
    Some(a) -> fmt.alias(ident, a)
    None -> ident
  }
}
