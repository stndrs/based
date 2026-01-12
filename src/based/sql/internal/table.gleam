import based/db
import based/sql/internal/fmt
import gleam/option.{type Option, None, Some}
import gleam/string

pub opaque type Table(v) {
  Table(identifier: Identifier)
  Subquery(query: db.Query(v), alias: Option(String))
}

pub fn subquery(query: db.Query(v)) -> Table(v) {
  Subquery(query, alias: None)
}

pub fn to_values(table: Table(v)) -> List(v) {
  case table {
    Table(..) -> []
    Subquery(query, _) -> query.values
  }
}

pub fn to_string(
  table: Table(v),
  with handle_identifier: fn(String) -> String,
) -> String {
  case table {
    Table(identifier:) -> {
      identifier
      |> identifier_to_string(handle_identifier)
    }
    Subquery(query:, alias: Some(alias)) -> {
      query.sql
      |> fmt.enclose
      |> fmt.alias(alias)
    }
    Subquery(query:, alias: None) -> fmt.enclose(query.sql)
  }
}

pub fn new(identifier: Identifier) -> Table(v) {
  Table(identifier:)
}

pub opaque type Identifier {
  Identifier(name: String, alias: Option(String), attr: Option(String))
}

pub fn identifier(name: String) -> Identifier {
  Identifier(name:, alias: None, attr: None)
}

pub fn alias(identifier: Identifier, alias: String) -> Identifier {
  Identifier(..identifier, alias: Some(alias))
}

pub fn attr(identifier: Identifier, attr: String) -> Identifier {
  Identifier(..identifier, attr: Some(attr))
}

pub fn identifier_to_string(
  identifier: Identifier,
  with handle_identifier: fn(String) -> String,
) -> String {
  let ident = case identifier.attr {
    Some(attr) -> {
      identifier.name
      |> handle_identifier
      |> string.append(".")
      |> string.append(handle_identifier(attr))
    }
    None -> handle_identifier(identifier.name)
  }

  case identifier.alias {
    Some(a) -> fmt.alias(ident, a)
    None -> ident
  }
}
