import based/db
import based/sql/internal/fmt
import based/sql/internal/node
import gleam/function
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

pub fn to_node(table: Table(v)) -> node.Node(v) {
  case table {
    Table(identifier:) -> {
      identifier
      |> identifier_to_string
      |> node.TableRef
    }
    Subquery(query:, alias:) -> node.Query(query:, alias:)
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

pub fn identifier_to_string(identifier: Identifier) -> String {
  let ident = case identifier.attr {
    Some(other) -> {
      let attr =
        Identifier(name: other, alias: None, attr: None)
        |> new
        |> to_node
        |> node.to_string(function.identity)

      identifier.name
      |> string.append(".")
      |> string.append(attr)
    }
    None -> identifier.name
  }

  case identifier.alias {
    Some(a) -> fmt.alias(ident, a)
    None -> ident
  }
}
