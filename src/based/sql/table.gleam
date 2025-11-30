import based/db
import based/format.{type Format}
import based/sql/internal/fmt
import gleam/option.{type Option, None, Some}

pub opaque type Table(v) {
  Table(name: String, alias: Option(String))
  Subquery(query: db.Query(v), alias: Option(String))
}

pub fn new(name: String) -> Table(v) {
  Table(name:, alias: None)
}

pub fn alias(table: Table(v), alias: String) -> Table(v) {
  case table {
    Table(..) -> Table(..table, alias: Some(alias))
    Subquery(..) -> Subquery(..table, alias: Some(alias))
  }
}

pub fn from_query(query: db.Query(v)) -> Table(v) {
  Subquery(query, alias: None)
}

pub fn to_values(table: Table(v)) -> List(v) {
  case table {
    Table(..) -> []
    Subquery(query, _) -> query.values
  }
}

pub fn to_string(table: Table(v), format: Format(v)) -> String {
  case table {
    Table(..) -> format.to_identifier(format, table.name)
    Subquery(..) -> fmt.enclose(table.query.sql)
  }
  |> maybe_aliased(table.alias)
}

fn maybe_aliased(left: String, alias: Option(String)) -> String {
  case alias {
    Some(a) -> left <> " AS " <> a
    None -> left
  }
}
