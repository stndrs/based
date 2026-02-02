import based/repo.{type Repo}
import based/sql/column
import based/sql/delete
import based/sql/insert
import based/sql/select
import based/sql/table
import based/sql/update
import gleam/dict.{type Dict}

pub type Field {
  Id
  Uuid
  Integer
  Number
  Text
  Boolean
  Float
  Date
  Datetime
  Time
  Timestamp
}

pub type Schema(v) {
  Schema(repo: Repo(v), table: table.Table, fields: Dict(String, Field))
}

pub fn new(repo: Repo(v), name: String) -> Schema(v) {
  let table = table.new(name)
  let fields = dict.new()

  Schema(repo:, table:, fields:)
}

pub fn alias(schema: Schema(v), alias: String) -> Schema(v) {
  let table = schema.table |> table.alias(alias)

  Schema(..schema, table:)
}

pub fn column(schema: Schema(v), field: String) -> column.Column {
  column.new(field)
  |> column.for(schema.table)
}

pub fn field(schema: Schema(v), name: String, field: Field) -> Schema(v) {
  let fields = schema.fields |> dict.insert(name, field)

  Schema(..schema, fields:)
}

pub fn timestamps(schema: Schema(v)) -> Schema(v) {
  schema
  |> field("created_at", Timestamp)
  |> field("updated_at", Timestamp)
}

pub fn select(schema: Schema(v)) -> select.Select(v) {
  select.from(schema.repo, schema.table)
}

pub fn insert(schema: Schema(v)) -> insert.Insert(v) {
  insert.into(schema.repo, schema.table)
}

pub fn update(schema: Schema(v)) -> update.Update(v) {
  update.table(schema.repo, schema.table)
}

pub fn delete(schema: Schema(v)) -> delete.Delete(v) {
  delete.from(schema.repo, schema.table)
}
