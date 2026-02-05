import based/repo.{type Repo}
import based/sql/column
import based/sql/delete
import based/sql/insert
import based/sql/select
import based/sql/table
import based/sql/update
import gleam/list

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

pub type Schema(a) {
  Schema(table: table.Table, fields: List(#(String, Field)))
}

pub fn new(name: String) -> Schema(a) {
  let table = table.new(name)

  Schema(table:, fields: [])
}

pub fn alias(schema: Schema(a), alias: String) -> Schema(a) {
  let table = schema.table |> table.alias(alias)

  Schema(..schema, table:)
}

pub fn column(schema: Schema(a), field: String) -> column.Column {
  column.new(field)
  |> column.for(schema.table)
}

pub fn field(schema: Schema(a), name: String, field: Field) -> Schema(a) {
  let fields = [#(name, field), ..schema.fields]

  Schema(..schema, fields:)
}

pub fn timestamps(schema: Schema(a)) -> Schema(a) {
  schema
  |> field("created_at", Timestamp)
  |> field("updated_at", Timestamp)
}

pub fn columns(schema: Schema(a)) -> List(String) {
  schema.fields
  |> list.reverse
  |> list.map(fn(kv) { kv.0 })
}

pub fn select(schema: Schema(a), repo: Repo(v)) -> select.Select(v) {
  select.from(repo, schema.table)
}

pub fn insert(schema: Schema(a), repo: Repo(v)) -> insert.Insert(v) {
  insert.into(repo, schema.table)
}

pub fn update(schema: Schema(a), repo: Repo(v)) -> update.Update(v) {
  update.table(repo, schema.table)
}

pub fn delete(schema: Schema(a), repo: Repo(v)) -> delete.Delete(v) {
  delete.from(repo, schema.table)
}
