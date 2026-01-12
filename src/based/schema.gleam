import based/sql
import based/sql/delete
import based/sql/insert
import based/sql/select
import based/sql/update
import gleam/dict.{type Dict}
import gleam/dynamic/decode.{type Decoder}

pub type Field {
  Id
  Number
  Text
  Boolean
  Float
  Date
  Datetime
  Time
  Timestamp
}

pub opaque type Schema(a) {
  Schema(
    table: sql.Identifier,
    fields: Dict(String, Field),
    decoder: fn() -> Decoder(a),
  )
}

pub fn new(name: String, decoder: fn() -> Decoder(a)) -> Schema(a) {
  let table = sql.identifier(name)
  let fields = dict.new()

  Schema(table:, fields:, decoder:)
}

pub fn alias(schema: Schema(a), alias: String) -> Schema(a) {
  let table =
    schema.table
    |> sql.alias(alias)

  Schema(..schema, table:)
}

pub fn column(schema: Schema(a), field: String) -> sql.Identifier {
  schema.table |> sql.attr(field)
}

pub fn field(schema: Schema(a), name: String, field: Field) -> Schema(a) {
  let fields = schema.fields |> dict.insert(name, field)

  Schema(..schema, fields:)
}

pub fn timestamps(schema: Schema(a)) -> Schema(a) {
  schema
  |> field("created_at", Timestamp)
  |> field("updated_at", Timestamp)
}

pub fn select(schema: Schema(a), sql: sql.Sql(v)) -> select.Select(v) {
  select.from(sql, schema.table)
}

pub fn insert(schema: Schema(a), sql: sql.Sql(v)) -> insert.Insert(v) {
  insert.into(sql, schema.table)
}

pub fn update(schema: Schema(a), sql: sql.Sql(v)) -> update.Update(v) {
  update.table(sql, schema.table)
}

pub fn delete(schema: Schema(a), sql: sql.Sql(v)) -> delete.Delete(v) {
  delete.from(sql, schema.table)
}
