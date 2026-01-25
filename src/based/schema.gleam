import based
import based/db
import based/sql/column
import based/sql/delete
import based/sql/insert
import based/sql/select
import based/sql/table
import based/sql/update
import gleam/dict.{type Dict}
import gleam/dynamic/decode.{type Decoder}
import gleam/result

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

pub type Schema(a, v) {
  Schema(
    repo: based.Repo(v),
    table: table.Table,
    fields: Dict(String, Field),
    decoder: fn() -> Decoder(a),
  )
}

pub fn new(
  repo: based.Repo(v),
  name: String,
  decoder: fn(based.Repo(v)) -> Decoder(a),
) -> Schema(a, v) {
  let table = table.new(name)
  let fields = dict.new()

  Schema(repo:, table:, fields:, decoder: fn() { decoder(repo) })
}

pub fn alias(schema: Schema(a, v), alias: String) -> Schema(a, v) {
  let table = schema.table |> table.alias(alias)

  Schema(..schema, table:)
}

pub fn column(schema: Schema(a, v), field: String) -> column.Column {
  column.new(field)
  |> column.for(schema.table)
}

pub fn field(schema: Schema(a, v), name: String, field: Field) -> Schema(a, v) {
  let fields = schema.fields |> dict.insert(name, field)

  Schema(..schema, fields:)
}

pub fn timestamps(schema: Schema(a, v)) -> Schema(a, v) {
  schema
  |> field("created_at", Timestamp)
  |> field("updated_at", Timestamp)
}

pub fn select(schema: Schema(a, v)) -> select.Select(v) {
  select.from(schema.repo, schema.table)
}

pub fn insert(schema: Schema(a, v)) -> insert.Insert(v) {
  insert.into(schema.repo, schema.table)
}

pub fn update(schema: Schema(a, v)) -> update.Update(v) {
  update.table(schema.repo, schema.table)
}

pub fn delete(schema: Schema(a, v)) -> delete.Delete(v) {
  delete.from(schema.repo, schema.table)
}

pub type All(v, a) =
  fn(db.Query(v), Schema(a, v)) -> Result(List(a), db.DbError)

pub type One(v, a) =
  fn(db.Query(v), Schema(a, v)) -> Result(a, db.DbError)

pub fn all(
  query: db.Query(v),
  schema: Schema(a, v),
  conn: conn,
  handler: db.QueryHandler(v, conn),
) -> Result(List(a), db.DbError) {
  db.all(query, conn, schema.decoder, handler)
  |> result.map(fn(returning) { returning.rows })
}

pub fn one(
  query: db.Query(v),
  schema: Schema(a, v),
  conn: conn,
  handler: db.QueryHandler(v, conn),
) -> Result(a, db.DbError) {
  db.one(query, conn, schema.decoder, handler)
}
