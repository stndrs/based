import gleam/dynamic.{type Dynamic}
import gleam/dynamic/decode
import gleam/list
import gleam/result
import gleam/string

// Driver

pub type DbError {
  DbError(message: String)
  DecodeError(errors: List(decode.DecodeError))
  NotFound
  DatabaseError(code: String, name: String, message: String)
  ConstraintError(code: String, name: String, message: String)
  SyntaxError(code: String, name: String, message: String)
}

pub fn error_to_string(err: DbError) -> String {
  case err {
    DbError(message:) -> "[based/db.DbError] " <> message
    DatabaseError(code:, name:, message:) ->
      format_db_error(code, name, message, for: "DatabaseError")
    ConstraintError(code:, name:, message:) ->
      format_db_error(code, name, message, for: "ConstraintError")
    SyntaxError(code:, name:, message:) ->
      format_db_error(code, name, message, for: "SyntaxError")
    NotFound -> "[based/db.NotFound]"
    DecodeError(errors:) -> {
      let error_string =
        list.map(errors, fn(err) {
          let decode.DecodeError(expected:, found:, path:) = err

          "[gleam/dynamic/decode.DecodeError] expected: "
          <> expected
          <> ", found: "
          <> found
          <> ", path: "
          <> string.join(path, with: ", ")
        })
        |> string.join(", ")

      "[based/db.DecodeError] errors: " <> error_string
    }
  }
}

fn format_db_error(
  code: String,
  name: String,
  message: String,
  for kind: String,
) -> String {
  "[based/db."
  <> kind
  <> "] code: "
  <> code
  <> ", name: "
  <> name
  <> ", message: "
  <> message
}

pub type TransactionError(error) {
  Rollback(cause: error)
  TransactionFailure(cause: error)
  TransactionError(message: String)
}

pub type Query(v) {
  Query(sql: String, values: List(v))
}

pub fn sql(sql: String) -> Query(v) {
  Query(sql:, values: [])
}

pub fn values(query: Query(v), values: List(v)) -> Query(v) {
  Query(..query, values:)
}

pub type Queried {
  Queried(count: Int, fields: List(String), rows: List(Dynamic))
}

pub type Returning(a) {
  Returning(count: Int, rows: List(a))
}

// Transaction

pub type TxHandler(conn, t, error) =
  fn(conn, fn(conn) -> Result(t, error)) -> Result(t, TransactionError(error))

pub fn transaction(
  conn: conn,
  handler: TxHandler(conn, t, error),
  next: fn(conn) -> Result(t, error),
) -> Result(t, TransactionError(error)) {
  handler(conn, next)
}

pub fn begin(
  conn: conn,
  handler: fn(conn) -> Result(conn, error),
) -> Result(conn, TransactionError(error)) {
  handler(conn)
  |> result.map_error(TransactionFailure)
}

pub fn commit(
  conn: conn,
  handler: fn(conn) -> Result(conn, error),
) -> Result(conn, TransactionError(error)) {
  handler(conn)
  |> result.map_error(TransactionFailure)
}

pub fn rollback(
  conn: conn,
  handler: fn(conn) -> Result(conn, error),
) -> Result(conn, TransactionError(error)) {
  handler(conn)
  |> result.map_error(TransactionFailure)
}

// Querying

pub type QueryHandler(v, conn) =
  fn(Query(v), conn) -> Result(Queried, DbError)

pub type ExecuteHandler(conn) =
  fn(String, conn) -> Result(Int, DbError)

pub fn query(
  query: Query(v),
  conn: conn,
  handler: QueryHandler(v, conn),
) -> Result(Queried, DbError) {
  handler(query, conn)
}

pub fn execute(
  sql: String,
  conn: conn,
  handler: ExecuteHandler(conn),
) -> Result(Int, DbError) {
  handler(sql, conn)
}

pub fn all(
  query: Query(v),
  conn: conn,
  decoder: fn() -> decode.Decoder(a),
  handler: QueryHandler(v, conn),
) -> Result(Returning(a), DbError) {
  use queried <- result.try(handler(query, conn))

  decode(queried, decoder)
}

pub fn one(
  query: Query(v),
  conn: conn,
  decoder: fn() -> decode.Decoder(a),
  handler: QueryHandler(v, conn),
) -> Result(a, DbError) {
  use queried <- result.try(handler(query, conn))
  use returning <- result.try(decode(queried, decoder))

  returning.rows
  |> list.first
  |> result.map_error(fn(_) { NotFound })
}

pub fn decode(
  queried: Queried,
  decoder: fn() -> decode.Decoder(a),
) -> Result(Returning(a), DbError) {
  queried.rows
  |> list.try_map(with: decode.run(_, decoder()))
  |> result.map_error(DecodeError)
  |> result.map(Returning(queried.count, _))
}
