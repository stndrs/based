//// A standardized interface for database opterations. This module defines
//// types for adapter packages to use in their public interfaces. The
//// transaction and query functions defined in this package require a
//// handler function to be passed in. This allows applications to pass in
//// handlers defined by the database adapter of their choice, or mock
//// handler functions for tests that shouldn't hit a real database.

import gleam/dynamic.{type Dynamic}
import gleam/dynamic/decode
import gleam/list
import gleam/result
import gleam/string

/// Error types covering database-specific errors, decoding failures,
/// and application-level errors.
pub type DbError {
  DbError(message: String)
  DecodeError(errors: List(decode.DecodeError))
  NotFound
  DatabaseError(code: String, name: String, message: String)
  ConstraintError(code: String, name: String, message: String)
  SyntaxError(code: String, name: String, message: String)
}

/// Formats the provided `DbError` as a string.
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

/// Error type for transaction operations including rollbacks and transaction
/// failures.
pub type TransactionError(error) {
  Rollback(cause: error)
  TransactionFailure(cause: error)
  TransactionError(message: String)
}

/// Holds a SQL query string and its relevant values.
pub type Query(v) {
  Query(sql: String, values: List(v))
}

/// Returns a `Query` type with the provided SQL string and an empty list
/// of values.
pub fn sql(sql: String) -> Query(v) {
  Query(sql:, values: [])
}

/// Applies the provided list of values to the given `Query`
pub fn values(query: Query(v), values: List(v)) -> Query(v) {
  Query(..query, values:)
}

/// Holds a count of affected rows, a list of queried fields, and
/// a list of `Dynamic` rows returned from the database. A `Queried`
/// record can be passed to `db.decode` with a decoder, returning
/// a `Returning` record containing the decoded rows.
pub type Queried {
  Queried(count: Int, fields: List(String), rows: List(Dynamic))
}

/// Holds a count of affected rows and a list of decoded rows returned
/// from the database.
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

pub type BatchQueryHandler(v, conn) =
  fn(List(Query(v)), conn) -> Result(List(Queried), DbError)

/// Accepts a `Query`, connection, and query handler function from an adapter
/// package.
/// This function currently only passes the `Query` and connection right back
/// to the handler.
pub fn query(
  query: Query(v),
  conn: conn,
  handler: QueryHandler(v, conn),
) -> Result(Queried, DbError) {
  handler(query, conn)
}

/// Accepts a SQL string, connection, and query handler function from an
/// adapter package.
/// This function currently only passes the SQL string and connection right
/// back to the handler.
pub fn execute(
  sql: String,
  conn: conn,
  handler: ExecuteHandler(conn),
) -> Result(Int, DbError) {
  handler(sql, conn)
}

pub fn batch(
  queries: List(Query(v)),
  conn: conn,
  handler: BatchQueryHandler(v, conn),
) -> Result(List(Queried), DbError) {
  handler(queries, conn)
}

/// Accepts a `Query`, connection, decoder, and query handler function from
/// an adapter package.
/// This function passes the `Query` and connection to the handler, and then
/// runs the provided decoder using `db.decode`.
pub fn all(
  query: Query(v),
  conn: conn,
  decoder: fn() -> decode.Decoder(a),
  handler: QueryHandler(v, conn),
) -> Result(Returning(a), DbError) {
  use queried <- result.try(handler(query, conn))

  decode(queried, decoder)
}

/// Accepts a `Query`, connection, decoder, and query handler function from
/// an adapter package.
/// This function passes the `Query` and connection to the handler, and then
/// runs the provided decoder using `db.decode`. It will decode all rows if
/// multiple are returned from the database, but will only return the first
/// row to the caller.
/// This is a convenience function for callers that are performing a query
/// that will only return one row. It's up to callers to ensure they're
/// providing the correct SQL query to avoid decoding `n` rows and then
/// losing all but the first.
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

/// Accepts a `Queried` record and applies a decoder to its rows. Returns a
/// `Returning` record with the decoded rows.
pub fn decode(
  queried: Queried,
  decoder: fn() -> decode.Decoder(a),
) -> Result(Returning(a), DbError) {
  queried.rows
  |> list.try_map(with: decode.run(_, decoder()))
  |> result.map_error(DecodeError)
  |> result.map(Returning(queried.count, _))
}
