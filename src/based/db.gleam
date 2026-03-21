//// A standardized interface for database opterations. This module defines
//// types for adapter packages to use in their public interfaces. The
//// transaction and query functions defined in this package require a
//// handler function to be passed in. This allows applications to pass in
//// handlers defined by the database adapter of their choice, or mock
//// handler functions for tests that shouldn't hit a real database.

import based/sql
import gleam/dynamic.{type Dynamic}
import gleam/dynamic/decode
import gleam/list
import gleam/result
import gleam/string

/// Error types covering database-specific errors, decoding failures,
/// and application-level errors.
pub type DbError {
  ConnectionTimeout
  ConnectionError(message: String)
  DatabaseError(code: String, name: String, message: String)
  ConstraintError(code: String, name: String, message: String)
  SyntaxError(code: String, name: String, message: String)
  DbError(message: String)
  DecodeError(errors: List(decode.DecodeError))
  NotFound
}

/// Formats the provided `DbError` as a string.
pub fn error_to_string(err: DbError) -> String {
  case err {
    ConnectionTimeout -> "[based/db.ConnectionTimeout]"
    ConnectionError(message:) -> "[based/db.ConnectionError] " <> message
    DatabaseError(code:, name:, message:) ->
      format_db_error(code, name, message, for: "DatabaseError")
    ConstraintError(code:, name:, message:) ->
      format_db_error(code, name, message, for: "ConstraintError")
    SyntaxError(code:, name:, message:) ->
      format_db_error(code, name, message, for: "SyntaxError")
    DbError(message:) -> "[based/db.DbError] " <> message
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
  NotInTransaction
  TransactionError(message: String)
  TransactionFailure(cause: error)
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
  db: Db(v, conn),
  handler: TxHandler(conn, t, error),
  next: fn(Db(v, conn)) -> Result(t, error),
) -> Result(t, TransactionError(error)) {
  handler(db.conn, fn(conn) { Db(..db, conn:) |> next })
}

// Querying

pub type QueryHandler(v, conn) =
  fn(sql.Query(v), conn) -> Result(Queried, DbError)

pub type ExecuteHandler(conn) =
  fn(String, conn) -> Result(Int, DbError)

pub type BatchQueryHandler(v, conn) =
  fn(List(sql.Query(v)), conn) -> Result(List(Queried), DbError)

pub type Db(v, conn) {
  Db(conn: conn, driver: Driver(v, conn), adapter: sql.Adapter(v))
}

pub fn new(
  driver: Driver(v, conn),
  adapter: sql.Adapter(v),
  conn: conn,
) -> Db(v, conn) {
  Db(conn:, driver:, adapter:)
}

pub opaque type Driver(v, conn) {
  Driver(
    handle_query: QueryHandler(v, conn),
    handle_execute: ExecuteHandler(conn),
    handle_batch: BatchQueryHandler(v, conn),
  )
}

pub fn driver() -> Driver(v, conn) {
  Driver(
    handle_query: fn(_, _) { panic },
    handle_execute: fn(_, _) { panic },
    handle_batch: fn(_, _) { panic },
  )
}

pub fn on_query(
  db: Driver(v, conn),
  handle_query: QueryHandler(v, conn),
) -> Driver(v, conn) {
  Driver(..db, handle_query:)
}

pub fn on_execute(
  db: Driver(v, conn),
  handle_execute: ExecuteHandler(conn),
) -> Driver(v, conn) {
  Driver(..db, handle_execute:)
}

pub fn on_batch(
  db: Driver(v, conn),
  handle_batch: BatchQueryHandler(v, conn),
) -> Driver(v, conn) {
  Driver(..db, handle_batch:)
}

// ----- Adapter ----- //

/// Sets the placeholder format function.
///
/// Receives a zero-based index and returns the placeholder string.
/// Defaults to PostgreSQL-style `$1`, `$2`, etc.
///
/// ```gleam
/// // MySQL-style ? placeholders (index ignored)
/// adapter |> sql.on_placeholder(with: fn(_) { "?" })
/// ```
pub fn on_placeholder(
  db: Db(v, conn),
  with handle_placeholder: fn(Int) -> String,
) -> Db(v, conn) {
  let adapter = sql.on_placeholder(db.adapter, handle_placeholder)

  Db(..db, adapter:)
}

/// Sets the function used to render a value as a literal SQL string.
///
/// Only needed for `to_string` output — `to_query` uses placeholders instead.
pub fn on_value(
  db: Db(v, conn),
  with handle_value: fn(v) -> String,
) -> Db(v, conn) {
  let adapter = sql.on_value(db.adapter, handle_value)

  Db(..db, adapter:)
}

/// Sets the identifier quoting function.
///
/// Defaults to identity (no quoting). Override to add database-specific quoting.
///
/// ```gleam
/// // Quote with double quotes
/// adapter |> sql.on_identifier(with: fn(name) { "\"" <> name <> "\"" })
/// ```
pub fn on_identifier(
  db: Db(v, conn),
  with handle_identifier: fn(String) -> String,
) -> Db(v, conn) {
  let adapter = sql.on_identifier(db.adapter, handle_identifier)

  Db(..db, adapter:)
}

/// Sets the function that produces the null representation for type `v`.
///
/// Used when a `nullable` kind resolves to `None`.
pub fn on_null(db: Db(v, conn), with fun: fn() -> v) -> Db(v, conn) {
  let adapter = sql.on_null(db.adapter, fun)

  Db(..db, adapter:)
}

/// Sets the function that wraps an `Int` into the value type `v`.
///
/// Used internally when rendering `LIMIT`, `OFFSET`, and other integer literals.
pub fn on_int(db: Db(v, conn), with fun: fn(Int) -> v) -> Db(v, conn) {
  let adapter = sql.on_int(db.adapter, fun)

  Db(..db, adapter:)
}

/// Sets the function that wraps a `String` into the value type `v`.
///
/// Used internally when rendering `LIKE` patterns and other string literals.
pub fn on_text(db: Db(v, conn), with fun: fn(String) -> v) -> Db(v, conn) {
  let adapter = sql.on_text(db.adapter, fun)

  Db(..db, adapter:)
}

/// Accepts a `Query`, connection, and query handler function from an adapter
/// package.
/// This function currently only passes the `Query` and connection right back
/// to the handler.
pub fn query(query: sql.Query(v), db: Db(v, conn)) -> Result(Queried, DbError) {
  db.driver.handle_query(query, db.conn)
}

/// Accepts a SQL string, connection, and query handler function from an
/// adapter package.
/// This function currently only passes the SQL string and connection right
/// back to the handler.
pub fn execute(sql: String, db: Db(v, conn)) -> Result(Int, DbError) {
  db.driver.handle_execute(sql, db.conn)
}

pub fn batch(
  queries: List(sql.Query(v)),
  db: Db(v, conn),
) -> Result(List(Queried), DbError) {
  db.driver.handle_batch(queries, db.conn)
}

/// Accepts a `Query`, connection, decoder, and query handler function from
/// an adapter package.
/// This function passes the `Query` and connection to the handler, and then
/// runs the provided decoder using `db.decode`.
pub fn all(
  query: sql.Query(v),
  db: Db(v, conn),
  decoder: decode.Decoder(a),
) -> Result(List(a), DbError) {
  use queried <- result.try(db.driver.handle_query(query, db.conn))

  use returning <- result.map(decode(queried, decoder))

  returning.rows
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
  query: sql.Query(v),
  db: Db(v, conn),
  decoder: decode.Decoder(a),
) -> Result(a, DbError) {
  use queried <- result.try(db.driver.handle_query(query, db.conn))
  use returning <- result.try(decode(queried, decoder))

  returning.rows
  |> list.first
  |> result.map_error(fn(_) { NotFound })
}

/// Accepts a `Queried` record and applies a decoder to its rows. Returns a
/// `Returning` record with the decoded rows.
pub fn decode(
  queried: Queried,
  decoder: decode.Decoder(a),
) -> Result(Returning(a), DbError) {
  queried.rows
  |> list.try_map(with: decode.run(_, decoder))
  |> result.map_error(DecodeError)
  |> result.map(Returning(queried.count, _))
}
