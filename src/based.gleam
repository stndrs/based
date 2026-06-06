//// A standardized interface for database operations. This module defines
//// types for adapter packages to use in their public interfaces.

import based/sql
import gleam/dynamic.{type Dynamic}
import gleam/dynamic/decode
import gleam/list
import gleam/result
import gleam/string

/// Error types covering database-specific errors, decoding failures,
/// and application-level errors.
pub type BasedError {
  BasedError(message: String)
  NotFound
  DecodeError(errors: List(decode.DecodeError))
  DbError(error: DatabaseError)
}

pub type DatabaseError {
  DatabaseError(code: String, name: String, message: String)
  ConstraintError(code: String, name: String, message: String)
  SyntaxError(code: String, name: String, message: String)
  ConnectionError(message: String)
  ConnectionUnavailable
  ConnectionTimeout
}

pub fn error_to_string(err: BasedError) -> String {
  case err {
    BasedError(message:) ->
      format_error_kind(["based"], "BasedError")
      |> string.append(" ")
      |> string.append(message)
    NotFound -> format_error_kind(["based"], "NotFound")
    DecodeError(errors:) -> {
      let error_string =
        list.map(errors, fn(err) {
          let decode.DecodeError(expected:, found:, path:) = err

          let error_description =
            [
              #("expected", expected),
              #("found", found),
              #("path", string.join(path, with: ", ")),
            ]
            |> format_error_parts

          ["gleam", "dynamic", "decode"]
          |> format_error_kind("DecodeError")
          |> string.append(" ")
          |> string.append(error_description)
        })
        |> string.join(", ")

      ["based"]
      |> format_error_kind("DecodeError")
      |> string.append(" errors: ")
      |> string.append(error_string)
    }
    DbError(error:) -> database_error_to_string(error)
  }
}

/// Formats the provided `DatabaseError` as a string.
pub fn database_error_to_string(err: DatabaseError) -> String {
  case err {
    ConnectionTimeout -> format_error_kind(["based"], "ConnectionTimeout")
    ConnectionUnavailable ->
      format_error_kind(["based"], "ConnectionUnavailable")
    ConnectionError(message:) ->
      format_error_kind(["based"], "ConnectionError")
      |> string.append(" ")
      |> string.append(message)
    DatabaseError(code:, name:, message:) ->
      format_error_kind(["based"], "DatabaseError")
      |> format_db_error(code, name, message)
    ConstraintError(code:, name:, message:) ->
      format_error_kind(["based"], "ConstraintError")
      |> format_db_error(code, name, message)
    SyntaxError(code:, name:, message:) ->
      format_error_kind(["based"], "SyntaxError")
      |> format_db_error(code, name, message)
  }
}

fn format_db_error(
  kind: String,
  code: String,
  name: String,
  message: String,
) -> String {
  let error_description =
    [
      #("code", code),
      #("name", name),
      #("message", message),
    ]
    |> format_error_parts

  kind <> " " <> error_description
}

fn format_error_kind(path: List(String), kind: String) -> String {
  let error_kind =
    string.join(path, "/")
    |> string.append(".")
    |> string.append(kind)

  "[" <> error_kind <> "]"
}

fn format_error_parts(errors: List(#(String, String))) -> String {
  errors
  |> list.map(fn(key_val) {
    let #(key, val) = key_val

    key <> ": " <> val
  })
  |> string.join(", ")
}

/// Error type for transaction operations including rollbacks and transaction
/// failures.
pub type TransactionError(error) {
  Rollback(cause: error)
  NotInTransaction
  TransactionError(message: String)
}

/// Holds a count of affected rows, a list of queried fields, and
/// a list of `Dynamic` rows returned from the database. A `Queried`
/// record can be passed to `based.decode` with a decoder, returning
/// a `Returning` record containing the decoded rows.
pub type Queried {
  Queried(count: Int, fields: List(String), rows: List(Dynamic))
}

/// Holds a count of affected rows and a list of decoded rows returned
/// from the database.
pub type Returning(a) {
  Returning(count: Int, rows: List(a))
}

/// A function provided by an adapter package that wraps a callback in a
/// database transaction. The handler receives a connection and a callback;
/// it is responsible for committing on `Ok` and rolling back on `Error`.
pub type TxHandler(conn, t, error) =
  fn(conn, fn(conn) -> Result(t, error)) -> Result(t, TransactionError(error))

/// Runs `next` inside a database transaction using the provided `handler`.
///
/// The handler (supplied by an adapter package) is responsible for beginning
/// the transaction, committing on success, and rolling back on failure.
pub fn transaction(
  db: Db(v, conn),
  handler: TxHandler(conn, t, error),
  next: fn(Db(v, conn)) -> Result(t, error),
) -> Result(t, TransactionError(error)) {
  handler(db.driver.conn, fn(conn) {
    let driver = Driver(..db.driver, conn:)

    Db(..db, driver:) |> next
  })
}

/// A function that executes a parameterized query and returns rows.
pub type QueryHandler(v, conn) =
  fn(sql.Query(v), conn) -> Result(Queried, BasedError)

/// A function that executes a raw SQL string and returns a count of
/// affected rows.
pub type ExecuteHandler(conn) =
  fn(String, conn) -> Result(Int, BasedError)

/// A function that executes a list of parameterized queries and returns
/// a list of results.
pub type BatchQueryHandler(v, conn) =
  fn(List(sql.Query(v)), conn) -> Result(List(Queried), BasedError)

/// A configured database connection bundling a connection value, a
/// `Driver` with query handlers, and a `sql.Adapter` for query rendering.
pub type Db(v, conn) {
  Db(driver: Driver(v, conn), sql: sql.Adapter(v))
}

/// Creates a new `Db` from a driver and adapter.
pub fn new(driver: Driver(v, conn), sql: sql.Adapter(v)) -> Db(v, conn) {
  Db(driver:, sql:)
}

/// An opaque driver that holds the query, execute, and batch handler
/// functions provided by an adapter package.
pub opaque type Driver(v, conn) {
  Driver(
    conn: conn,
    handle_query: QueryHandler(v, conn),
    handle_execute: ExecuteHandler(conn),
    handle_batch: BatchQueryHandler(v, conn),
  )
}

/// Returns a configured `Driver` record.
pub fn driver(
  conn: conn,
  on_query handle_query: QueryHandler(v, conn),
  on_execute handle_execute: ExecuteHandler(conn),
  on_batch handle_batch: BatchQueryHandler(v, conn),
) -> Driver(v, conn) {
  Driver(conn, handle_query:, handle_execute:, handle_batch:)
}

/// Executes a query using the configured driver.
pub fn query(
  query: sql.Query(v),
  db: Db(v, conn),
) -> Result(Queried, BasedError) {
  db.driver.handle_query(query, db.driver.conn)
}

/// Executes a raw SQL string using the configured driver.
pub fn execute(sql: String, db: Db(v, conn)) -> Result(Int, BasedError) {
  db.driver.handle_execute(sql, db.driver.conn)
}

/// Executes a list of queries as a batch.
pub fn batch(
  queries: List(sql.Query(v)),
  db: Db(v, conn),
) -> Result(List(Queried), BasedError) {
  db.driver.handle_batch(queries, db.driver.conn)
}

/// A convenience function for callers performing a query that will return
/// a list of rows, but don't need the full `Returning` record.
pub fn all(
  query: sql.Query(v),
  db: Db(v, conn),
  decoder: decode.Decoder(a),
) -> Result(List(a), BasedError) {
  use queried <- result.try(db.driver.handle_query(query, db.driver.conn))
  use returning <- result.map(decode(queried, decoder))

  returning.rows
}

/// A convenience function for callers performing a query that will return
/// only one row. It's up to callers to ensure they're providing the correct
/// SQL query to avoid decoding `n` rows and then losing all but the first.
/// Returns `Error(NotFound)` if the query returns zero rows.
pub fn one(
  query: sql.Query(v),
  db: Db(v, conn),
  decoder: decode.Decoder(a),
) -> Result(a, BasedError) {
  use queried <- result.try(db.driver.handle_query(query, db.driver.conn))
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
) -> Result(Returning(a), BasedError) {
  queried.rows
  |> list.try_map(with: decode.run(_, decoder))
  |> result.map_error(DecodeError)
  |> result.map(Returning(queried.count, _))
}
