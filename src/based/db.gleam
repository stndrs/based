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

// Querying

/// A function that executes a parameterized query and returns rows.
pub type QueryHandler(v, conn) =
  fn(sql.Query(v), conn) -> Result(Queried, DbError)

/// A function that executes a raw SQL string and returns a count of
/// affected rows.
pub type ExecuteHandler(conn) =
  fn(String, conn) -> Result(Int, DbError)

/// A function that executes a list of parameterized queries and returns
/// a list of results.
pub type BatchQueryHandler(v, conn) =
  fn(List(sql.Query(v)), conn) -> Result(List(Queried), DbError)

/// A configured database connection bundling a connection value, a
/// `Driver` with query handlers, and an `sql.Adapter` for query rendering.
pub type Db(v, conn) {
  Db(driver: Driver(v, conn), adapter: sql.Adapter(v))
}

/// Creates a new `Db` from a driver and adapter.
pub fn new(driver: Driver(v, conn), adapter: sql.Adapter(v)) -> Db(v, conn) {
  Db(driver:, adapter:)
}

/// An opaque driver that holds the query, execute, and batch handler
/// functions provided by an adapter package.
///
/// Create one with `driver()` and configure it with `on_query`,
/// `on_execute`, and `on_batch`.
pub opaque type Driver(v, conn) {
  Driver(
    conn: conn,
    handle_query: QueryHandler(v, conn),
    handle_execute: ExecuteHandler(conn),
    handle_batch: BatchQueryHandler(v, conn),
  )
}

pub fn driver(
  conn: conn,
  on_query handle_query: QueryHandler(v, conn),
  on_execute handle_execute: ExecuteHandler(conn),
  on_batch handle_batch: BatchQueryHandler(v, conn),
) -> Driver(v, conn) {
  Driver(conn, handle_query:, handle_execute:, handle_batch:)
}

/// Executes a parameterized query using the configured driver.
pub fn query(query: sql.Query(v), db: Db(v, conn)) -> Result(Queried, DbError) {
  db.driver.handle_query(query, db.driver.conn)
}

/// Executes a raw SQL string using the configured driver.
pub fn execute(sql: String, db: Db(v, conn)) -> Result(Int, DbError) {
  db.driver.handle_execute(sql, db.driver.conn)
}

/// Executes a list of parameterized queries as a batch.
pub fn batch(
  queries: List(sql.Query(v)),
  db: Db(v, conn),
) -> Result(List(Queried), DbError) {
  db.driver.handle_batch(queries, db.driver.conn)
}

/// A convenience function for callers performing a query that will return
/// a list of rows, but don't need the full `Returning` record.
pub fn all(
  query: sql.Query(v),
  db: Db(v, conn),
  decoder: decode.Decoder(a),
) -> Result(List(a), DbError) {
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
) -> Result(a, DbError) {
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
) -> Result(Returning(a), DbError) {
  queried.rows
  |> list.try_map(with: decode.run(_, decoder))
  |> result.map_error(DecodeError)
  |> result.map(Returning(queried.count, _))
}
