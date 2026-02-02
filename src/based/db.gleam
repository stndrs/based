//// A standardized interface for database opterations. This module defines
//// types for adapter packages to use in their public interfaces. The
//// transaction and query functions defined in this package require a
//// handler function to be passed in. This allows applications to pass in
//// handlers defined by the database adapter of their choice, or mock
//// handler functions for tests that shouldn't hit a real database.

import based/interval
import gleam/dynamic.{type Dynamic}
import gleam/dynamic/decode
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import gleam/time/calendar
import gleam/time/timestamp

/// Values
/// `Offset` represents a UTC offset. `Timestamptz` is composed
/// of a [`gleam/time/timestamp.Timestamp`][1] and `Offset`. The offset will be
/// applied to the timestamp when being encoded.
///
/// A timestamp with a positive offset represents some time in
/// the future, relative to UTC.
/// A timestamp with a negative offset represents some time in
/// the past, relative to UTC.
///
/// `Offset`s will be subtracted from the `gleam/time/timestamp.Timestamp`
/// so the encoded value is a UTC timestamp.
///
/// [1]: https://hexdocs.pm/gleam_time/gleam/time/timestamp.html
pub type Offset {
  Offset(hours: Int, minutes: Int)
}

/// Returns an Offset with the provided hours and 0 minutes.
pub fn offset(hours: Int) -> Offset {
  Offset(hours:, minutes: 0)
}

/// Applies some number of minutes to the Offset
pub fn minutes(offset: Offset, minutes: Int) -> Offset {
  Offset(..offset, minutes:)
}

/// The `Value` type represents PostgreSQL data types. Values can be encoded
/// to PostgreSQL's binary format. `Value`s can be used when interacting with
/// PostgreSQL databases through client libraries like [pgl][1].
///
/// [1]: https://github.com/stndrs/pgl
pub type Value {
  Null
  Bool(Bool)
  Int(Int)
  Float(Float)
  Text(String)
  Bytea(BitArray)
  Date(calendar.Date)
  Time(calendar.TimeOfDay)
  Datetime(calendar.Date, calendar.TimeOfDay)
  Timestamp(timestamp.Timestamp)
  Timestamptz(timestamp.Timestamp, Offset)
  Interval(interval.Interval)
  Array(List(Value))
}

pub const null = Null

pub const true = Bool(True)

pub const false = Bool(False)

pub fn bool(bool: Bool) -> Value {
  Bool(bool)
}

pub fn int(int: Int) -> Value {
  Int(int)
}

pub fn float(float: Float) -> Value {
  Float(float)
}

pub fn text(text: String) -> Value {
  Text(text)
}

pub fn bytea(bytea: BitArray) -> Value {
  Bytea(bytea)
}

pub fn date(date: calendar.Date) -> Value {
  Date(date)
}

pub fn time(time_of_day: calendar.TimeOfDay) -> Value {
  Time(time_of_day)
}

pub fn datetime(date: calendar.Date, time: calendar.TimeOfDay) -> Value {
  Datetime(date, time)
}

pub fn timestamp(timestamp: timestamp.Timestamp) -> Value {
  Timestamp(timestamp)
}

pub fn timestamptz(timestamp: timestamp.Timestamp, offset: Offset) -> Value {
  Timestamptz(timestamp, offset)
}

pub fn interval(interval: interval.Interval) -> Value {
  Interval(interval)
}

pub fn array(elements: List(a), of kind: fn(a) -> Value) -> Value {
  elements
  |> list.map(kind)
  |> Array
}

/// Checks if the provided value is `option.Some` or `option.None`. If
/// `None` then the value returned is `value.Null`. If `Some` value is
/// provided then it is passed to the `inner_type` function.
///
/// Example:
///
/// ```gleam
///   let int = pg_value.nullable(pg_value.int, Some(10))
///
///   let null = pg_value.nullable(pg_value.int, None)
/// ```
pub fn nullable(inner_type: fn(a) -> Value, optional: Option(a)) -> Value {
  case optional {
    Some(term) -> inner_type(term)
    None -> Null
  }
}

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
pub fn params(query: Query(v), values: List(v)) -> Query(v) {
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
  db: Db(v, conn),
  handler: TxHandler(conn, t, error),
  next: fn(Db(v, conn)) -> Result(t, error),
) -> Result(t, TransactionError(error)) {
  handler(db.conn, fn(conn) { Db(..db, conn:) |> next })
}

// Querying

pub type QueryHandler(v, conn) =
  fn(Query(v), conn) -> Result(Queried, DbError)

pub type ExecuteHandler(conn) =
  fn(String, conn) -> Result(Int, DbError)

pub type BatchQueryHandler(v, conn) =
  fn(List(Query(v)), conn) -> Result(List(Queried), DbError)

pub type Db(v, conn) {
  Db(conn: conn, driver: Driver(v, conn))
}

pub fn new(driver: Driver(v, conn), conn: conn) -> Db(v, conn) {
  Db(conn:, driver:)
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

/// Accepts a `Query`, connection, and query handler function from an adapter
/// package.
/// This function currently only passes the `Query` and connection right back
/// to the handler.
pub fn query(query: Query(v), db: Db(v, conn)) -> Result(Queried, DbError) {
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
  queries: List(Query(v)),
  db: Db(v, conn),
) -> Result(List(Queried), DbError) {
  db.driver.handle_batch(queries, db.conn)
}

/// Accepts a `Query`, connection, decoder, and query handler function from
/// an adapter package.
/// This function passes the `Query` and connection to the handler, and then
/// runs the provided decoder using `db.decode`.
pub fn all(
  query: Query(v),
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
  query: Query(v),
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
