import gleam/dynamic.{type Decoder, type Dynamic}
import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/otp/actor.{type StartError}
import gleam/result
import gleam/string

/// Callers will interact with these Value types when building queries. Their chosen
/// backend is responsible for converting these Value types to the appropriate type.
pub type Value {
  String(String)
  Int(Int)
  Float(Float)
  Bool(Bool)
  Null
}

// TODO: Improve errors 
pub type BasedError {
  BasedError(code: String, name: String, message: String)
}

pub type Query {
  Query(sql: String, values: List(Value))
}

pub type BasedAdapter(conf, conn, t) {
  BasedAdapter(
    with_connection: WithConnection(conf, conn, t),
    conf: conf,
    service: Service(conn),
  )
}

/// Defines a valid `with_connection` function
pub type WithConnection(conf, conn, t) =
  fn(conf, fn(conn) -> t) -> t

pub type Returned(a) {
  Returned(count: Int, rows: List(a))
}

pub opaque type DB {
  DB(Subject(Message))
}

pub type Service(conn) =
  fn(Query, conn) -> Result(List(Dynamic), BasedError)

pub type Message {
  Execute(reply_with: Subject(Result(List(Dynamic), BasedError)), query: Query)
  Shutdown
}

/// Expects a `with_connection` function and its first required argument. For a library
/// implementing a `with_connection` function, the required argument will likely be its
/// configuration data.
/// In the case of `based/testing.with_connection`, the required argument is the expected
/// return data.
pub fn register(
  based_adapter: BasedAdapter(conf, conn, t),
  callback: fn(DB) -> t,
) -> t {
  let BasedAdapter(with_connection, conf, service) = based_adapter

  use connection <- with_connection(conf)

  let assert Ok(actor) = start(connection, service)

  let result = callback(DB(actor))
  shutdown(actor)
  result
}

fn start(
  conn: conn,
  service: Service(conn),
) -> Result(Subject(Message), StartError) {
  actor.start(#(conn, service), handle_message)
}

fn shutdown(actor) -> Nil {
  process.send(actor, Shutdown)
}

fn handle_message(
  message: Message,
  backend: #(conn, Service(conn)),
) -> actor.Next(Message, #(conn, Service(conn))) {
  case message {
    Shutdown -> actor.Stop(process.Normal)
    Execute(client, query) -> {
      let #(conn, service) = backend

      process.send(client, service(query, conn))
      actor.continue(backend)
    }
  }
}

/// Returns a Query record with the provided SQL string and an empty list of values.
/// This function can be used on its own for queries that don't require values. Its
/// return value may also be piped into `with_values` to be given the appropriate
/// list of values required for the query.
pub fn new_query(sql: String) -> Query {
  Query(sql, [])
}

/// Appends a list of values to the provided Query record.
pub fn with_values(query: Query, values: List(Value)) -> Query {
  Query(query.sql, values: list.append(query.values, values))
}

/// Applies the provided `Decoder` to all rows returned by the Query.
pub fn all(
  query: Query,
  db: DB,
  decoder: Decoder(a),
) -> Result(Returned(a), BasedError) {
  use rows <- result.try(execute(query, db))
  decode(rows, decoder)
}

/// Returns the first row returned by the query after being decoded by the provided
/// `Decoder`. Useful for queries where only one row should be returned. If more rows
/// are returned by the query, only the first row will be decoded and returned from
/// this function.
pub fn one(query: Query, db: DB, decoder: Decoder(a)) -> Result(a, BasedError) {
  use rows <- result.try(execute(query, db))
  let returned = decode(rows, decoder)
  use returned <- result.try(returned)

  let Returned(_, rows) = returned

  use row <- result.try(
    rows
    |> list.first
    |> result.replace_error(BasedError(
      code: "",
      name: "not_found",
      message: "Expected one row but found none",
    )),
  )

  Ok(row)
}

/// Performs a query and returns a list of Dynamic values.
pub fn execute(query: Query, db: DB) -> Result(List(Dynamic), BasedError) {
  let DB(subject) = db

  process.call(subject, Execute(_, query), 5000)
}

/// Decodes a list of Dynamic values with the provided `Decoder`.
pub fn decode(
  rows: List(Dynamic),
  decoder: Decoder(a),
) -> Result(Returned(a), BasedError) {
  use rows <- result.try(
    list.try_map(over: rows, with: decoder)
    |> result.map_error(decode_error),
  )

  list.length(rows)
  |> Returned(rows)
  |> Ok
}

fn decode_error(errors: List(dynamic.DecodeError)) -> BasedError {
  let assert [dynamic.DecodeError(expected, actual, path), ..] = errors
  let path = string.join(path, ".")
  let message =
    "Decoder failed, expected "
    <> expected
    <> ", got "
    <> actual
    <> " in "
    <> path

  BasedError(code: "", name: "decode_error", message: message)
}

/// Converts a string to a `Value` type
pub fn string(value: String) -> Value {
  String(value)
}

/// Converts an int to a `Value` type
pub fn int(value: Int) -> Value {
  Int(value)
}

/// Converts a float to a `Value` type
pub fn float(value: Float) -> Value {
  Float(value)
}

/// Converts a bool to a `Value` type
pub fn bool(value: Bool) -> Value {
  Bool(value)
}

/// Returns a Null Value type
pub fn null() -> Value {
  Null
}
