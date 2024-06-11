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

pub type Query {
  Query(sql: String, args: List(Value))
}

/// Defines a valid `with_connection` function
pub type WithConnection(b, conn, t) =
  fn(b, fn(conn) -> t) -> t

pub type Returned(a) {
  Returned(count: Int, rows: List(a))
}

pub opaque type DB {
  DB(Subject(Message))
}

pub type Service(c) =
  fn(Query, c) -> Result(List(Dynamic), Nil)

pub type QueryDecoder(a) =
  fn(List(Dynamic), Decoder(a)) -> List(a)

/// Expects a `with_connection` function and its first required argument. For a library
/// implementing a `with_connection` function, the required argument will likely be its
/// configuration data.
/// In the case of `based/testing.with_connection`, the required argument is the expected
/// return data.
pub fn register(
  with_connection: WithConnection(conf, conn, ret),
  conf: conf,
  service: Service(conn),
  callback: fn(DB) -> ret,
) -> ret {
  use connection <- with_connection(conf)

  let assert Ok(actor) = start(connection, service)

  let result = callback(DB(actor))
  shutdown(actor)
  result
}

pub type Message {
  Execute(reply_with: Subject(Result(List(Dynamic), Nil)), query: Query)
  Shutdown
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

pub fn new_query(sql: String) -> Query {
  Query(sql, [])
}

pub fn with_args(query: Query, args: List(Value)) -> Query {
  Query(..query, args: args)
}

pub fn all(
  query: Query,
  db: DB,
  decoder: Decoder(a),
) -> Result(Returned(a), BasedError) {
  use rows <- result.try(execute(query, db))
  decode(rows, decoder)
}

/// Returns one queried row
pub fn one(query: Query, db: DB, decoder: Decoder(a)) -> Result(a, BasedError) {
  use rows <- result.try(execute(query, db))
  let returned = decode(rows, decoder)
  use returned <- result.try(returned)

  let Returned(_, rows) = returned

  use row <- result.try(
    rows
    |> list.first
    |> result.replace_error(BasedError(
      code: NotFound,
      message: "Not found",
      offset: -1,
    )),
  )

  Ok(row)
}

/// The same as `exec`, but explicitly tells a reader that all queried rows are expected.
pub fn execute(query: Query, db: DB) -> Result(List(Dynamic), BasedError) {
  let DB(subject) = db

  process.call(subject, Execute(_, query), 10)
  |> result.replace_error(BasedError(
    code: QueryExec,
    message: "Query failed",
    offset: -1,
  ))
}

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

pub type Code {
  Generic
  QueryExec
  NotFound
}

pub type BasedError {
  BasedError(code: Code, message: String, offset: Int)
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

  BasedError(code: Generic, message: message, offset: -1)
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
