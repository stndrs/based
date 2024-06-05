import gleam/dynamic
import gleam/list
import gleam/option.{type Option}
import gleam/result

/// Callers will interact with these Value types when building queries. Their chosen
/// backend is responsible for converting these Value types to the appropriate type.
pub type Value {
  String(String)
  Int(Int)
  Float(Float)
  Bool(Bool)
  Null
}

pub type Query(a) {
  Query(sql: String, args: List(Value), decoder: Option(dynamic.Decoder(a)))
}

/// Defines a valid `with_connection` function
pub type WithConnection(a, b, c) =
  fn(b, fn(DB(a, c)) -> Result(Returned(a), Nil)) -> Result(Returned(a), Nil)

pub type Returned(a) {
  Returned(count: Int, rows: List(a))
}

pub type Adapter(a, c) =
  fn(Query(a), c) -> Result(Returned(a), Nil)

pub type DB(a, c) {
  DB(conn: c, execute: Adapter(a, c))
}

/// Expects a `with_connection` function and its first required argument. For a library
/// implementing a `with_connection` function, the required argument will likely be its
/// configuration data.
/// In the case of `based/testing.with_connection`, the required argument is the expected
/// return data.
pub fn register(
  with_connection: WithConnection(a, b, c),
  b: b,
  callback: fn(DB(a, c)) -> Result(Returned(a), Nil),
) -> Result(Returned(a), Nil) {
  with_connection(b, callback)
}

/// The same as `exec`, but explicitly tells a reader that all queried rows are expected.
pub fn all(query: Query(a), db: DB(a, c)) -> Result(Returned(a), Nil) {
  exec(query, db)
}

/// Returns one queried row
pub fn one(query: Query(a), db: DB(a, c)) -> Result(a, Nil) {
  use returned <- result.try(exec(query, db))

  let Returned(_, rows) = returned

  use row <- result.try(rows |> list.first)

  Ok(row)
}

/// Performs the query against the provided db
pub fn exec(query: Query(a), db: DB(a, c)) -> Result(Returned(a), Nil) {
  query |> db.execute(db.conn)
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
