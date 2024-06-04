import gleam/dynamic
import gleam/list
import gleam/option.{type Option}
import gleam/result

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

pub type WithConnection(a, b, c) =
  fn(b, fn(DB(a, c)) -> a) -> a

pub type Returned(a) {
  Returned(count: Int, rows: List(a))
}

pub type Adapter(a, c) =
  fn(Query(a), c) -> Result(Returned(a), Nil)

pub type DB(a, c) {
  DB(conn: c, execute: Adapter(a, c))
}

pub fn all(query: Query(a), db: DB(a, c)) -> Result(Returned(a), Nil) {
  exec(query, db)
}

pub fn one(query: Query(a), db: DB(a, c)) -> Result(a, Nil) {
  use returned <- result.try(query |> db.execute(db.conn))

  let Returned(_, rows) = returned

  use row <- result.try(rows |> list.first)

  Ok(row)
}

pub fn exec(query: Query(a), db: DB(a, c)) -> Result(Returned(a), Nil) {
  query |> db.execute(db.conn)
}

pub fn string(value: String) -> Value {
  String(value)
}

pub fn int(value: Int) -> Value {
  Int(value)
}

pub fn float(value: Float) -> Value {
  Float(value)
}

pub fn bool(value: Bool) -> Value {
  Bool(value)
}

pub fn null() -> Value {
  Null
}
