import gleam/dynamic
import gleam/option.{type Option}

pub type Value {
  String(String)
  Int(Int)
  Float(Float)
  Bool(Bool)
  List(List(Value))
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
  fn(String, c, List(Value), Option(dynamic.Decoder(a))) ->
    Result(Returned(a), Nil)

pub type DB(a, c) {
  DB(conn: c, execute: Adapter(a, c))
}

pub fn exec(query: Query(a), db: DB(a, c)) -> Result(Returned(a), Nil) {
  query.sql
  |> db.execute(db.conn, query.args, query.decoder)
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

pub fn list(value: List(Value)) -> Value {
  List(value)
}

pub fn null() -> Value {
  Null
}
