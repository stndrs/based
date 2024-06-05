import based.{type Adapter, type DB, type Returned, DB, Returned}

/// A mock connection
pub type Connection {
  Connection(c: Nil)
}

/// A mock database adapter
pub fn adapter(returned: Result(Returned(a), Nil)) -> Adapter(a, c) {
  fn(_query, _connection) { returned }
}

/// For testing code without hitting a real database
pub fn with_connection(
  returned: Result(Returned(a), Nil),
  callback: fn(DB(a, Connection)) -> Result(o, e),
) -> Result(o, e) {
  Connection(Nil)
  |> DB(adapter(returned))
  |> callback
}
