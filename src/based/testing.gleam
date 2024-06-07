import based.{type DB, type Returned, DB, Returned}

/// A mock connection
pub type Connection {
  Connection(c: Nil)
}

/// For testing code without hitting a real database
pub fn with_connection(
  returned: Result(Returned(a), Nil),
  callback: fn(DB(a, Connection)) -> t,
) -> t {
  Connection(Nil)
  |> DB(fn(_, _) { returned })
  |> callback
}

pub fn mock_connection(_conf: conf, callback: fn(Connection) -> t) -> t {
  Connection(Nil) |> callback
}
