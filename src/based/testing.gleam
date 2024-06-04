import based.{type Adapter, type DB, type Returned, DB, Returned}

pub type Connection {
  Connection(c: Nil)
}

pub fn adapter(returning: Result(Returned(a), Nil)) -> Adapter(a, c) {
  fn(_query, _connection) { returning }
}

pub fn with_connection(
  returning: Result(Returned(a), Nil),
  callback: fn(DB(a, Connection)) -> r,
) -> r {
  Connection(Nil)
  |> DB(adapter(returning))
  |> callback
}
