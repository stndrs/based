import based.{type Query}
import gleam/dict.{type Dict}
import gleam/dynamic.{type Dynamic}
import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/otp/actor

/// A mock connection
pub type Connection {
  Connection(Subject(Message))
}

pub type State {
  State(init: Dict(String, List(Dynamic)))
}

pub fn new_state() -> State {
  State(dict.new())
}

pub fn empty_returns_for(queries: List(Query)) -> State {
  queries
  |> list.map(fn(query) { #(query.sql, []) })
  |> dict.from_list
  |> State
}

pub fn add(state: State, key: String, value: List(Dynamic)) -> State {
  let new_state =
    state.init
    |> dict.insert(key, value)

  State(new_state)
}

/// For testing code without hitting a real database
pub fn with_connection(state: State, callback: fn(Connection) -> t) -> t {
  let assert Ok(conn) = actor.start(state.init, handle_message)
  let result = conn |> Connection |> callback
  process.send(conn, Shutdown)
  result
}

pub fn mock_service(
  query: Query,
  conn: Connection,
) -> Result(List(Dynamic), Nil) {
  let Connection(subject) = conn

  process.call(subject, Get(_, query.sql), 10)
}

pub type Message {
  Shutdown
  Insert(State)
  Get(reply_with: Subject(Result(List(Dynamic), Nil)), key: String)
}

pub fn insert(conn: Connection, state: State) -> Connection {
  let Connection(subject) = conn

  process.send(subject, Insert(state))

  conn
}

fn handle_message(
  message: Message,
  store: Dict(String, List(Dynamic)),
) -> actor.Next(Message, Dict(String, List(Dynamic))) {
  case message {
    Shutdown -> actor.Stop(process.Normal)
    Insert(state) -> {
      let State(init) = state

      store
      |> dict.merge(init)
      |> actor.continue
    }
    Get(client, key) -> {
      let value = store |> dict.get(key)

      process.send(client, value)
      actor.continue(store)
    }
  }
}
