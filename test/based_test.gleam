import based.{type Query, Query}
import based/testing
import gleam/dynamic
import gleam/list
import gleeunit
import gleeunit/should

pub fn main() {
  gleeunit.main()
}

pub type Record {
  Record(value: Int)
}

pub type User {
  User(name: String)
}

pub fn exec_test() {
  let user_decoder = dynamic.decode1(User, dynamic.element(0, dynamic.string))
  let record_decoder = dynamic.decode1(Record, dynamic.element(0, dynamic.int))

  let users_sql = "SELECT name FROM users WHERE id=1"
  let records_sql = "SELECT id FROM records LIMIT 1"

  let user_query = Query(sql: users_sql, args: [])
  let record_query = Query(sql: records_sql, args: [])

  let result = {
    let state =
      testing.new_state()
      |> testing.add(users_sql, [dynamic.from(#("Firstname Lastname"))])
      |> testing.add(records_sql, [dynamic.from(#(1))])

    use db <- based.register(
      testing.with_connection,
      state,
      testing.mock_service,
    )

    user_query
    |> based.execute(db)
    |> based.decode(user_decoder)
    |> should.be_ok

    record_query
    |> based.execute(db)
    |> based.decode(record_decoder)
    |> should.be_ok

    Nil
  }

  result |> should.equal(Nil)
}

pub fn exec_without_return_test() {
  let query = Query(sql: "INSERT INTO records (id) VALUES (?);", args: [])

  let result = {
    let state = testing.empty_returns_for([query])

    use db <- based.register(
      testing.with_connection,
      state,
      testing.mock_service,
    )

    query
    |> based.execute(db)
    |> should.be_ok

    Nil
  }

  result |> should.equal(Nil)
}

pub fn register_test() {
  let records_query = based.new_query("SELECT * FROM records")
  let other_records_query = based.new_query("SELECT * FROM other_records")

  let state = testing.empty_returns_for([records_query, other_records_query])

  use db <- based.register(testing.with_connection, state, testing.mock_service)

  records_query
  |> based.execute(db)
  |> should.be_ok

  other_records_query
  |> based.execute(db)
  |> should.be_ok
}

pub fn new_query_test() {
  let sql = "SELECT 1;"
  let query = based.new_query(sql)

  query.sql |> should.equal(sql)
  query.args |> list.length |> should.equal(0)
}

pub fn new_query_with_args_test() {
  let sql = "SELECT * FROM users WHERE id=$1;"
  let query =
    based.new_query(sql)
    |> based.with_args([based.int(1)])

  query.sql |> should.equal(sql)
  query.args |> list.length |> should.equal(1)
}

pub fn string_test() {
  let string_value = based.string("Some String")

  case string_value {
    based.String(val) -> val |> should.equal("Some String")
    _ -> should.fail()
  }
}

pub fn int_test() {
  let int_value = based.int(10)

  case int_value {
    based.Int(val) -> val |> should.equal(10)
    _ -> should.fail()
  }
}

pub fn float_test() {
  let float_value = based.float(10.4)

  case float_value {
    based.Float(val) -> val |> should.equal(10.4)
    _ -> should.fail()
  }
}

pub fn bool_test() {
  let bool_value = based.bool(True)

  case bool_value {
    based.Bool(val) -> val |> should.be_true
    _ -> should.fail()
  }
}
