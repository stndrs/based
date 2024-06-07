import based.{type Query, type Returned, Query, Returned}
import based/testing
import gleam/dynamic
import gleam/list
import gleam/option.{None, Some}
import gleeunit
import gleeunit/should

pub fn main() {
  gleeunit.main()
}

pub type Record {
  Record(value: Int)
}

pub fn exec_test() {
  let decoder = dynamic.decode1(Record, dynamic.element(0, dynamic.int))
  let query = Query(sql: "SELECT 1;", args: [], decoder: Some(decoder))

  let result = {
    let returned = Ok(Returned(1, [Record(1)]))

    use db <- based.register(testing.with_connection, returned)

    query
    |> based.exec(db)
    |> should.be_ok

    Nil
  }

  result |> should.equal(Nil)
}

pub fn exec_without_return_test() {
  let query =
    Query(sql: "INSERT INTO records (id) VALUES (?);", args: [], decoder: None)

  let result = {
    let returning = Ok(Returned(0, []))

    use db <- based.register(testing.with_connection, returning)

    query
    |> based.exec(db)
    |> should.be_ok

    Nil
  }

  result |> should.equal(Nil)
}

pub fn multiple_query_test() {
  let records_service = fn(_: Query(a), _: c) { Ok(Returned(0, [])) }
  let int_service = fn(_: Query(a), _: c) { Ok(Returned(1, [1])) }

  let result = {
    use conn <- based.using(testing.mock_connection, Nil)

    // Query for records with records_service
    let records =
      based.new_query("SELECT * FROM records")
      |> based.execute(conn, records_service)
      |> should.be_ok

    let int =
      based.new_query("SELECT int FROM ints LIMIT 1")
      |> based.execute(conn, int_service)
      |> should.be_ok

    let other_records =
      based.new_query("SELECT * FROM other_records")
      |> based.execute(conn, records_service)
      |> should.be_ok

    #(records.rows, int.rows, other_records.rows)
  }

  let #(records, int, other_records) = result

  records |> should.equal([])
  int |> should.equal([1])
  other_records |> should.equal([])
}

pub fn new_query_test() {
  let sql = "SELECT 1;"
  let query = based.new_query(sql)

  query.sql |> should.equal(sql)
  query.args |> list.length |> should.equal(0)
  query.decoder |> should.be_none
}

pub fn new_query_with_args_test() {
  let sql = "SELECT * FROM users WHERE id=$1;"
  let query =
    based.new_query(sql)
    |> based.with_args([based.int(1)])

  query.sql |> should.equal(sql)
  query.args |> list.length |> should.equal(1)
  query.decoder |> should.be_none
}

pub fn new_query_with_args_and_decoder_test() {
  let sql = "SELECT 1;"
  let decoder = dynamic.decode1(Record, dynamic.element(0, dynamic.int))

  let query =
    based.new_query(sql)
    |> based.with_args([based.int(1)])
    |> based.with_decoder(decoder)

  query.sql |> should.equal(sql)
  query.args |> list.length |> should.equal(1)
  query.decoder |> should.be_some
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
