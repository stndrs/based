import based.{Query, Returned}
import gleam/dynamic
import gleam/option.{None, Some}
import gleeunit
import gleeunit/should
import testing

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
    let returning = Ok(Returned(1, [Record(1)]))

    use db <- testing.with_connection(returning)

    query |> based.exec(db)
  }

  result |> should.be_ok
}

pub fn exec_without_return_test() {
  let query =
    Query(sql: "INSERT INTO records (id) VALUES (?);", args: [], decoder: None)

  let result = {
    let returning = Ok(Returned(0, []))

    use db <- testing.with_connection(returning)

    query |> based.exec(db)
  }

  result |> should.be_ok
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
