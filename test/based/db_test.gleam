import based/db
import gleam/dynamic
import gleam/dynamic/decode
import gleam/list
import gleam/result

pub fn sql_test() {
  let sql = "SELECT 1;"
  let query = db.sql(sql)

  let assert True = query.sql == sql
  let assert True = query.values |> list.length == 0
}

pub fn sql_with_values_test() {
  let sql = "SELECT * FROM users WHERE id=$1;"
  let query =
    db.sql(sql)
    |> db.params([db.int(1)])

  let assert True = query.sql == sql
  let assert True = query.values |> list.length == 1
}

pub fn query_test() {
  let sql = "SELECT * FROM users WHERE id=$1;"
  let rows = [dynamic.array([dynamic.int(1), dynamic.string("Steve")])]

  let returning = Ok(db.Queried(count: 1, fields: ["id", "name"], rows:))

  let assert Ok(_) =
    db.sql(sql)
    |> db.params([db.int(1)])
    |> db.query(query_handler(returning:))
}

pub fn query_error_test() {
  let sql = "SELECT * FROM users WHERE id=$1;"

  let returning = Error(db.DbError("something failed"))

  let assert Error(_) =
    db.sql(sql)
    |> db.params([db.int(1)])
    |> db.query(query_handler(returning:))
}

pub fn execute_test() {
  let sql = "INSERT INTO users (id, name) VALUES (2, 'William')"

  let assert Ok(1) = db.execute(sql, execute_handler(returning: Ok(1)))
}

pub fn execute_error_test() {
  let sql = "INSERT INTO users (id, name) VALUES (2, 'William')"

  let returning = Error(db.DbError("something failed"))

  let assert Error(_) = db.execute(sql, execute_handler(returning:))
}

pub fn all_test() {
  let sql = "SELECT * FROM users WHERE id=$1;"
  let rows = [dynamic.array([dynamic.int(1), dynamic.string("Steve")])]

  let returning = Ok(db.Queried(count: 1, fields: ["id", "name"], rows:))

  let assert Ok([#(1, "Steve")]) =
    db.sql(sql)
    |> db.params([db.int(1)])
    |> db.all(query_handler(returning:), user_decoder())
}

pub fn all_error_test() {
  let sql = "SELECT * FROM users WHERE id=$1;"

  let returning = Error(db.DbError("something failed"))

  let assert Error(_) =
    db.sql(sql)
    |> db.params([db.int(1)])
    |> db.all(query_handler(returning:), user_decoder())
}

pub fn one_test() {
  let sql = "SELECT * FROM users WHERE id=$1;"
  let rows = [dynamic.array([dynamic.int(1), dynamic.string("Steve")])]

  let returning = Ok(db.Queried(count: 1, fields: ["id", "name"], rows:))

  let assert Ok(#(1, "Steve")) =
    db.sql(sql)
    |> db.params([db.int(1)])
    |> db.one(query_handler(returning:), user_decoder())
}

pub fn one_error_test() {
  let sql = "SELECT * FROM users WHERE id=$1;"

  let returning = Error(db.DbError("something failed"))

  let assert Error(_) =
    db.sql(sql)
    |> db.params([db.int(1)])
    |> db.one(query_handler(returning:), user_decoder())
}

pub fn transaction_test() {
  let db = db.driver() |> db.new(Conn)

  let assert Ok("success") = {
    use _tx <- db.transaction(db, tx_handler)

    Ok("success")
  }
}

pub fn transaction_error_test() {
  let db = db.driver() |> db.new(Conn)

  let assert Error(db.Rollback("failure")) = {
    use _tx <- db.transaction(db, tx_handler)

    Error("failure")
  }
}

pub type Conn {
  Conn
}

fn user_decoder() -> decode.Decoder(#(Int, String)) {
  use id <- decode.field(0, decode.int)
  use name <- decode.field(1, decode.string)

  decode.success(#(id, name))
}

fn query_handler(
  returning queried: Result(db.Queried, db.DbError),
) -> db.Db(v, Conn) {
  db.driver()
  |> db.on_query(fn(_, _) { queried })
  |> db.new(Conn)
}

fn execute_handler(
  returning executed: Result(Int, db.DbError),
) -> db.Db(v, Conn) {
  db.driver()
  |> db.on_execute(fn(_, _) { executed })
  |> db.new(Conn)
}

fn tx_handler(
  conn: Conn,
  next: fn(Conn) -> Result(t, error),
) -> Result(t, db.TransactionError(error)) {
  next(conn)
  |> result.map_error(db.Rollback)
}

// error_to_string tests

pub fn error_to_string_connection_timeout_test() {
  let result = db.error_to_string(db.ConnectionTimeout)

  let assert True = result == "[based/db.ConnectionTimeout]"
}

pub fn error_to_string_connection_error_test() {
  let result = db.error_to_string(db.ConnectionError("refused"))

  let assert True = result == "[based/db.ConnectionError] refused"
}

pub fn error_to_string_database_error_test() {
  let result =
    db.error_to_string(db.DatabaseError(
      code: "42P01",
      name: "undefined_table",
      message: "relation does not exist",
    ))

  let assert True =
    result
    == "[based/db.DatabaseError] code: 42P01, name: undefined_table, message: relation does not exist"
}

pub fn error_to_string_constraint_error_test() {
  let result =
    db.error_to_string(db.ConstraintError(
      code: "23505",
      name: "unique_violation",
      message: "duplicate key",
    ))

  let assert True =
    result
    == "[based/db.ConstraintError] code: 23505, name: unique_violation, message: duplicate key"
}

pub fn error_to_string_syntax_error_test() {
  let result =
    db.error_to_string(db.SyntaxError(
      code: "42601",
      name: "syntax_error",
      message: "unexpected token",
    ))

  let assert True =
    result
    == "[based/db.SyntaxError] code: 42601, name: syntax_error, message: unexpected token"
}

pub fn error_to_string_db_error_test() {
  let result = db.error_to_string(db.DbError("something failed"))

  let assert True = result == "[based/db.DbError] something failed"
}

pub fn error_to_string_not_found_test() {
  let result = db.error_to_string(db.NotFound)

  let assert True = result == "[based/db.NotFound]"
}

pub fn error_to_string_decode_error_test() {
  let result =
    db.error_to_string(
      db.DecodeError([
        decode.DecodeError(expected: "Int", found: "String", path: ["0"]),
      ]),
    )

  let assert True =
    result
    == "[based/db.DecodeError] errors: [gleam/dynamic/decode.DecodeError] expected: Int, found: String, path: 0"
}

// batch tests

pub fn batch_test() {
  let rows = [dynamic.array([dynamic.int(1), dynamic.string("Steve")])]
  let returning = Ok([db.Queried(count: 1, fields: ["id", "name"], rows:)])

  let database =
    db.driver()
    |> db.on_batch(fn(_, _) { returning })
    |> db.new(Conn)

  let queries = [
    db.sql("SELECT * FROM users WHERE id=$1;") |> db.params([db.int(1)]),
    db.sql("SELECT * FROM users WHERE id=$1;") |> db.params([db.int(2)]),
  ]

  let assert Ok(results) = db.batch(queries, database)
  let assert True = list.length(results) == 1
}

pub fn batch_error_test() {
  let returning = Error(db.DbError("batch failed"))

  let database =
    db.driver()
    |> db.on_batch(fn(_, _) { returning })
    |> db.new(Conn)

  let queries = [db.sql("SELECT 1;")]

  let assert Error(_) = db.batch(queries, database)
}
