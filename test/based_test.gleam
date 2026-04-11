import based
import based/sql
import based/value.{type Value}
import gleam/dynamic
import gleam/dynamic/decode
import gleam/int
import gleam/list
import gleam/result
import gleeunit

pub fn main() {
  gleeunit.main()
}

pub fn query_test() {
  let sql = "SELECT * FROM users WHERE id=$1;"
  let rows = [dynamic.array([dynamic.int(1), dynamic.string("Steve")])]

  let returning = Ok(based.Queried(count: 1, fields: ["id", "name"], rows:))

  let assert Ok(_) =
    sql.query(sql)
    |> sql.params([value.int(1)])
    |> based.query(query_handler(returning:))
}

pub fn query_error_test() {
  let sql = "SELECT * FROM users WHERE id=$1;"

  let returning = Error(based.BasedError("something failed"))

  let assert Error(_) =
    sql.query(sql)
    |> sql.params([value.int(1)])
    |> based.query(query_handler(returning:))
}

pub fn execute_test() {
  let sql = "INSERT INTO users (id, name) VALUES (2, 'William')"

  let assert Ok(1) = based.execute(sql, execute_handler(returning: Ok(1)))
}

pub fn execute_error_test() {
  let sql = "INSERT INTO users (id, name) VALUES (2, 'William')"

  let returning = Error(based.BasedError("something failed"))

  let assert Error(_) = based.execute(sql, execute_handler(returning:))
}

pub fn all_test() {
  let sql = "SELECT * FROM users WHERE id=$1;"
  let rows = [dynamic.array([dynamic.int(1), dynamic.string("Steve")])]

  let returning = Ok(based.Queried(count: 1, fields: ["id", "name"], rows:))

  let assert Ok([#(1, "Steve")]) =
    sql.query(sql)
    |> sql.params([value.int(1)])
    |> based.all(query_handler(returning:), user_decoder())
}

pub fn all_error_test() {
  let sql = "SELECT * FROM users WHERE id=$1;"

  let returning = Error(based.BasedError("something failed"))

  let assert Error(_) =
    sql.query(sql)
    |> sql.params([value.int(1)])
    |> based.all(query_handler(returning:), user_decoder())
}

pub fn one_test() {
  let sql = "SELECT * FROM users WHERE id=$1;"
  let rows = [dynamic.array([dynamic.int(1), dynamic.string("Steve")])]

  let returning = Ok(based.Queried(count: 1, fields: ["id", "name"], rows:))

  let assert Ok(#(1, "Steve")) =
    sql.query(sql)
    |> sql.params([value.int(1)])
    |> based.one(query_handler(returning:), user_decoder())
}

pub fn one_error_test() {
  let sql = "SELECT * FROM users WHERE id=$1;"

  let returning = Error(based.BasedError("something failed"))

  let assert Error(_) =
    sql.query(sql)
    |> sql.params([value.int(1)])
    |> based.one(query_handler(returning:), user_decoder())
}

pub fn transaction_test() {
  let db =
    based.driver(
      Conn,
      on_query: fn(_, _) { Ok(based.Queried(0, [], [])) },
      on_execute: fn(_, _) { Ok(0) },
      on_batch: fn(_, _) { Ok([]) },
    )
    |> based.new(sql_adapter())

  let assert Ok("success") = {
    use _tx <- based.transaction(db, tx_handler)

    Ok("success")
  }
}

pub fn transaction_error_test() {
  let db =
    based.driver(
      Conn,
      on_query: fn(_, _) { Ok(based.Queried(0, [], [])) },
      on_execute: fn(_, _) { Ok(0) },
      on_batch: fn(_, _) { Ok([]) },
    )
    |> based.new(sql_adapter())

  let assert Error(based.Rollback("failure")) = {
    use _tx <- based.transaction(db, tx_handler)

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
  returning queried: Result(based.Queried, based.BasedError),
) -> based.Db(Value, Conn) {
  let db =
    based.driver(
      Conn,
      on_query: fn(_, _) { queried },
      on_execute: fn(_, _) { Ok(0) },
      on_batch: fn(_, _) { Ok([]) },
    )
    |> based.new(sql_adapter())

  db
}

fn execute_handler(
  returning executed: Result(Int, based.BasedError),
) -> based.Db(Value, Conn) {
  let db =
    based.driver(
      Conn,
      on_query: fn(_, _) { Ok(based.Queried(0, [], [])) },
      on_execute: fn(_, _) { executed },
      on_batch: fn(_, _) { Ok([]) },
    )
    |> based.new(sql_adapter())

  db
}

fn tx_handler(
  conn: Conn,
  next: fn(Conn) -> Result(t, error),
) -> Result(t, based.TransactionError(error)) {
  next(conn)
  |> result.map_error(based.Rollback)
}

pub fn error_to_string_connection_timeout_test() {
  let result = based.error_to_string(based.ConnectionTimeout)

  assert result == "[based.ConnectionTimeout]"
}

pub fn error_to_string_connection_error_test() {
  let result = based.error_to_string(based.ConnectionError("refused"))

  assert result == "[based.ConnectionError] refused"
}

pub fn error_to_string_database_error_test() {
  let result =
    based.error_to_string(based.DatabaseError(
      code: "42P01",
      name: "undefined_table",
      message: "relation does not exist",
    ))

  assert result
    == "[based.DatabaseError] code: 42P01, name: undefined_table, message: relation does not exist"
}

pub fn error_to_string_constraint_error_test() {
  let result =
    based.error_to_string(based.ConstraintError(
      code: "23505",
      name: "unique_violation",
      message: "duplicate key",
    ))

  assert result
    == "[based.ConstraintError] code: 23505, name: unique_violation, message: duplicate key"
}

pub fn error_to_string_syntax_error_test() {
  let result =
    based.error_to_string(based.SyntaxError(
      code: "42601",
      name: "syntax_error",
      message: "unexpected token",
    ))

  assert result
    == "[based.SyntaxError] code: 42601, name: syntax_error, message: unexpected token"
}

pub fn error_to_string_db_error_test() {
  let result = based.error_to_string(based.BasedError("something failed"))

  assert result == "[based.BasedError] something failed"
}

pub fn error_to_string_not_found_test() {
  let result = based.error_to_string(based.NotFound)

  assert result == "[based.NotFound]"
}

pub fn error_to_string_decode_error_test() {
  let result =
    based.error_to_string(
      based.DecodeError([
        decode.DecodeError(expected: "Int", found: "String", path: ["0"]),
      ]),
    )

  assert result
    == "[based.DecodeError] errors: [gleam/dynamic/decode.DecodeError] expected: Int, found: String, path: 0"
}

pub fn batch_test() {
  let rows = [dynamic.array([dynamic.int(1), dynamic.string("Steve")])]
  let returning = Ok([based.Queried(count: 1, fields: ["id", "name"], rows:)])

  let database =
    based.driver(
      Conn,
      on_query: fn(_, _) { Ok(based.Queried(0, [], [])) },
      on_execute: fn(_, _) { Ok(0) },
      on_batch: fn(_, _) { returning },
    )
    |> based.new(sql_adapter())

  let queries = [
    sql.query("SELECT * FROM users WHERE id=$1;") |> sql.params([value.int(1)]),
    sql.query("SELECT * FROM users WHERE id=$1;") |> sql.params([value.int(2)]),
  ]

  let assert Ok(results) = based.batch(queries, database)
  assert list.length(results) == 1
}

pub fn batch_error_test() {
  let returning = Error(based.BasedError("batch failed"))

  let database =
    based.driver(
      Conn,
      on_query: fn(_, _) { Ok(based.Queried(0, [], [])) },
      on_execute: fn(_, _) { Ok(0) },
      on_batch: fn(_, _) { returning },
    )
    |> based.new(sql_adapter())

  let queries = [sql.query("SELECT 1;")]

  let assert Error(_) = based.batch(queries, database)
}

pub fn to_query_select_test() {
  let database =
    based.driver(
      Conn,
      on_query: fn(_, _) { Ok(based.Queried(0, [], [])) },
      on_execute: fn(_, _) { Ok(0) },
      on_batch: fn(_, _) { Ok([]) },
    )
    |> based.new(sql_adapter())

  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.where([sql.column("id") |> sql.eq(value.int(1), of: sql.val)])
    |> based.to_query(database)

  assert q.sql == "SELECT id, name FROM users WHERE id = $1;"
  assert q.values == [value.int(1)]
}

pub fn to_query_select_no_params_test() {
  let database = database()

  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("name")])
    |> based.to_query(database)

  assert q.sql == "SELECT name FROM users;"
  assert q.values == []
}

pub fn to_query_insert_test() {
  let database = database()

  let row =
    sql.rows([#("Alice", 30)])
    |> sql.value("name", fn(r) { value.text(r.0) })
    |> sql.value("age", fn(r) { value.int(r.1) })
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values(row)
    |> based.to_query(database)

  assert q.sql == "INSERT INTO users (name, age) VALUES ($1, $2);"
  assert q.values == [value.text("Alice"), value.int(30)]
}

pub fn to_query_update_test() {
  let database = database()

  let q =
    sql.table("users")
    |> sql.update([sql.set("name", value.text("Bob"), of: sql.val)])
    |> sql.where([sql.column("id") |> sql.eq(value.int(1), of: sql.val)])
    |> based.to_query(database)

  assert q.sql == "UPDATE users SET name = $1 WHERE id = $2;"
  assert q.values == [value.text("Bob"), value.int(1)]
}

pub fn to_query_delete_test() {
  let database = database()

  let q =
    sql.from(sql.table("users"))
    |> sql.delete
    |> sql.where([sql.column("id") |> sql.eq(value.int(42), of: sql.val)])
    |> based.to_query(database)

  assert q.sql == "DELETE FROM users WHERE id = $1;"
  assert q.values == [value.int(42)]
}

pub fn to_sql_select_test() {
  let database =
    based.driver(
      Conn,
      on_query: fn(_, _) { Ok(based.Queried(0, [], [])) },
      on_execute: fn(_, _) { Ok(0) },
      on_batch: fn(_, _) { Ok([]) },
    )
    |> based.new(sql_adapter())

  let sql_string =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.where([sql.column("id") |> sql.eq(value.int(1), of: sql.val)])
    |> based.to_sql(database)

  assert sql_string == "SELECT id, name FROM users WHERE id = 1;"
}

pub fn to_sql_select_no_params_test() {
  let database = database()

  let sql_string =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("name")])
    |> based.to_sql(database)

  assert sql_string == "SELECT name FROM users;"
}

pub fn to_sql_insert_test() {
  let database = database()

  let row =
    sql.rows([#("Alice", 30)])
    |> sql.value("name", fn(r) { value.text(r.0) })
    |> sql.value("age", fn(r) { value.int(r.1) })

  let sql_string =
    sql.insert(into: sql.table("users"))
    |> sql.values(row)
    |> based.to_sql(database)

  assert sql_string == "INSERT INTO users (name, age) VALUES ('Alice', 30);"
}

pub fn to_sql_update_test() {
  let database = database()

  let sql_string =
    sql.table("users")
    |> sql.update([sql.set("name", value.text("Bob"), of: sql.val)])
    |> sql.where([sql.column("id") |> sql.eq(value.int(1), of: sql.val)])
    |> based.to_sql(database)

  assert sql_string == "UPDATE users SET name = 'Bob' WHERE id = 1;"
}

pub fn to_sql_delete_test() {
  let database = database()

  let sql_string =
    sql.from(sql.table("users"))
    |> sql.delete
    |> sql.where([sql.column("id") |> sql.eq(value.int(42), of: sql.val)])
    |> based.to_sql(database)

  assert sql_string == "DELETE FROM users WHERE id = 42;"
}

fn sql_adapter() -> sql.Adapter(Value) {
  value.adapter()
  |> sql.on_placeholder(fn(idx) { "$" <> int.to_string(idx) })
}

pub fn one_not_found_test() {
  let sql = "SELECT * FROM users WHERE id=$1;"
  let returning = Ok(based.Queried(count: 0, fields: ["id", "name"], rows: []))

  let assert Error(based.NotFound) =
    sql.query(sql)
    |> sql.params([value.int(999)])
    |> based.one(query_handler(returning:), user_decoder())
}

pub fn decode_success_test() {
  let rows = [dynamic.array([dynamic.int(1), dynamic.string("Steve")])]
  let queried = based.Queried(count: 1, fields: ["id", "name"], rows:)

  let assert Ok(returning) = based.decode(queried, user_decoder())
  assert returning.count == 1
  assert returning.rows == [#(1, "Steve")]
}

pub fn decode_empty_rows_test() {
  let queried = based.Queried(count: 0, fields: ["id", "name"], rows: [])

  let assert Ok(returning) = based.decode(queried, user_decoder())
  assert returning.count == 0
  assert returning.rows == []
}

pub fn decode_error_test() {
  // Provide a row that doesn't match the decoder (string where int expected)
  let rows = [
    dynamic.array([dynamic.string("not_an_int"), dynamic.string("Steve")]),
  ]
  let queried = based.Queried(count: 1, fields: ["id", "name"], rows:)

  let assert Error(based.DecodeError(_)) = based.decode(queried, user_decoder())
}

fn database() -> based.Db(Value, Conn) {
  let database =
    based.driver(
      Conn,
      on_query: fn(_, _) { Ok(based.Queried(0, [], [])) },
      on_execute: fn(_, _) { Ok(0) },
      on_batch: fn(_, _) { Ok([]) },
    )
    |> based.new(sql_adapter())

  database
}
