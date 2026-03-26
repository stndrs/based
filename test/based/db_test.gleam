import based/db
import based/sql
import gleam/dynamic
import gleam/dynamic/decode
import gleam/int
import gleam/list
import gleam/result

pub fn query_test() {
  let sql = "SELECT * FROM users WHERE id=$1;"
  let rows = [dynamic.array([dynamic.int(1), dynamic.string("Steve")])]

  let returning = Ok(db.Queried(count: 1, fields: ["id", "name"], rows:))

  let assert Ok(_) =
    sql.query(sql)
    |> sql.params([sql.int(1)])
    |> db.query(query_handler(returning:))
}

pub fn query_error_test() {
  let sql = "SELECT * FROM users WHERE id=$1;"

  let returning = Error(db.DbError("something failed"))

  let assert Error(_) =
    sql.query(sql)
    |> sql.params([sql.int(1)])
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
    sql.query(sql)
    |> sql.params([sql.int(1)])
    |> db.all(query_handler(returning:), user_decoder())
}

pub fn all_error_test() {
  let sql = "SELECT * FROM users WHERE id=$1;"

  let returning = Error(db.DbError("something failed"))

  let assert Error(_) =
    sql.query(sql)
    |> sql.params([sql.int(1)])
    |> db.all(query_handler(returning:), user_decoder())
}

pub fn one_test() {
  let sql = "SELECT * FROM users WHERE id=$1;"
  let rows = [dynamic.array([dynamic.int(1), dynamic.string("Steve")])]

  let returning = Ok(db.Queried(count: 1, fields: ["id", "name"], rows:))

  let assert Ok(#(1, "Steve")) =
    sql.query(sql)
    |> sql.params([sql.int(1)])
    |> db.one(query_handler(returning:), user_decoder())
}

pub fn one_error_test() {
  let sql = "SELECT * FROM users WHERE id=$1;"

  let returning = Error(db.DbError("something failed"))

  let assert Error(_) =
    sql.query(sql)
    |> sql.params([sql.int(1)])
    |> db.one(query_handler(returning:), user_decoder())
}

pub fn transaction_test() {
  let db =
    db.driver(
      Conn,
      handle_query: fn(_, _) { Ok(db.Queried(0, [], [])) },
      handle_execute: fn(_, _) { Ok(0) },
      handle_batch: fn(_, _) { Ok([]) },
    )
    |> db.new(sql_adapter())

  let assert Ok("success") = {
    use _tx <- db.transaction(db, tx_handler)

    Ok("success")
  }
}

pub fn transaction_error_test() {
  let db =
    db.driver(
      Conn,
      handle_query: fn(_, _) { Ok(db.Queried(0, [], [])) },
      handle_execute: fn(_, _) { Ok(0) },
      handle_batch: fn(_, _) { Ok([]) },
    )
    |> db.new(sql_adapter())

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
) -> db.Db(sql.Value, Conn) {
  let db =
    db.driver(
      Conn,
      handle_query: fn(_, _) { queried },
      handle_execute: fn(_, _) { Ok(0) },
      handle_batch: fn(_, _) { Ok([]) },
    )
    |> db.new(sql_adapter())

  db
}

fn execute_handler(
  returning executed: Result(Int, db.DbError),
) -> db.Db(sql.Value, Conn) {
  let db =
    db.driver(
      Conn,
      handle_query: fn(_, _) { Ok(db.Queried(0, [], [])) },
      handle_execute: fn(_, _) { executed },
      handle_batch: fn(_, _) { Ok([]) },
    )
    |> db.new(sql_adapter())

  db
}

fn tx_handler(
  conn: Conn,
  next: fn(Conn) -> Result(t, error),
) -> Result(t, db.TransactionError(error)) {
  next(conn)
  |> result.map_error(db.Rollback)
}

pub fn error_to_string_connection_timeout_test() {
  let result = db.error_to_string(db.ConnectionTimeout)

  assert result == "[based/db.ConnectionTimeout]"
}

pub fn error_to_string_connection_error_test() {
  let result = db.error_to_string(db.ConnectionError("refused"))

  assert result == "[based/db.ConnectionError] refused"
}

pub fn error_to_string_database_error_test() {
  let result =
    db.error_to_string(db.DatabaseError(
      code: "42P01",
      name: "undefined_table",
      message: "relation does not exist",
    ))

  assert result
    == "[based/db.DatabaseError] code: 42P01, name: undefined_table, message: relation does not exist"
}

pub fn error_to_string_constraint_error_test() {
  let result =
    db.error_to_string(db.ConstraintError(
      code: "23505",
      name: "unique_violation",
      message: "duplicate key",
    ))

  assert result
    == "[based/db.ConstraintError] code: 23505, name: unique_violation, message: duplicate key"
}

pub fn error_to_string_syntax_error_test() {
  let result =
    db.error_to_string(db.SyntaxError(
      code: "42601",
      name: "syntax_error",
      message: "unexpected token",
    ))

  assert result
    == "[based/db.SyntaxError] code: 42601, name: syntax_error, message: unexpected token"
}

pub fn error_to_string_db_error_test() {
  let result = db.error_to_string(db.DbError("something failed"))

  assert result == "[based/db.DbError] something failed"
}

pub fn error_to_string_not_found_test() {
  let result = db.error_to_string(db.NotFound)

  assert result == "[based/db.NotFound]"
}

pub fn error_to_string_decode_error_test() {
  let result =
    db.error_to_string(
      db.DecodeError([
        decode.DecodeError(expected: "Int", found: "String", path: ["0"]),
      ]),
    )

  assert result
    == "[based/db.DecodeError] errors: [gleam/dynamic/decode.DecodeError] expected: Int, found: String, path: 0"
}

pub fn batch_test() {
  let rows = [dynamic.array([dynamic.int(1), dynamic.string("Steve")])]
  let returning = Ok([db.Queried(count: 1, fields: ["id", "name"], rows:)])

  let database =
    db.driver(
      Conn,
      handle_query: fn(_, _) { Ok(db.Queried(0, [], [])) },
      handle_execute: fn(_, _) { Ok(0) },
      handle_batch: fn(_, _) { returning },
    )
    |> db.new(sql_adapter())

  let queries = [
    sql.query("SELECT * FROM users WHERE id=$1;") |> sql.params([sql.int(1)]),
    sql.query("SELECT * FROM users WHERE id=$1;") |> sql.params([sql.int(2)]),
  ]

  let assert Ok(results) = db.batch(queries, database)
  assert list.length(results) == 1
}

pub fn batch_error_test() {
  let returning = Error(db.DbError("batch failed"))

  let database =
    db.driver(
      Conn,
      handle_query: fn(_, _) { Ok(db.Queried(0, [], [])) },
      handle_execute: fn(_, _) { Ok(0) },
      handle_batch: fn(_, _) { returning },
    )
    |> db.new(sql_adapter())

  let queries = [sql.query("SELECT 1;")]

  let assert Error(_) = db.batch(queries, database)
}

pub fn to_sql_query_select_test() {
  let database =
    db.driver(
      Conn,
      handle_query: fn(_, _) { Ok(db.Queried(0, [], [])) },
      handle_execute: fn(_, _) { Ok(0) },
      handle_batch: fn(_, _) { Ok([]) },
    )
    |> db.new(sql_adapter())

  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id"), sql.col("name")])
    |> sql.where([sql.col("id") |> sql.eq(sql.int(1), of: sql.value)])
    |> sql.to_query(database.adapter)

  assert q.sql == "SELECT id, name FROM users WHERE id = $1"
  assert q.values == [sql.int(1)]
}

pub fn to_sql_query_select_no_params_test() {
  let database = database()

  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("name")])
    |> sql.to_query(database.adapter)

  assert q.sql == "SELECT name FROM users"
  assert q.values == []
}

pub fn to_sql_query_insert_test() {
  let database = database()

  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values([
      {
        use <- sql.field(column: "name", value: sql.text("Alice"))
        sql.final(column: "age", value: sql.int(30))
      },
    ])
    |> sql.to_query(database.adapter)

  assert q.sql == "INSERT INTO users (name, age) VALUES ($1, $2)"
  assert q.values == [sql.text("Alice"), sql.int(30)]
}

pub fn to_sql_query_update_test() {
  let database = database()

  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("name", sql.text("Bob"), of: sql.value)
    |> sql.where([sql.col("id") |> sql.eq(sql.int(1), of: sql.value)])
    |> sql.to_query(database.adapter)

  assert q.sql == "UPDATE users SET name = $1 WHERE id = $2"
  assert q.values == [sql.text("Bob"), sql.int(1)]
}

pub fn to_sql_query_delete_test() {
  let database = database()

  let q =
    sql.from(sql.table("users"))
    |> sql.delete()
    |> sql.where([sql.col("id") |> sql.eq(sql.int(42), of: sql.value)])
    |> sql.to_query(database.adapter)

  assert q.sql == "DELETE FROM users WHERE id = $1"
  assert q.values == [sql.int(42)]
}

fn sql_adapter() -> sql.Adapter(sql.Value) {
  sql.adapter()
  |> sql.on_placeholder(fn(idx) { "$" <> int.to_string(idx + 1) })
}

pub fn one_not_found_test() {
  let sql = "SELECT * FROM users WHERE id=$1;"
  let returning = Ok(db.Queried(count: 0, fields: ["id", "name"], rows: []))

  let assert Error(db.NotFound) =
    sql.query(sql)
    |> sql.params([sql.int(999)])
    |> db.one(query_handler(returning:), user_decoder())
}

pub fn decode_success_test() {
  let rows = [dynamic.array([dynamic.int(1), dynamic.string("Steve")])]
  let queried = db.Queried(count: 1, fields: ["id", "name"], rows:)

  let assert Ok(returning) = db.decode(queried, user_decoder())
  assert returning.count == 1
  assert returning.rows == [#(1, "Steve")]
}

pub fn decode_empty_rows_test() {
  let queried = db.Queried(count: 0, fields: ["id", "name"], rows: [])

  let assert Ok(returning) = db.decode(queried, user_decoder())
  assert returning.count == 0
  assert returning.rows == []
}

pub fn decode_error_test() {
  // Provide a row that doesn't match the decoder (string where int expected)
  let rows = [
    dynamic.array([dynamic.string("not_an_int"), dynamic.string("Steve")]),
  ]
  let queried = db.Queried(count: 1, fields: ["id", "name"], rows:)

  let assert Error(db.DecodeError(_)) = db.decode(queried, user_decoder())
}

fn database() -> db.Db(sql.Value, Conn) {
  let database =
    db.driver(
      Conn,
      handle_query: fn(_, _) { Ok(db.Queried(0, [], [])) },
      handle_execute: fn(_, _) { Ok(0) },
      handle_batch: fn(_, _) { Ok([]) },
    )
    |> db.new(sql_adapter())

  database
}
