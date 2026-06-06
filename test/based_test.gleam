import based
import based/sql
import based/value.{type Value}
import gleam/dynamic
import gleam/dynamic/decode
import gleam/int
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

fn tx_handler(
  conn: Conn,
  next: fn(Conn) -> Result(t, error),
) -> Result(t, based.TransactionError(error)) {
  next(conn)
  |> result.map_error(based.Rollback)
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

pub fn error_to_string_connection_timeout_test() {
  let result = based.ConnectionTimeout |> based.DbError |> based.error_to_string

  assert result == "[based.ConnectionTimeout]"
}

pub fn error_to_string_connection_error_test() {
  let result =
    based.ConnectionError("refused") |> based.DbError |> based.error_to_string

  assert result == "[based.ConnectionError] refused"
}

pub fn error_to_string_database_error_test() {
  let result =
    based.DatabaseError(
      code: "42P01",
      name: "undefined_table",
      message: "relation does not exist",
    )
    |> based.DbError
    |> based.error_to_string

  assert result
    == "[based.DatabaseError] code: 42P01, name: undefined_table, message: relation does not exist"
}

pub fn error_to_string_constraint_error_test() {
  let result =
    based.ConstraintError(
      code: "23505",
      name: "unique_violation",
      message: "duplicate key",
    )
    |> based.DbError
    |> based.error_to_string

  assert result
    == "[based.ConstraintError] code: 23505, name: unique_violation, message: duplicate key"
}

pub fn error_to_string_syntax_error_test() {
  let result =
    based.SyntaxError(
      code: "42601",
      name: "syntax_error",
      message: "unexpected token",
    )
    |> based.DbError
    |> based.error_to_string

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
  let rows = [
    dynamic.array([dynamic.string("not_an_int"), dynamic.string("Steve")]),
  ]
  let queried = based.Queried(count: 1, fields: ["id", "name"], rows:)

  let assert Error(based.DecodeError(_)) = based.decode(queried, user_decoder())
}

pub fn one_returns_first_of_multiple_rows_test() {
  let sql = "SELECT * FROM users;"
  let rows = [
    dynamic.array([dynamic.int(1), dynamic.string("Steve")]),
    dynamic.array([dynamic.int(2), dynamic.string("Alice")]),
  ]

  let returning = Ok(based.Queried(count: 2, fields: ["id", "name"], rows:))

  let assert Ok(#(1, "Steve")) =
    sql.query(sql)
    |> based.one(query_handler(returning:), user_decoder())
}

pub fn error_to_string_connection_unavailable_test() {
  let result =
    based.ConnectionUnavailable |> based.DbError |> based.error_to_string

  assert result == "[based.ConnectionUnavailable]"
}

pub fn error_to_string_unique_violation_test() {
  let result =
    based.UniqueViolation(
      code: "23505",
      name: "unique_violation",
      message: "duplicate key value violates unique constraint",
    )
    |> based.DbError
    |> based.error_to_string

  assert result
    == "[based.UniqueViolation] code: 23505, name: unique_violation, message: duplicate key value violates unique constraint"
}

pub fn error_to_string_foreign_key_violation_test() {
  let result =
    based.ForeignKeyViolation(
      code: "23503",
      name: "foreign_key_violation",
      message: "insert or update on table violates foreign key constraint",
    )
    |> based.DbError
    |> based.error_to_string

  assert result
    == "[based.ForeignKeyViolation] code: 23503, name: foreign_key_violation, message: insert or update on table violates foreign key constraint"
}

pub fn error_to_string_not_null_violation_test() {
  let result =
    based.NotNullViolation(
      code: "23502",
      name: "not_null_violation",
      message: "null value in column violates not-null constraint",
    )
    |> based.DbError
    |> based.error_to_string

  assert result
    == "[based.NotNullViolation] code: 23502, name: not_null_violation, message: null value in column violates not-null constraint"
}

pub fn error_to_string_check_violation_test() {
  let result =
    based.CheckViolation(
      code: "23514",
      name: "check_violation",
      message: "new row for relation violates check constraint",
    )
    |> based.DbError
    |> based.error_to_string

  assert result
    == "[based.CheckViolation] code: 23514, name: check_violation, message: new row for relation violates check constraint"
}

pub fn error_to_string_deadlock_detected_test() {
  let result =
    based.DeadlockDetected(
      code: "40P01",
      name: "deadlock_detected",
      message: "detected deadlock while trying to acquire lock",
    )
    |> based.DbError
    |> based.error_to_string

  assert result
    == "[based.DeadlockDetected] code: 40P01, name: deadlock_detected, message: detected deadlock while trying to acquire lock"
}

pub fn error_to_string_serialization_failure_test() {
  let result =
    based.SerializationFailure(
      code: "40001",
      name: "serialization_failure",
      message: "could not serialize access due to concurrent update",
    )
    |> based.DbError
    |> based.error_to_string

  assert result
    == "[based.SerializationFailure] code: 40001, name: serialization_failure, message: could not serialize access due to concurrent update"
}

pub fn error_to_string_query_timeout_test() {
  let result =
    based.QueryTimeout(
      code: "57014",
      name: "query_canceled",
      message: "canceling statement due to statement timeout",
    )
    |> based.DbError
    |> based.error_to_string

  assert result
    == "[based.QueryTimeout] code: 57014, name: query_canceled, message: canceling statement due to statement timeout"
}

pub fn error_to_string_permission_denied_test() {
  let result =
    based.PermissionDenied(
      code: "42501",
      name: "insufficient_privilege",
      message: "permission denied for table",
    )
    |> based.DbError
    |> based.error_to_string

  assert result
    == "[based.PermissionDenied] code: 42501, name: insufficient_privilege, message: permission denied for table"
}

pub fn error_to_string_read_only_transaction_test() {
  let result =
    based.ReadOnlyTransaction(
      code: "25006",
      name: "read_only_sql_transaction",
      message: "cannot execute write in a read-only transaction",
    )
    |> based.DbError
    |> based.error_to_string

  assert result
    == "[based.ReadOnlyTransaction] code: 25006, name: read_only_sql_transaction, message: cannot execute write in a read-only transaction"
}

pub fn database_error_to_string_test() {
  let result =
    based.database_error_to_string(based.DatabaseError(
      code: "42P01",
      name: "undefined_table",
      message: "relation does not exist",
    ))

  assert result
    == "[based.DatabaseError] code: 42P01, name: undefined_table, message: relation does not exist"
}
