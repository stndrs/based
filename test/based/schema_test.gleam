import based
import based/db
import based/schema
import based/sql
import based/sql/column
import based/sql/delete
import based/sql/insert
import based/sql/select
import based/sql/update
import based/value
import gleam/dynamic/decode

pub fn schema_column_test() {
  let repo =
    based.repo()
    |> based.on_identifier(fn(ident) { "`" <> ident <> "`" })

  let users = schema.new("users", fn() { decode.dynamic })

  assert "`users`.`id`"
    == users
    |> schema.column("id")
    |> column.to_string(repo)
}

pub fn schema_select_test() {
  let sql = based.repo()

  let users = schema.new("users", fn() { decode.dynamic })

  let db.Query(sql:, values:) =
    users
    |> schema.select(sql)
    |> select.to_query

  assert "SELECT * FROM users" == sql
  assert [] == values
}

pub fn schema_insert_test() {
  let sql = based.repo()

  let users = schema.new("users", fn() { decode.dynamic })

  let db.Query(sql:, values:) =
    users
    |> schema.insert(sql)
    |> insert.columns(["id", "name"])
    |> insert.values([[value.int(10), value.text("Richard")]])
    |> insert.to_query

  assert "INSERT INTO users (id, name) VALUES (?, ?)" == sql
  assert [value.int(10), value.text("Richard")] == values
}

pub fn schema_delete_test() {
  let sql = based.repo()

  let users = schema.new("users", fn() { decode.dynamic })

  let db.Query(sql:, values:) =
    users
    |> schema.delete(sql)
    |> delete.to_query

  assert "DELETE FROM users" == sql
  assert [] == values
}

pub fn schema_update_test() {
  let sql = based.repo()

  let users = schema.new("users", fn() { decode.dynamic })

  let db.Query(sql:, values:) =
    users
    |> schema.update(sql)
    |> update.set("name", value.text("Dick"), of: sql.value)
    |> update.where([
      users
      |> schema.column("id")
      |> column.eq(value.int(10), of: sql.value),
    ])
    |> update.to_query

  assert "UPDATE users SET name = ? WHERE users.id = ?" == sql
  assert [value.text("Dick"), value.int(10)] == values
}
