import based/db
import based/repo
import based/schema
import based/sql
import based/sql/column
import based/sql/delete
import based/sql/insert
import based/sql/select
import based/sql/update
import gleam/dynamic/decode

pub fn schema_column_test() {
  let repo =
    repo.new()
    |> repo.on_identifier(fn(ident) { "`" <> ident <> "`" })

  let users = schema.new(repo, "users", fn(_) { decode.dynamic })

  assert "`users`.`id`"
    == users
    |> schema.column("id")
    |> column.to_string(repo)
}

pub fn schema_select_test() {
  let repo = repo.new()

  let users = schema.new(repo, "users", fn(_) { decode.dynamic })

  let db.Query(sql:, values:) =
    users
    |> schema.select
    |> select.to_query

  assert "SELECT * FROM users" == sql
  assert [] == values
}

pub fn schema_insert_test() {
  let repo = repo.new()

  let users = schema.new(repo, "users", fn(_) { decode.dynamic })

  let db.Query(sql:, values:) =
    users
    |> schema.insert
    |> insert.columns(["id", "name"])
    |> insert.values([[db.int(10), db.text("Richard")]])
    |> insert.to_query

  assert "INSERT INTO users (id, name) VALUES (?, ?)" == sql
  assert [db.int(10), db.text("Richard")] == values
}

pub fn schema_delete_test() {
  let repo = repo.new()

  let users = schema.new(repo, "users", fn(_) { decode.dynamic })

  let db.Query(sql:, values:) =
    users
    |> schema.delete
    |> delete.to_query

  assert "DELETE FROM users" == sql
  assert [] == values
}

pub fn schema_update_test() {
  let repo = repo.new()

  let users = schema.new(repo, "users", fn(_) { decode.dynamic })

  let db.Query(sql:, values:) =
    users
    |> schema.update
    |> update.set("name", db.text("Dick"), of: sql.val)
    |> update.where([
      users
      |> schema.column("id")
      |> sql.eq(db.int(10), of: sql.val),
    ])
    |> update.to_query

  assert "UPDATE users SET name = ? WHERE users.id = ?" == sql
  assert [db.text("Dick"), db.int(10)] == values
}
