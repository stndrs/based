import based/interval
import based/sql
import based/uuid
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/string
import gleam/time/calendar
import gleam/time/duration
import gleam/time/timestamp

fn a() -> sql.Adapter(sql.Value) {
  sql.adapter()
  |> sql.on_placeholder(fn(idx) { "$" <> int.to_string(idx) })
  |> sql.on_null(with: fn() { sql.null })
  |> sql.on_int(with: fn(i) { sql.int(i) })
  |> sql.on_text(with: fn(s) { sql.text(s) })
  |> sql.on_value(with: sql.value_to_string)
}

fn backtick_a() -> sql.Adapter(sql.Value) {
  sql.adapter()
  |> sql.on_null(with: fn() { sql.null })
  |> sql.on_int(with: fn(i) { sql.int(i) })
  |> sql.on_text(with: fn(s) { sql.text(s) })
  |> sql.on_placeholder(with: fn(_i) { "?" })
  |> sql.on_value(with: sql.value_to_string)
  |> sql.on_identifier(with: fn(name) { "`" <> name <> "`" })
}

/// Adapter that uses the default built-in value handler (handles all Value
/// variants including Bytea, Timestamp, Uuid, Array, etc).
fn default_a() -> sql.Adapter(sql.Value) {
  sql.adapter()
  |> sql.on_placeholder(fn(idx) { "$" <> int.to_string(idx) })
}

pub fn sql_test() {
  let sql = "SELECT 1;"
  let query = sql.query(sql)

  assert query.sql == sql
  assert query.values == []
}

pub fn sql_with_values_test() {
  let sql = "SELECT * FROM users WHERE id=$1;"
  let query =
    sql.query(sql)
    |> sql.params([sql.int(1)])

  assert query.sql == sql
  assert list.length(query.values) == 1
}

pub fn select_all_to_query_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users"
  assert q.values == []
}

pub fn select_columns_to_query_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("name"), sql.column("age")])
    |> sql.to_query(a())

  assert q.sql == "SELECT name, age FROM users"
  assert q.values == []
}

pub fn select_qualified_columns_test() {
  let users = sql.table("users") |> sql.table_as("u")

  let q =
    sql.from(users)
    |> sql.select([
      sql.column("name") |> sql.column_for(users),
      sql.column("email") |> sql.column_for(users),
    ])
    |> sql.to_query(a())

  assert q.sql == "SELECT u.name, u.email FROM users AS u"
}

pub fn select_aliased_column_test() {
  let users = sql.table("users") |> sql.table_as("u")

  let q =
    sql.from(users)
    |> sql.select([
      sql.column("email")
      |> sql.column_for(users)
      |> sql.column_as("user_email"),
    ])
    |> sql.to_query(a())

  assert q.sql == "SELECT u.email AS user_email FROM users AS u"
}

pub fn select_where_eq_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where([sql.column("age") |> sql.eq(sql.int(21), of: sql.val)])
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE age = $1"
  assert q.values == [sql.int(21)]
}

pub fn select_multiple_wheres_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where([
      sql.column("active") |> sql.eq(sql.true, of: sql.val),
      sql.column("age") |> sql.gt(sql.int(18), of: sql.val),
    ])
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE (active = $1 AND age > $2)"
  assert q.values == [sql.true, sql.int(18)]
}

pub fn select_or_where_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where([
      sql.or(
        sql.column("role") |> sql.eq(sql.text("admin"), of: sql.val),
        sql.column("role") |> sql.eq(sql.text("superadmin"), of: sql.val),
      ),
    ])
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE (role = $1 OR role = $2)"
  assert q.values == [sql.text("admin"), sql.text("superadmin")]
}

pub fn select_where_between_test() {
  let q =
    sql.from(sql.table("products"))
    |> sql.select([sql.star])
    |> sql.where([
      sql.column("price")
      |> sql.between(sql.float(10.0), sql.float(100.0), of: sql.val),
    ])
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM products WHERE price BETWEEN $1 AND $2"
  assert q.values == [sql.float(10.0), sql.float(100.0)]
}

pub fn select_where_in_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where([
      sql.column("id")
      |> sql.in(
        [
          sql.int(1),
          sql.int(2),
          sql.int(3),
        ],
        of: sql.val,
      ),
    ])
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE id IN ($1, $2, $3)"
  assert q.values == [sql.int(1), sql.int(2), sql.int(3)]
}

pub fn select_where_is_null_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where([sql.column("deleted_at") |> sql.is_null])
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE deleted_at IS NULL"
  assert q.values == []
}

pub fn select_where_is_not_null_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where([sql.column("email") |> sql.is_not_null])
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE email IS NOT NULL"
  assert q.values == []
}

pub fn select_where_not_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where([
      sql.not(sql.column("active") |> sql.eq(sql.true, of: sql.val)),
    ])
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE NOT (active = $1)"
  assert q.values == [sql.true]
}

pub fn select_where_like_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where([
      sql.column("name") |> sql.like(sql.text("%john%"), of: sql.val),
    ])
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE name LIKE $1"
  assert q.values == [sql.text("%john%")]
}

pub fn select_inner_join_test() {
  let users = sql.table("users") |> sql.table_as("u")
  let orders = sql.table("orders") |> sql.table_as("o")

  let q =
    sql.from(users)
    |> sql.select([
      sql.column("name"),
      sql.column("total") |> sql.column_for(orders),
    ])
    |> sql.inner_join(table: orders, on: [
      sql.column("id")
      |> sql.column_for(users)
      |> sql.eq(sql.column("user_id") |> sql.column_for(orders), of: sql.col),
    ])
    |> sql.to_query(a())

  assert q.sql
    == "SELECT name, o.total FROM users AS u INNER JOIN orders AS o ON u.id = o.user_id"
  assert q.values == []
}

pub fn select_left_join_test() {
  let users = sql.table("users") |> sql.table_as("u")
  let profiles = sql.table("profiles") |> sql.table_as("p")

  let q =
    sql.from(users)
    |> sql.select([sql.star])
    |> sql.left_join(table: profiles, on: [
      sql.column("id")
      |> sql.column_for(users)
      |> sql.eq(sql.column("user_id") |> sql.column_for(profiles), of: sql.col),
    ])
    |> sql.to_query(a())

  assert q.sql
    == "SELECT * FROM users AS u LEFT JOIN profiles AS p ON u.id = p.user_id"
}

pub fn select_order_by_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.order_by([
      sql.column("name") |> sql.asc,
      sql.column("age") |> sql.desc,
    ])
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users ORDER BY name ASC, age DESC"
}

pub fn select_limit_offset_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.limit(10)
    |> sql.offset(20)
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users LIMIT 10 OFFSET 20"
}

pub fn select_group_by_test() {
  let q =
    sql.from(sql.table("employees"))
    |> sql.select([sql.column("department"), sql.column("count")])
    |> sql.group_by([sql.column("department")])
    |> sql.to_query(a())

  assert q.sql == "SELECT department, count FROM employees GROUP BY department"
}

pub fn insert_single_row_test() {
  let inserter =
    sql.rows([#("Alice", 30)])
    |> sql.value("name", fn(r) { sql.text(r.0) })
    |> sql.value("age", fn(r) { sql.int(r.1) })

  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values(inserter)
    |> sql.to_query(a())

  assert q.sql == "INSERT INTO users (name, age) VALUES ($1, $2)"
  assert q.values == [sql.text("Alice"), sql.int(30)]
}

pub fn insert_multiple_rows_test() {
  let inserter =
    sql.rows([#("Alice", 30), #("Bob", 25)])
    |> sql.value("name", fn(r) { sql.text(r.0) })
    |> sql.value("age", fn(r) { sql.int(r.1) })
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values(inserter)
    |> sql.to_query(a())

  assert q.sql == "INSERT INTO users (name, age) VALUES ($1, $2), ($3, $4)"
  assert q.values
    == [sql.text("Alice"), sql.int(30), sql.text("Bob"), sql.int(25)]
}

pub fn insert_nullable_field_test() {
  let inserter =
    sql.rows([#(None, 25)])
    |> sql.value("name", fn(r: #(Option(String), Int)) {
      sql.nullable(r.0, of: sql.text)
    })
    |> sql.value("age", fn(r) { sql.int(r.1) })
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values(inserter)
    |> sql.to_query(a())

  assert q.sql == "INSERT INTO users (name, age) VALUES ($1, $2)"
  assert q.values == [sql.Null, sql.int(25)]
}

pub fn insert_multiple_nullable_fields_test() {
  let inserter =
    sql.rows([#(None, 30), #(Some("Bob"), 25)])
    |> sql.value("name", fn(r: #(Option(String), Int)) {
      sql.nullable(r.0, of: sql.text)
    })
    |> sql.value("age", fn(r) { sql.int(r.1) })
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values(inserter)
    |> sql.to_query(a())

  assert q.sql == "INSERT INTO users (name, age) VALUES ($1, $2), ($3, $4)"
  assert q.values == [sql.Null, sql.int(30), sql.text("Bob"), sql.int(25)]
}

pub fn update_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("name", sql.text("Alice"), of: sql.val)
    |> sql.set("age", sql.int(31), of: sql.val)
    |> sql.where([sql.column("id") |> sql.eq(sql.int(1), of: sql.val)])
    |> sql.to_query(a())

  assert q.sql == "UPDATE users SET name = $1, age = $2 WHERE id = $3"
  assert q.values == [sql.text("Alice"), sql.int(31), sql.int(1)]
}

pub fn update_no_where_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("active", sql.false, of: sql.val)
    |> sql.to_query(a())

  assert q.sql == "UPDATE users SET active = $1"
  assert q.values == [sql.false]
}

pub fn delete_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.delete()
    |> sql.where([sql.column("id") |> sql.eq(sql.int(42), of: sql.val)])
    |> sql.to_query(a())

  assert q.sql == "DELETE FROM users WHERE id = $1"
  assert q.values == [sql.int(42)]
}

pub fn delete_no_where_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.delete()
    |> sql.to_query(a())

  assert q.sql == "DELETE FROM users"
  assert q.values == []
}

pub fn to_string_select_test() {
  let s =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("name"), sql.column("age")])
    |> sql.where([sql.column("age") |> sql.eq(sql.int(21), of: sql.val)])
    |> sql.to_string(a())

  assert s == "SELECT name, age FROM users WHERE age = 21"
}

pub fn to_string_insert_test() {
  let inserter =
    sql.rows([#("Alice", True)])
    |> sql.value("name", fn(r) { sql.text(r.0) })
    |> sql.value("active", fn(r) { sql.bool(r.1) })
  let s =
    sql.insert(into: sql.table("users"))
    |> sql.values(inserter)
    |> sql.to_string(a())

  assert s == "INSERT INTO users (name, active) VALUES ('Alice', TRUE)"
}

pub fn to_string_update_test() {
  let s =
    sql.update(table: sql.table("users"))
    |> sql.set("name", sql.text("Bob"), of: sql.val)
    |> sql.where([sql.column("id") |> sql.eq(sql.int(1), of: sql.val)])
    |> sql.to_string(a())

  assert s == "UPDATE users SET name = 'Bob' WHERE id = 1"
}

pub fn to_string_delete_test() {
  let s =
    sql.from(sql.table("users"))
    |> sql.delete()
    |> sql.where([sql.column("age") |> sql.lt(sql.int(18), of: sql.val)])
    |> sql.to_string(a())

  assert s == "DELETE FROM users WHERE age < 18"
}

pub fn default_formatter_placeholder_test() {
  let r =
    sql.adapter()
    |> sql.on_placeholder(fn(idx) { "$" <> int.to_string(idx) })

  let q =
    sql.from(sql.table("users"))
    |> sql.select([
      sql.column("a"),
      sql.column("b"),
      sql.column("c"),
      sql.column("d"),
      sql.column("e"),
    ])
    |> sql.where([sql.column("x") |> sql.eq(sql.int(1), of: sql.val)])
    |> sql.to_query(r)

  assert q.sql == "SELECT a, b, c, d, e FROM users WHERE x = $1"
}

pub fn default_formatter_quote_test() {
  let r =
    sql.adapter()
    |> sql.on_placeholder(fn(idx) { "$" <> int.to_string(idx + 1) })

  // default_formatter uses identity for quote_identifier, so names are unquoted
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("name")])
    |> sql.to_query(r)
  assert q.sql == "SELECT name FROM users"
}

pub fn default_formatter_value_to_string_test() {
  let r =
    sql.adapter()
    |> sql.on_placeholder(fn(idx) { "$" <> int.to_string(idx + 1) })

  let base =
    sql.from(sql.table("t"))
    |> sql.select([sql.column("x")])

  assert sql.to_string(
      base |> sql.where([sql.column("x") |> sql.eq(sql.int(42), of: sql.val)]),
      r,
    )
    == "SELECT x FROM t WHERE x = 42"
  assert sql.to_string(
      base
        |> sql.where([sql.column("x") |> sql.eq(sql.text("hello"), of: sql.val)]),
      r,
    )
    == "SELECT x FROM t WHERE x = 'hello'"
  assert sql.to_string(
      base |> sql.where([sql.column("x") |> sql.eq(sql.true, of: sql.val)]),
      r,
    )
    == "SELECT x FROM t WHERE x = TRUE"
  assert sql.to_string(
      base |> sql.where([sql.column("x") |> sql.eq(sql.false, of: sql.val)]),
      r,
    )
    == "SELECT x FROM t WHERE x = FALSE"
  assert sql.to_string(
      base |> sql.where([sql.column("x") |> sql.eq(sql.null, of: sql.val)]),
      r,
    )
    == "SELECT x FROM t WHERE x = NULL"
}

pub fn default_formatter_escapes_quotes_test() {
  let r =
    sql.adapter()
    |> sql.on_placeholder(fn(idx) { "$" <> int.to_string(idx + 1) })

  let s =
    sql.from(sql.table("t"))
    |> sql.select([sql.column("x")])
    |> sql.where([sql.column("x") |> sql.eq(sql.text("it's"), of: sql.val)])
    |> sql.to_string(r)
  assert s == "SELECT x FROM t WHERE x = 'it''s'"
}

pub fn default_formatter_to_query_test() {
  let r =
    sql.adapter()
    |> sql.on_placeholder(fn(idx) { "$" <> int.to_string(idx) })

  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("name"), sql.column("email")])
    |> sql.where([sql.column("active") |> sql.eq(sql.true, of: sql.val)])
    |> sql.limit(5)
    |> sql.to_query(r)

  assert q.sql == "SELECT name, email FROM users WHERE active = $1 LIMIT 5"
  assert q.values == [sql.true]
}

pub fn custom_backtick_formatter_test() {
  let backtick_r =
    sql.adapter()
    |> sql.on_null(with: fn() { sql.null })
    |> sql.on_int(with: fn(i) { sql.int(i) })
    |> sql.on_text(with: fn(s) { sql.text(s) })
    |> sql.on_placeholder(with: fn(_) { "?" })
    |> sql.on_value(with: sql.value_to_string)
    |> sql.on_identifier(with: fn(name) { "`" <> name <> "`" })

  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("name")])
    |> sql.where([sql.column("id") |> sql.eq(sql.int(1), of: sql.val)])
    |> sql.to_query(backtick_r)

  assert q.sql == "SELECT `name` FROM `users` WHERE `id` = ?"
  assert q.values == [sql.int(1)]
}

pub fn backtick_quote_identifier_test() {
  let f =
    sql.adapter()
    |> sql.on_null(with: fn() { sql.null })
    |> sql.on_int(with: fn(i) { sql.int(i) })
    |> sql.on_text(with: fn(s) { sql.text(s) })
    |> sql.on_placeholder(with: fn(_) { "?" })
    |> sql.on_value(with: sql.value_to_string)
    |> sql.on_identifier(with: fn(name) { "`" <> name <> "`" })

  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("name")])
    |> sql.where([sql.column("id") |> sql.eq(sql.int(1), of: sql.val)])
    |> sql.to_query(f)

  assert q.sql == "SELECT `name` FROM `users` WHERE `id` = ?"

  let s =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("name")])
    |> sql.where([sql.column("id") |> sql.eq(sql.int(1), of: sql.val)])
    |> sql.to_string(f)

  assert s == "SELECT `name` FROM `users` WHERE `id` = 1"
}

pub fn double_quote_quote_identifier_test() {
  let f =
    sql.adapter()
    |> sql.on_null(with: fn() { sql.null })
    |> sql.on_int(with: fn(i) { sql.int(i) })
    |> sql.on_text(with: fn(s) { sql.text(s) })
    |> sql.on_value(with: sql.value_to_string)
    |> sql.on_identifier(with: fn(name) { "\"" <> name <> "\"" })
    |> sql.on_placeholder(fn(idx) { "$" <> int.to_string(idx) })

  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("name")])
    |> sql.where([sql.column("id") |> sql.eq(sql.int(1), of: sql.val)])
    |> sql.to_query(f)

  assert q.sql == "SELECT \"name\" FROM \"users\" WHERE \"id\" = $1"

  let s =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("name")])
    |> sql.where([sql.column("id") |> sql.eq(sql.int(1), of: sql.val)])
    |> sql.to_string(f)

  assert s == "SELECT \"name\" FROM \"users\" WHERE \"id\" = 1"
}

pub fn double_quote_aliased_identifiers_test() {
  let f =
    sql.adapter()
    |> sql.on_null(with: fn() { sql.null })
    |> sql.on_int(with: fn(i) { sql.int(i) })
    |> sql.on_text(with: fn(s) { sql.text(s) })
    |> sql.on_value(with: sql.value_to_string)
    |> sql.on_identifier(with: fn(name) { "\"" <> name <> "\"" })
    |> sql.on_placeholder(fn(idx) { "$" <> int.to_string(idx) })

  let users = sql.table("users") |> sql.table_as("u")
  let orders = sql.table("orders") |> sql.table_as("o")

  let q =
    sql.from(users)
    |> sql.select([
      sql.column("name") |> sql.column_for(users),
      sql.column("total")
        |> sql.column_for(orders)
        |> sql.column_as("order_total"),
    ])
    |> sql.inner_join(table: orders, on: [
      sql.column("id")
      |> sql.column_for(users)
      |> sql.eq(sql.column("user_id") |> sql.column_for(orders), of: sql.col),
    ])
    |> sql.where([
      sql.column("active")
      |> sql.column_for(users)
      |> sql.eq(sql.true, of: sql.val),
    ])
    |> sql.to_query(f)

  assert q.sql
    == "SELECT \"u\".\"name\", \"o\".\"total\" AS \"order_total\" FROM \"users\" AS \"u\" INNER JOIN \"orders\" AS \"o\" ON \"u\".\"id\" = \"o\".\"user_id\" WHERE \"u\".\"active\" = $1"
  assert q.values == [sql.Bool(True)]
}

pub fn double_quote_question_mark_identifier_test() {
  let f =
    sql.adapter()
    |> sql.on_null(with: fn() { sql.null })
    |> sql.on_int(with: fn(i) { sql.int(i) })
    |> sql.on_text(with: fn(s) { sql.text(s) })
    |> sql.on_placeholder(with: fn(_) { "?" })
    |> sql.on_value(with: sql.value_to_string)
    |> sql.on_identifier(with: fn(name) { "\"" <> name <> "\"" })

  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("name")])
    |> sql.where([sql.column("id") |> sql.eq(sql.int(1), of: sql.val)])
    |> sql.to_query(f)

  assert q.sql == "SELECT \"name\" FROM \"users\" WHERE \"id\" = ?"

  let s =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("name")])
    |> sql.where([sql.column("id") |> sql.eq(sql.int(1), of: sql.val)])
    |> sql.to_string(f)

  assert s == "SELECT \"name\" FROM \"users\" WHERE \"id\" = 1"
}

pub type MyValue {
  MyInt(Int)
  MyStr(String)
}

pub fn generic_value_type_test() {
  let my_adapter =
    sql.new_adapter()
    |> sql.on_null(with: fn() { MyStr("NULL") })
    |> sql.on_int(with: fn(i) { MyInt(i) })
    |> sql.on_text(with: fn(s) { MyStr(s) })
    |> sql.on_placeholder(with: fn(i) { "?" <> int.to_string(i) })
    |> sql.on_value(with: fn(v: MyValue) {
      case v {
        MyInt(n) -> int.to_string(n)
        MyStr(s) -> "'" <> s <> "'"
      }
    })
    |> sql.on_identifier(with: fn(name) { "[" <> name <> "]" })

  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("name")])
    |> sql.where([sql.column("id") |> sql.eq(MyInt(42), of: sql.val)])
    |> sql.to_query(my_adapter)

  assert q.sql == "SELECT [name] FROM [users] WHERE [id] = ?1"
  assert q.values == [MyInt(42)]

  let s =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("name")])
    |> sql.where([sql.column("id") |> sql.eq(MyInt(42), of: sql.val)])
    |> sql.to_string(my_adapter)

  assert s == "SELECT [name] FROM [users] WHERE [id] = 42"
}

pub fn complex_query_test() {
  let users = sql.table("users") |> sql.table_as("u")
  let orders = sql.table("orders") |> sql.table_as("o")

  let q =
    sql.from(sql.table("users") |> sql.table_as("u"))
    |> sql.select([
      sql.column("name") |> sql.column_for(users),
      sql.column("email") |> sql.column_for(users),
      sql.column("total")
        |> sql.column_for(orders)
        |> sql.column_as("order_total"),
    ])
    |> sql.inner_join(table: orders, on: [
      sql.column("id")
      |> sql.column_for(users)
      |> sql.eq(sql.column("user_id") |> sql.column_for(orders), of: sql.col),
    ])
    |> sql.where([
      sql.column("total")
        |> sql.column_for(orders)
        |> sql.gt(sql.float(50.0), of: sql.val),
      sql.column("active")
        |> sql.column_for(users)
        |> sql.eq(sql.true, of: sql.val),
    ])
    |> sql.order_by([sql.column("total") |> sql.column_for(orders) |> sql.desc])
    |> sql.limit(10)
    |> sql.offset(0)
    |> sql.to_query(a())

  assert q.sql
    == "SELECT u.name, u.email, o.total AS order_total FROM users AS u INNER JOIN orders AS o ON u.id = o.user_id WHERE (o.total > $1 AND u.active = $2) ORDER BY o.total DESC LIMIT 10 OFFSET 0"
  assert q.values == [sql.float(50.0), sql.true]
}

pub fn column_to_column_where_test() {
  let q =
    sql.from(sql.table("products"))
    |> sql.select([sql.star])
    |> sql.where([
      sql.column("price") |> sql.gt(sql.column("cost"), of: sql.col),
    ])
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM products WHERE price > cost"
  assert q.values == []
}

pub fn select_distinct_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("name")])
    |> sql.distinct
    |> sql.to_query(a())

  assert q.sql == "SELECT DISTINCT name FROM users"
  assert q.values == []
}

pub fn select_distinct_multiple_columns_test() {
  let q =
    sql.from(sql.table("employees"))
    |> sql.select([sql.column("department"), sql.column("role")])
    |> sql.distinct
    |> sql.to_query(a())

  assert q.sql == "SELECT DISTINCT department, role FROM employees"
}

pub fn select_distinct_to_string_test() {
  let s =
    sql.from(sql.table("readings"))
    |> sql.select([sql.column("value")])
    |> sql.distinct
    |> sql.to_string(a())

  assert s == "SELECT DISTINCT value FROM readings"
}

pub fn count_column_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.count("*")])
    |> sql.to_query(a())

  assert q.sql == "SELECT COUNT(*) FROM users"
}

pub fn count_named_column_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.count("id")])
    |> sql.to_query(a())

  assert q.sql == "SELECT COUNT(id) FROM users"
}

pub fn sum_column_test() {
  let q =
    sql.from(sql.table("orders"))
    |> sql.select([sql.sum("amount")])
    |> sql.to_query(a())

  assert q.sql == "SELECT SUM(amount) FROM orders"
}

pub fn avg_column_test() {
  let q =
    sql.from(sql.table("employees"))
    |> sql.select([sql.avg("salary")])
    |> sql.to_query(a())

  assert q.sql == "SELECT AVG(salary) FROM employees"
}

pub fn max_column_test() {
  let q =
    sql.from(sql.table("products"))
    |> sql.select([sql.max("price")])
    |> sql.to_query(a())

  assert q.sql == "SELECT MAX(price) FROM products"
}

pub fn min_column_test() {
  let q =
    sql.from(sql.table("products"))
    |> sql.select([sql.min("price")])
    |> sql.to_query(a())

  assert q.sql == "SELECT MIN(price) FROM products"
}

pub fn aggregate_with_alias_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.count("*") |> sql.column_as("total")])
    |> sql.to_query(a())

  assert q.sql == "SELECT COUNT(*) AS total FROM users"
}

pub fn aggregate_with_table_test() {
  let orders = sql.table("orders") |> sql.table_as("o")

  let q =
    sql.from(orders)
    |> sql.select([sql.sum("amount") |> sql.column_for(orders)])
    |> sql.to_query(a())

  assert q.sql == "SELECT SUM(o.amount) FROM orders AS o"
}

pub fn multiple_aggregates_test() {
  let q =
    sql.from(sql.table("employees"))
    |> sql.select([
      sql.column("department"),
      sql.count("*") |> sql.column_as("cnt"),
      sql.avg("salary") |> sql.column_as("avg_salary"),
    ])
    |> sql.group_by([sql.column("department")])
    |> sql.to_query(a())

  assert q.sql
    == "SELECT department, COUNT(*) AS cnt, AVG(salary) AS avg_salary FROM employees GROUP BY department"
}

pub fn having_test() {
  let q =
    sql.from(sql.table("employees"))
    |> sql.select([
      sql.column("department"),
      sql.count("*") |> sql.column_as("cnt"),
    ])
    |> sql.group_by([sql.column("department")])
    |> sql.having([sql.count("*") |> sql.gt(sql.int(5), of: sql.val)])
    |> sql.to_query(a())

  assert q.sql
    == "SELECT department, COUNT(*) AS cnt FROM employees GROUP BY department HAVING COUNT(*) > $1"
  assert q.values == [sql.int(5)]
}

pub fn having_multiple_test() {
  let q =
    sql.from(sql.table("employees"))
    |> sql.select([sql.column("department")])
    |> sql.group_by([sql.column("department")])
    |> sql.having([
      sql.count("*") |> sql.gt(sql.int(5), of: sql.val),
      sql.avg("salary") |> sql.gt(sql.float(50_000.0), of: sql.val),
    ])
    |> sql.to_query(a())

  assert q.sql
    == "SELECT department FROM employees GROUP BY department HAVING (COUNT(*) > $1 AND AVG(salary) > $2)"
  assert q.values == [sql.int(5), sql.float(50_000.0)]
}

pub fn having_to_string_test() {
  let s =
    sql.from(sql.table("employees"))
    |> sql.select([
      sql.column("department"),
      sql.count("*") |> sql.column_as("cnt"),
    ])
    |> sql.group_by([sql.column("department")])
    |> sql.having([sql.count("*") |> sql.gt(sql.int(5), of: sql.val)])
    |> sql.to_string(a())

  assert s
    == "SELECT department, COUNT(*) AS cnt FROM employees GROUP BY department HAVING COUNT(*) > 5"
}

pub fn insert_returning_test() {
  let inserter =
    sql.rows(["Alice"])
    |> sql.value("name", fn(name) { sql.text(name) })
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values(inserter)
    |> sql.returning([sql.column("id"), sql.column("name")])
    |> sql.to_query(a())

  assert q.sql == "INSERT INTO users (name) VALUES ($1) RETURNING id, name"
  assert q.values == [sql.text("Alice")]
}

pub fn update_returning_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("name", sql.text("Bob"), of: sql.val)
    |> sql.where([sql.column("id") |> sql.eq(sql.int(1), of: sql.val)])
    |> sql.returning([sql.column("id"), sql.column("name")])
    |> sql.to_query(a())

  assert q.sql == "UPDATE users SET name = $1 WHERE id = $2 RETURNING id, name"
  assert q.values == [sql.text("Bob"), sql.int(1)]
}

pub fn delete_returning_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.delete()
    |> sql.where([sql.column("id") |> sql.eq(sql.int(42), of: sql.val)])
    |> sql.returning([sql.column("id")])
    |> sql.to_query(a())

  assert q.sql == "DELETE FROM users WHERE id = $1 RETURNING id"
  assert q.values == [sql.int(42)]
}

pub fn returning_to_string_test() {
  let inserter =
    sql.rows(["Alice"])
    |> sql.value("name", fn(name) { sql.text(name) })
  let s =
    sql.insert(into: sql.table("users"))
    |> sql.values(inserter)
    |> sql.returning([sql.column("id")])
    |> sql.to_string(a())

  assert s == "INSERT INTO users (name) VALUES ('Alice') RETURNING id"
}

pub fn on_conflict_do_nothing_test() {
  let inserter =
    sql.rows([#(1, "Alice")])
    |> sql.value("id", fn(r) { sql.int(r.0) })
    |> sql.value("name", fn(r) { sql.text(r.1) })
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values(inserter)
    |> sql.on_conflict(target: "id", action: sql.DoNothing, where: [])
    |> sql.to_query(a())

  assert q.sql
    == "INSERT INTO users (id, name) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING"
  assert q.values == [sql.int(1), sql.text("Alice")]
}

pub fn on_conflict_do_update_test() {
  let inserter =
    sql.rows([#(1, 10)])
    |> sql.value("id", fn(r) { sql.int(r.0) })
    |> sql.value("quantity", fn(r) { sql.int(r.1) })
  let q =
    sql.insert(into: sql.table("counts"))
    |> sql.values(inserter)
    |> sql.on_conflict(
      target: "id",
      action: sql.DoUpdate(sets: [#("quantity", "excluded.quantity")]),
      where: [],
    )
    |> sql.to_query(a())

  assert q.sql
    == "INSERT INTO counts (id, quantity) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET quantity = excluded.quantity"
  assert q.values == [sql.int(1), sql.int(10)]
}

pub fn on_conflict_do_update_with_where_test() {
  let inserter =
    sql.rows([#(1, 10)])
    |> sql.value("id", fn(r) { sql.int(r.0) })
    |> sql.value("quantity", fn(r) { sql.int(r.1) })
  let q =
    sql.insert(into: sql.table("counts"))
    |> sql.values(inserter)
    |> sql.on_conflict(
      target: "id",
      action: sql.DoUpdate(sets: [#("quantity", "excluded.quantity")]),
      where: [sql.column("quantity") |> sql.gt(sql.int(5), of: sql.val)],
    )
    |> sql.to_query(a())

  assert q.sql
    == "INSERT INTO counts (id, quantity) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET quantity = excluded.quantity WHERE quantity > $3"
  assert q.values == [sql.int(1), sql.int(10), sql.int(5)]
}

pub fn on_conflict_do_nothing_returning_test() {
  let inserter =
    sql.rows([#(1, 10)])
    |> sql.value("id", fn(r) { sql.int(r.0) })
    |> sql.value("quantity", fn(r) { sql.int(r.1) })
  let q =
    sql.insert(into: sql.table("counts"))
    |> sql.values(inserter)
    |> sql.on_conflict(target: "id", action: sql.DoNothing, where: [])
    |> sql.returning([sql.column("id")])
    |> sql.to_query(a())

  assert q.sql
    == "INSERT INTO counts (id, quantity) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING RETURNING id"
  assert q.values == [sql.int(1), sql.int(10)]
}

pub fn on_conflict_to_string_test() {
  let inserter =
    sql.rows([#(1, 10)])
    |> sql.value("id", fn(r) { sql.int(r.0) })
    |> sql.value("quantity", fn(r) { sql.int(r.1) })
  let s =
    sql.insert(into: sql.table("counts"))
    |> sql.values(inserter)
    |> sql.on_conflict(
      target: "id",
      action: sql.DoUpdate(sets: [#("quantity", "excluded.quantity")]),
      where: [],
    )
    |> sql.to_string(a())

  assert s
    == "INSERT INTO counts (id, quantity) VALUES (1, 10) ON CONFLICT (id) DO UPDATE SET quantity = excluded.quantity"
}

pub fn on_conflict_where_to_string_test() {
  let inserter =
    sql.rows([#(1, 10)])
    |> sql.value("id", fn(r) { sql.int(r.0) })
    |> sql.value("quantity", fn(r) { sql.int(r.1) })
  let s =
    sql.insert(into: sql.table("counts"))
    |> sql.values(inserter)
    |> sql.on_conflict(
      target: "id",
      action: sql.DoUpdate(sets: [#("quantity", "excluded.quantity")]),
      where: [sql.column("quantity") |> sql.gt(sql.int(5), of: sql.val)],
    )
    |> sql.to_string(a())

  assert s
    == "INSERT INTO counts (id, quantity) VALUES (1, 10) ON CONFLICT (id) DO UPDATE SET quantity = excluded.quantity WHERE quantity > 5"
}

pub fn complex_query_with_aggregates_having_test() {
  let q =
    sql.from(sql.table("employees"))
    |> sql.select([
      sql.column("department"),
      sql.count("*") |> sql.column_as("emp_count"),
      sql.sum("salary") |> sql.column_as("total_salary"),
    ])
    |> sql.where([sql.column("active") |> sql.eq(sql.true, of: sql.val)])
    |> sql.group_by([sql.column("department")])
    |> sql.having([sql.count("*") |> sql.gt(sql.int(3), of: sql.val)])
    |> sql.order_by([sql.column("department") |> sql.asc])
    |> sql.limit(10)
    |> sql.to_query(a())

  assert q.sql
    == "SELECT department, COUNT(*) AS emp_count, SUM(salary) AS total_salary FROM employees WHERE active = $1 GROUP BY department HAVING COUNT(*) > $2 ORDER BY department ASC LIMIT 10"
  assert q.values == [sql.true, sql.int(3)]
}

pub fn union_basic_test() {
  let q =
    sql.union([
      sql.from(sql.table("contractors")) |> sql.select([sql.column("name")]),
      sql.from(sql.table("employees")) |> sql.select([sql.column("name")]),
    ])
    |> sql.to_query(a())

  assert q.sql
    == "SELECT name FROM employees UNION SELECT name FROM contractors"
  assert q.values == []
}

pub fn union_all_basic_test() {
  let q =
    sql.union_all([
      sql.from(sql.table("contractors")) |> sql.select([sql.column("name")]),
      sql.from(sql.table("employees")) |> sql.select([sql.column("name")]),
    ])
    |> sql.to_query(a())

  assert q.sql
    == "SELECT name FROM employees UNION ALL SELECT name FROM contractors"
  assert q.values == []
}

pub fn union_three_way_test() {
  let q =
    sql.union([
      sql.from(sql.table("interns")) |> sql.select([sql.column("name")]),
      sql.from(sql.table("contractors")) |> sql.select([sql.column("name")]),
      sql.from(sql.table("employees")) |> sql.select([sql.column("name")]),
    ])
    |> sql.to_query(a())

  assert q.sql
    == "SELECT name FROM employees UNION SELECT name FROM contractors UNION SELECT name FROM interns"
  assert q.values == []
}

pub fn union_with_where_sequential_placeholders_test() {
  let q =
    sql.union([
      sql.from(sql.table("contractors"))
        |> sql.select([sql.column("name")])
        |> sql.where([
          sql.column("department")
          |> sql.eq(sql.text("Engineering"), of: sql.val),
        ]),
      sql.from(sql.table("employees"))
        |> sql.select([sql.column("name")])
        |> sql.where([
          sql.column("department")
          |> sql.eq(sql.text("Engineering"), of: sql.val),
        ]),
    ])
    |> sql.to_query(a())

  assert q.sql
    == "SELECT name FROM employees WHERE department = $1 UNION SELECT name FROM contractors WHERE department = $2"
  assert q.values == [sql.text("Engineering"), sql.text("Engineering")]
}

pub fn union_multi_params_sequential_test() {
  let q =
    sql.union([
      sql.from(sql.table("contractors"))
        |> sql.select([sql.column("name")])
        |> sql.where([
          sql.column("department") |> sql.eq(sql.text("Sales"), of: sql.val),
        ]),
      sql.from(sql.table("employees"))
        |> sql.select([sql.column("name")])
        |> sql.where([
          sql.column("department")
            |> sql.eq(sql.text("Engineering"), of: sql.val),
          sql.column("salary") |> sql.gt(sql.int(50_000), of: sql.val),
        ]),
    ])
    |> sql.to_query(a())

  assert q.sql
    == "SELECT name FROM employees WHERE (department = $1 AND salary > $2) UNION SELECT name FROM contractors WHERE department = $3"
  assert q.values
    == [sql.text("Engineering"), sql.int(50_000), sql.text("Sales")]
}

pub fn union_three_way_params_test() {
  let q =
    sql.union([
      sql.from(sql.table("c"))
        |> sql.select([sql.star])
        |> sql.where([sql.column("x") |> sql.eq(sql.int(4), of: sql.val)]),
      sql.from(sql.table("b"))
        |> sql.select([sql.star])
        |> sql.where([
          sql.column("x") |> sql.eq(sql.int(2), of: sql.val),
          sql.column("y") |> sql.eq(sql.int(3), of: sql.val),
        ]),
      sql.from(sql.table("a"))
        |> sql.select([sql.star])
        |> sql.where([sql.column("x") |> sql.eq(sql.int(1), of: sql.val)]),
    ])
    |> sql.to_query(a())

  assert q.sql
    == "SELECT * FROM a WHERE x = $1 UNION SELECT * FROM b WHERE (x = $2 AND y = $3) UNION SELECT * FROM c WHERE x = $4"
  assert q.values == [sql.int(1), sql.int(2), sql.int(3), sql.int(4)]
}

pub fn union_to_string_test() {
  let s =
    sql.union([
      sql.from(sql.table("contractors"))
        |> sql.select([sql.column("name")])
        |> sql.where([
          sql.column("department")
          |> sql.eq(sql.text("Engineering"), of: sql.val),
        ]),
      sql.from(sql.table("employees"))
        |> sql.select([sql.column("name")])
        |> sql.where([
          sql.column("department")
          |> sql.eq(sql.text("Engineering"), of: sql.val),
        ]),
    ])
    |> sql.to_string(a())

  assert s
    == "SELECT name FROM employees WHERE department = 'Engineering' UNION SELECT name FROM contractors WHERE department = 'Engineering'"
}

pub fn union_all_to_string_test() {
  let s =
    sql.union_all([
      sql.from(sql.table("contractors")) |> sql.select([sql.column("name")]),
      sql.from(sql.table("employees")) |> sql.select([sql.column("name")]),
    ])
    |> sql.to_string(a())

  assert s
    == "SELECT name FROM employees UNION ALL SELECT name FROM contractors"
}

pub fn union_three_way_to_string_test() {
  let s =
    sql.union([
      sql.from(sql.table("c")) |> sql.select([sql.column("name")]),
      sql.from(sql.table("b")) |> sql.select([sql.column("name")]),
      sql.from(sql.table("a")) |> sql.select([sql.column("name")]),
    ])
    |> sql.to_string(a())

  assert s
    == "SELECT name FROM a UNION SELECT name FROM b UNION SELECT name FROM c"
}

pub fn cte_basic_to_query_test() {
  let active_users =
    sql.cte(
      name: "active_users",
      query: sql.from(sql.table("users"))
        |> sql.select([sql.column("id"), sql.column("name")])
        |> sql.where([sql.column("active") |> sql.eq(sql.true, of: sql.val)]),
    )

  let q =
    sql.from(sql.table("active_users"))
    |> sql.select([sql.column("name")])
    |> sql.with(ctes: [active_users])
    |> sql.to_query(a())

  assert q.sql
    == "WITH active_users AS (SELECT id, name FROM users WHERE active = $1) SELECT name FROM active_users;"
  assert q.values == [sql.Bool(True)]
}

pub fn cte_basic_to_string_test() {
  let active_users =
    sql.cte(
      name: "active_users",
      query: sql.from(sql.table("users"))
        |> sql.select([sql.column("id"), sql.column("name")])
        |> sql.where([sql.column("active") |> sql.eq(sql.true, of: sql.val)]),
    )

  let s =
    sql.from(sql.table("active_users"))
    |> sql.select([sql.column("name")])
    |> sql.with(ctes: [active_users])
    |> sql.to_string(a())

  assert s
    == "WITH active_users AS (SELECT id, name FROM users WHERE active = TRUE) SELECT name FROM active_users;"
}

pub fn cte_multiple_test() {
  let active_users = sql.table("active_users")
  let recent_orders = sql.table("recent_orders")

  let active_users_cte =
    sql.cte(
      name: "active_users",
      query: sql.from(sql.table("users"))
        |> sql.select([sql.column("id")])
        |> sql.where([sql.column("active") |> sql.eq(sql.true, of: sql.val)]),
    )

  let recent_orders_cte =
    sql.cte(
      name: "recent_orders",
      query: sql.from(sql.table("orders"))
        |> sql.select([sql.column("user_id"), sql.column("total")])
        |> sql.where([
          sql.column("total") |> sql.gt(sql.float(100.0), of: sql.val),
        ]),
    )

  let q =
    sql.from(sql.table("active_users"))
    |> sql.select([sql.column("id"), sql.column("total")])
    |> sql.inner_join(table: sql.table("recent_orders"), on: [
      sql.column("id")
      |> sql.column_for(active_users)
      |> sql.eq(
        sql.column("user_id") |> sql.column_for(recent_orders),
        of: sql.col,
      ),
    ])
    |> sql.with(ctes: [active_users_cte, recent_orders_cte])
    |> sql.to_query(a())

  assert q.sql
    == "WITH active_users AS (SELECT id FROM users WHERE active = $1), recent_orders AS (SELECT user_id, total FROM orders WHERE total > $2) SELECT id, total FROM active_users INNER JOIN recent_orders ON active_users.id = recent_orders.user_id;"
  assert q.values == [sql.Bool(True), sql.Float(100.0)]
}

pub fn cte_with_column_aliases_test() {
  let totals =
    sql.cte(
      name: "totals",
      query: sql.from(sql.table("orders"))
        |> sql.select([sql.column("user_id"), sql.column("amount")]),
    )
    |> sql.cte_columns(columns: ["uid", "total"])

  let s =
    sql.from(sql.table("totals"))
    |> sql.select([sql.column("uid"), sql.column("total")])
    |> sql.with(ctes: [totals])
    |> sql.to_string(a())

  assert s
    == "WITH totals(uid, total) AS (SELECT user_id, amount FROM orders) SELECT uid, total FROM totals;"
}

pub fn cte_recursive_test() {
  let base =
    sql.from(sql.table("categories"))
    |> sql.select([
      sql.column("id"),
      sql.column("parent_id"),
      sql.column("name"),
    ])
    |> sql.where([sql.column("parent_id") |> sql.is_null])

  let category_tree = sql.cte(name: "category_tree", query: base)

  let s =
    sql.from(sql.table("category_tree"))
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.with(ctes: [category_tree])
    |> sql.recursive()
    |> sql.to_string(a())

  assert s
    == "WITH RECURSIVE category_tree AS (SELECT id, parent_id, name FROM categories WHERE parent_id IS NULL) SELECT id, name FROM category_tree;"
}

pub fn cte_with_insert_test() {
  let new_users =
    sql.cte(
      name: "new_users",
      query: sql.from(sql.table("users"))
        |> sql.select([sql.column("id")])
        |> sql.where([
          sql.column("status") |> sql.eq(sql.text("new"), of: sql.val),
        ]),
    )

  let inserter =
    sql.rows([#(1, "Welcome!")])
    |> sql.value("user_id", fn(r) { sql.int(r.0) })
    |> sql.value("message", fn(r) { sql.text(r.1) })
  let q =
    sql.insert(into: sql.table("notifications"))
    |> sql.values(inserter)
    |> sql.with(ctes: [new_users])
    |> sql.to_query(a())

  assert q.sql
    == "WITH new_users AS (SELECT id FROM users WHERE status = $1) INSERT INTO notifications (user_id, message) VALUES ($2, $3);"
  assert q.values == [sql.Text("new"), sql.Int(1), sql.Text("Welcome!")]
}

pub fn cte_with_update_test() {
  let target_users =
    sql.cte(
      name: "target_users",
      query: sql.from(sql.table("users"))
        |> sql.select([sql.column("id")])
        |> sql.where([sql.column("score") |> sql.lt(sql.int(10), of: sql.val)]),
    )

  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("status", sql.text("inactive"), of: sql.val)
    |> sql.where([
      sql.column("id") |> sql.in([sql.int(1), sql.int(2)], of: sql.val),
    ])
    |> sql.with(ctes: [target_users])
    |> sql.to_query(a())

  assert q.sql
    == "WITH target_users AS (SELECT id FROM users WHERE score < $1) UPDATE users SET status = $2 WHERE id IN ($3, $4);"
  assert q.values
    == [
      sql.Int(10),
      sql.Text("inactive"),
      sql.Int(1),
      sql.Int(2),
    ]
}

pub fn cte_with_delete_test() {
  let old_orders =
    sql.cte(
      name: "old_orders",
      query: sql.from(sql.table("orders"))
        |> sql.select([sql.column("id")])
        |> sql.where([sql.column("year") |> sql.lt(sql.int(2020), of: sql.val)]),
    )

  let q =
    sql.from(sql.table("order_items"))
    |> sql.delete()
    |> sql.where([
      sql.column("order_id")
      |> sql.in(
        [
          sql.int(100),
          sql.int(200),
        ],
        of: sql.val,
      ),
    ])
    |> sql.with(ctes: [old_orders])
    |> sql.to_query(a())

  assert q.sql
    == "WITH old_orders AS (SELECT id FROM orders WHERE year < $1) DELETE FROM order_items WHERE order_id IN ($2, $3);"
  assert q.values == [sql.Int(2020), sql.Int(100), sql.Int(200)]
}

pub fn cte_placeholder_threading_test() {
  let cte1 =
    sql.cte(
      name: "cte1",
      query: sql.from(sql.table("t1"))
        |> sql.select([sql.column("id")])
        |> sql.where([sql.column("a") |> sql.eq(sql.int(1), of: sql.val)]),
    )

  let cte2 =
    sql.cte(
      name: "cte2",
      query: sql.from(sql.table("t2"))
        |> sql.select([sql.column("id")])
        |> sql.where([sql.column("b") |> sql.eq(sql.int(2), of: sql.val)]),
    )

  let q =
    sql.from(sql.table("cte1"))
    |> sql.select([sql.column("id")])
    |> sql.where([sql.column("c") |> sql.eq(sql.int(3), of: sql.val)])
    |> sql.with(ctes: [cte1, cte2])
    |> sql.to_query(a())

  // CTE1 body uses $1, CTE2 body uses $2, main query uses $3
  assert q.sql
    == "WITH cte1 AS (SELECT id FROM t1 WHERE a = $1), cte2 AS (SELECT id FROM t2 WHERE b = $2) SELECT id FROM cte1 WHERE c = $3;"
  assert q.values == [sql.Int(1), sql.Int(2), sql.Int(3)]
}

pub fn for_update_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where([sql.column("id") |> sql.eq(sql.int(1), of: sql.val)])
    |> sql.for_update
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE id = $1 FOR UPDATE"
  assert q.values == [sql.Int(1)]
}

pub fn for_update_with_order_by_and_limit_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.where([sql.column("age") |> sql.gt(sql.int(18), of: sql.val)])
    |> sql.order_by([sql.column("name") |> sql.asc])
    |> sql.limit(10)
    |> sql.for_update
    |> sql.to_query(a())

  assert q.sql
    == "SELECT id, name FROM users WHERE age > $1 ORDER BY name ASC LIMIT 10 FOR UPDATE"
  assert q.values == [sql.Int(18)]
}

pub fn for_update_with_join_test() {
  let users = sql.table("users") |> sql.table_as("u")
  let orders = sql.table("orders") |> sql.table_as("o")

  let q =
    sql.from(sql.table("users") |> sql.table_as("u"))
    |> sql.select([
      sql.column("id") |> sql.column_for(users),
      sql.column("total") |> sql.column_for(orders),
    ])
    |> sql.inner_join(table: sql.table("orders") |> sql.table_as("o"), on: [
      sql.column("id")
      |> sql.column_for(users)
      |> sql.eq(sql.column("user_id") |> sql.column_for(orders), of: sql.col),
    ])
    |> sql.where([
      sql.column("id")
      |> sql.column_for(users)
      |> sql.eq(sql.int(1), of: sql.val),
    ])
    |> sql.for_update
    |> sql.to_query(a())

  assert q.sql
    == "SELECT u.id, o.total FROM users AS u INNER JOIN orders AS o ON u.id = o.user_id WHERE u.id = $1 FOR UPDATE"
  assert q.values == [sql.Int(1)]
}

pub fn for_update_to_string_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where([sql.column("id") |> sql.eq(sql.int(42), of: sql.val)])
    |> sql.for_update
    |> sql.to_string(a())

  assert q == "SELECT * FROM users WHERE id = 42 FOR UPDATE"
}

pub fn backtick_select_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.where([sql.column("active") |> sql.eq(sql.true, of: sql.val)])
    |> sql.to_query(backtick_a())

  assert q.sql == "SELECT `id`, `name` FROM `users` WHERE `active` = ?"
  assert q.values == [sql.Bool(True)]
}

pub fn backtick_select_multiple_wheres_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.where([
      sql.column("active") |> sql.eq(sql.true, of: sql.val),
      sql.column("age") |> sql.gt(sql.int(18), of: sql.val),
    ])
    |> sql.to_query(backtick_a())

  assert q.sql
    == "SELECT `id`, `name` FROM `users` WHERE (`active` = ? AND `age` > ?)"
  assert q.values == [sql.Bool(True), sql.Int(18)]
}

pub fn backtick_insert_test() {
  let inserter =
    sql.rows([#("Alice", "alice@example.com")])
    |> sql.value("name", fn(r) { sql.text(r.0) })
    |> sql.value("email", fn(r) { sql.text(r.1) })
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values(inserter)
    |> sql.to_query(backtick_a())

  assert q.sql == "INSERT INTO `users` (`name`, `email`) VALUES (?, ?)"
  assert q.values == [sql.Text("Alice"), sql.Text("alice@example.com")]
}

pub fn backtick_update_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("name", sql.text("Bob"), of: sql.val)
    |> sql.where([sql.column("id") |> sql.eq(sql.int(1), of: sql.val)])
    |> sql.to_query(backtick_a())

  assert q.sql == "UPDATE `users` SET `name` = ? WHERE `id` = ?"
  assert q.values == [sql.Text("Bob"), sql.Int(1)]
}

pub fn backtick_delete_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.delete()
    |> sql.where([sql.column("id") |> sql.eq(sql.int(1), of: sql.val)])
    |> sql.to_query(backtick_a())

  assert q.sql == "DELETE FROM `users` WHERE `id` = ?"
  assert q.values == [sql.Int(1)]
}

pub fn backtick_union_test() {
  let q1 =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id")])
    |> sql.where([sql.column("active") |> sql.eq(sql.true, of: sql.val)])

  let q2 =
    sql.from(sql.table("admins"))
    |> sql.select([sql.column("id")])
    |> sql.where([sql.column("active") |> sql.eq(sql.true, of: sql.val)])

  let q =
    sql.union([q2, q1])
    |> sql.to_query(backtick_a())

  assert q.sql
    == "SELECT `id` FROM `users` WHERE `active` = ? UNION SELECT `id` FROM `admins` WHERE `active` = ?"
  assert q.values == [sql.Bool(True), sql.Bool(True)]
}

pub fn backtick_to_string_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.where([sql.column("active") |> sql.eq(sql.true, of: sql.val)])
    |> sql.to_string(backtick_a())

  assert q == "SELECT `id`, `name` FROM `users` WHERE `active` = TRUE"
}

pub fn backtick_aliased_identifiers_test() {
  let users = sql.table("users") |> sql.table_as("u")
  let orders = sql.table("orders") |> sql.table_as("o")

  let q =
    sql.from(sql.table("users") |> sql.table_as("u"))
    |> sql.select([
      sql.column("name") |> sql.column_for(users),
      sql.column("total")
        |> sql.column_for(orders)
        |> sql.column_as("order_total"),
    ])
    |> sql.inner_join(table: sql.table("orders") |> sql.table_as("o"), on: [
      sql.column("id")
      |> sql.column_for(users)
      |> sql.eq(sql.column("user_id") |> sql.column_for(orders), of: sql.col),
    ])
    |> sql.where([
      sql.column("active")
      |> sql.column_for(users)
      |> sql.eq(sql.true, of: sql.val),
    ])
    |> sql.to_query(backtick_a())

  assert q.sql
    == "SELECT `u`.`name`, `o`.`total` AS `order_total` FROM `users` AS `u` INNER JOIN `orders` AS `o` ON `u`.`id` = `o`.`user_id` WHERE `u`.`active` = ?"
  assert q.values == [sql.Bool(True)]
}

pub fn select_not_like_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.where([
      sql.column("name") |> sql.not_like(sql.text("%admin%"), of: sql.val),
    ])
    |> sql.to_query(a())

  assert q.sql == "SELECT id, name FROM users WHERE name NOT LIKE $1"
  assert q.values == [sql.Text("%admin%")]
}

pub fn select_not_like_to_string_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.where([
      sql.column("name") |> sql.not_like(sql.text("%admin%"), of: sql.val),
    ])
    |> sql.to_string(a())

  assert q == "SELECT id, name FROM users WHERE name NOT LIKE '%admin%'"
}

pub fn select_not_between_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id"), sql.column("age")])
    |> sql.where([
      sql.not(
        sql.column("age")
        |> sql.between(sql.int(18), sql.int(65), of: sql.val),
      ),
    ])
    |> sql.to_query(a())

  assert q.sql == "SELECT id, age FROM users WHERE NOT (age BETWEEN $1 AND $2)"
  assert q.values == [sql.Int(18), sql.Int(65)]
}

pub fn select_not_between_to_string_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id"), sql.column("age")])
    |> sql.where([
      sql.not(
        sql.column("age")
        |> sql.between(sql.int(18), sql.int(65), of: sql.val),
      ),
    ])
    |> sql.to_string(a())

  assert q == "SELECT id, age FROM users WHERE NOT (age BETWEEN 18 AND 65)"
}

pub fn select_right_join_test() {
  let users = sql.table("users") |> sql.table_as("u")
  let orders = sql.table("orders") |> sql.table_as("o")

  let q =
    sql.from(sql.table("users") |> sql.table_as("u"))
    |> sql.select([
      sql.column("id") |> sql.column_for(users),
      sql.column("order_id") |> sql.column_for(orders),
    ])
    |> sql.right_join(table: sql.table("orders") |> sql.table_as("o"), on: [
      sql.column("id")
      |> sql.column_for(users)
      |> sql.eq(sql.column("user_id") |> sql.column_for(orders), of: sql.col),
    ])
    |> sql.to_query(a())

  assert q.sql
    == "SELECT u.id, o.order_id FROM users AS u RIGHT JOIN orders AS o ON u.id = o.user_id"
  assert q.values == []
}

pub fn select_full_join_test() {
  let users = sql.table("users") |> sql.table_as("u")
  let orders = sql.table("orders") |> sql.table_as("o")

  let q =
    sql.from(sql.table("users") |> sql.table_as("u"))
    |> sql.select([
      sql.column("id") |> sql.column_for(users),
      sql.column("order_id") |> sql.column_for(orders),
    ])
    |> sql.full_join(table: sql.table("orders") |> sql.table_as("o"), on: [
      sql.column("id")
      |> sql.column_for(users)
      |> sql.eq(sql.column("user_id") |> sql.column_for(orders), of: sql.col),
    ])
    |> sql.to_query(a())

  assert q.sql
    == "SELECT u.id, o.order_id FROM users AS u FULL JOIN orders AS o ON u.id = o.user_id"
  assert q.values == []
}

pub fn select_multiple_joins_test() {
  let users = sql.table("users") |> sql.table_as("u")
  let orders = sql.table("orders") |> sql.table_as("o")
  let products = sql.table("products") |> sql.table_as("p")

  let q =
    sql.from(users)
    |> sql.select([
      sql.column("id") |> sql.column_for(users),
      sql.column("order_id") |> sql.column_for(orders),
      sql.column("product_name") |> sql.column_for(products),
    ])
    |> sql.inner_join(table: orders, on: [
      sql.column("id")
      |> sql.column_for(users)
      |> sql.eq(sql.column("user_id") |> sql.column_for(orders), of: sql.col),
    ])
    |> sql.left_join(table: products, on: [
      sql.column("product_id")
      |> sql.column_for(orders)
      |> sql.eq(sql.column("id") |> sql.column_for(products), of: sql.col),
    ])
    |> sql.to_query(a())

  assert q.sql
    == "SELECT u.id, o.order_id, p.product_name FROM users AS u INNER JOIN orders AS o ON u.id = o.user_id LEFT JOIN products AS p ON o.product_id = p.id"
  assert q.values == []
}

pub fn select_join_with_and_conditions_test() {
  let users = sql.table("users") |> sql.table_as("u")
  let orders = sql.table("orders") |> sql.table_as("o")

  let q =
    sql.from(users)
    |> sql.select([sql.column("id") |> sql.column_for(users)])
    |> sql.inner_join(table: orders, on: [
      sql.and(
        sql.column("id")
          |> sql.column_for(users)
          |> sql.eq(
            sql.column("user_id") |> sql.column_for(orders),
            of: sql.col,
          ),
        sql.column("status")
          |> sql.column_for(orders)
          |> sql.eq(sql.text("active"), of: sql.val),
      ),
    ])
    |> sql.to_query(a())

  assert q.sql
    == "SELECT u.id FROM users AS u INNER JOIN orders AS o ON (u.id = o.user_id AND o.status = $1)"
  assert q.values == [sql.Text("active")]
}

pub fn select_join_with_multiple_on_conditions_test() {
  let users = sql.table("users") |> sql.table_as("u")
  let orders = sql.table("orders") |> sql.table_as("o")

  let q =
    sql.from(users)
    |> sql.select([sql.column("id") |> sql.column_for(users)])
    |> sql.inner_join(table: orders, on: [
      sql.column("id")
        |> sql.column_for(users)
        |> sql.eq(sql.column("user_id") |> sql.column_for(orders), of: sql.col),
      sql.column("status")
        |> sql.column_for(orders)
        |> sql.eq(sql.text("active"), of: sql.val),
    ])
    |> sql.to_query(a())

  assert q.sql
    == "SELECT u.id FROM users AS u INNER JOIN orders AS o ON (u.id = o.user_id AND o.status = $1)"
  assert q.values == [sql.Text("active")]
}

pub fn select_join_with_three_on_conditions_test() {
  let users = sql.table("users") |> sql.table_as("u")
  let orders = sql.table("orders") |> sql.table_as("o")

  let q =
    sql.from(users)
    |> sql.select([sql.column("id") |> sql.column_for(users)])
    |> sql.inner_join(table: orders, on: [
      sql.column("id")
        |> sql.column_for(users)
        |> sql.eq(sql.column("user_id") |> sql.column_for(orders), of: sql.col),
      sql.column("status")
        |> sql.column_for(orders)
        |> sql.eq(sql.text("active"), of: sql.val),
      sql.column("total")
        |> sql.column_for(orders)
        |> sql.gt(sql.float(100.0), of: sql.val),
    ])
    |> sql.to_query(a())

  assert q.sql
    == "SELECT u.id FROM users AS u INNER JOIN orders AS o ON ((u.id = o.user_id AND o.status = $1) AND o.total > $2)"
  assert q.values == [sql.Text("active"), sql.Float(100.0)]
}

pub fn select_offset_without_limit_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.offset(10)
    |> sql.to_query(a())

  assert q.sql == "SELECT id, name FROM users OFFSET 10"
  assert q.values == []
}

pub fn select_where_gt_lt_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.where([
      sql.column("age") |> sql.gt(sql.int(18), of: sql.val),
      sql.column("age") |> sql.lt(sql.int(65), of: sql.val),
    ])
    |> sql.to_query(a())

  assert q.sql == "SELECT id, name FROM users WHERE (age > $1 AND age < $2)"
  assert q.values == [sql.Int(18), sql.Int(65)]
}

pub fn select_where_gt_eq_lt_eq_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id")])
    |> sql.where([
      sql.column("age") |> sql.gt_eq(sql.int(18), of: sql.val),
      sql.column("age") |> sql.lt_eq(sql.int(65), of: sql.val),
    ])
    |> sql.to_query(a())

  assert q.sql == "SELECT id FROM users WHERE (age >= $1 AND age <= $2)"
  assert q.values == [sql.Int(18), sql.Int(65)]
}

pub fn select_where_not_eq_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.where([
      sql.column("status") |> sql.not_eq(sql.text("banned"), of: sql.val),
    ])
    |> sql.to_query(a())

  assert q.sql == "SELECT id, name FROM users WHERE status != $1"
  assert q.values == [sql.Text("banned")]
}

pub fn select_complex_between_with_conditions_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id"), sql.column("name"), sql.column("age")])
    |> sql.where([
      sql.column("age") |> sql.between(sql.int(18), sql.int(65), of: sql.val),
      sql.column("active") |> sql.eq(sql.true, of: sql.val),
    ])
    |> sql.to_query(a())

  assert q.sql
    == "SELECT id, name, age FROM users WHERE (age BETWEEN $1 AND $2 AND active = $3)"
  assert q.values == [sql.Int(18), sql.Int(65), sql.Bool(True)]
}

pub fn select_chained_three_wheres_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id")])
    |> sql.where([
      sql.column("a") |> sql.eq(sql.int(1), of: sql.val),
      sql.column("b") |> sql.eq(sql.int(2), of: sql.val),
      sql.column("c") |> sql.eq(sql.int(3), of: sql.val),
    ])
    |> sql.to_query(a())

  assert q.sql == "SELECT id FROM users WHERE ((a = $1 AND b = $2) AND c = $3)"
  assert q.values == [sql.Int(1), sql.Int(2), sql.Int(3)]
}

pub fn select_where_not_eq_to_string_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.where([
      sql.column("status") |> sql.not_eq(sql.text("banned"), of: sql.val),
    ])
    |> sql.to_string(a())

  assert q == "SELECT id, name FROM users WHERE status != 'banned'"
}

pub fn select_date_to_string_test() {
  let d = calendar.Date(2024, calendar.January, 15)
  let q =
    sql.from(sql.table("events"))
    |> sql.select([sql.column("id")])
    |> sql.where([sql.column("event_date") |> sql.eq(sql.date(d), of: sql.val)])
    |> sql.to_string(sql.adapter())

  assert q == "SELECT id FROM events WHERE event_date = '2024-01-15'"
}

pub fn select_time_to_string_test() {
  let t = calendar.TimeOfDay(14, 30, 0, 0)
  let q =
    sql.from(sql.table("events"))
    |> sql.select([sql.column("id")])
    |> sql.where([sql.column("event_time") |> sql.eq(sql.time(t), of: sql.val)])
    |> sql.to_string(sql.adapter())

  assert q == "SELECT id FROM events WHERE event_time = '14:30:00'"
}

pub fn select_datetime_to_string_test() {
  let d = calendar.Date(2024, calendar.January, 15)
  let t = calendar.TimeOfDay(14, 30, 0, 0)
  let q =
    sql.from(sql.table("events"))
    |> sql.select([sql.column("id")])
    |> sql.where([
      sql.column("event_at") |> sql.eq(sql.datetime(d, t), of: sql.val),
    ])
    |> sql.to_string(sql.adapter())

  assert q == "SELECT id FROM events WHERE event_at = '2024-01-15 14:30:00'"
}

pub fn select_like_to_string_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.where([
      sql.column("name") |> sql.like(sql.text("%alice%"), of: sql.val),
    ])
    |> sql.to_string(a())

  assert q == "SELECT id, name FROM users WHERE name LIKE '%alice%'"
}

pub fn select_in_to_string_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.where([
      sql.column("id")
      |> sql.in(
        [
          sql.int(1),
          sql.int(2),
          sql.int(3),
        ],
        of: sql.val,
      ),
    ])
    |> sql.to_string(a())

  assert q == "SELECT id, name FROM users WHERE id IN (1, 2, 3)"
}

pub fn select_is_null_to_string_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.where([sql.column("deleted_at") |> sql.is_null])
    |> sql.to_string(a())

  assert q == "SELECT id, name FROM users WHERE deleted_at IS NULL"
}

pub fn select_is_not_null_to_string_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.where([sql.column("email") |> sql.is_not_null])
    |> sql.to_string(a())

  assert q == "SELECT id, name FROM users WHERE email IS NOT NULL"
}

pub fn select_between_to_string_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id"), sql.column("age")])
    |> sql.where([
      sql.column("age") |> sql.between(sql.int(18), sql.int(65), of: sql.val),
    ])
    |> sql.to_string(a())

  assert q == "SELECT id, age FROM users WHERE age BETWEEN 18 AND 65"
}

pub fn select_or_where_to_string_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id")])
    |> sql.where([
      sql.or(
        sql.column("active") |> sql.eq(sql.true, of: sql.val),
        sql.column("role") |> sql.eq(sql.text("admin"), of: sql.val),
      ),
    ])
    |> sql.to_string(a())

  assert q == "SELECT id FROM users WHERE (active = TRUE OR role = 'admin')"
}

pub fn select_join_to_string_test() {
  let users = sql.table("users") |> sql.table_as("u")
  let orders = sql.table("orders") |> sql.table_as("o")

  let q =
    sql.from(users)
    |> sql.select([
      sql.column("id") |> sql.column_for(users),
      sql.column("total") |> sql.column_for(orders),
    ])
    |> sql.inner_join(table: orders, on: [
      sql.column("id")
      |> sql.column_for(users)
      |> sql.eq(sql.column("user_id") |> sql.column_for(orders), of: sql.col),
    ])
    |> sql.to_string(a())

  assert q
    == "SELECT u.id, o.total FROM users AS u INNER JOIN orders AS o ON u.id = o.user_id"
}

pub fn select_order_by_to_string_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.order_by([
      sql.column("name") |> sql.asc,
      sql.column("id") |> sql.desc,
    ])
    |> sql.to_string(a())

  assert q == "SELECT id, name FROM users ORDER BY name ASC, id DESC"
}

pub fn select_limit_offset_to_string_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id")])
    |> sql.limit(10)
    |> sql.offset(20)
    |> sql.to_string(a())

  assert q == "SELECT id FROM users LIMIT 10 OFFSET 20"
}

pub fn select_where_and_or_combined_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id")])
    |> sql.where([
      sql.or(
        sql.column("role") |> sql.eq(sql.text("admin"), of: sql.val),
        sql.and(
          sql.column("active") |> sql.eq(sql.true, of: sql.val),
          sql.column("age") |> sql.gt(sql.int(18), of: sql.val),
        ),
      ),
    ])
    |> sql.to_query(a())

  assert q.sql
    == "SELECT id FROM users WHERE (role = $1 OR (active = $2 AND age > $3))"
  assert q.values == [sql.Text("admin"), sql.Bool(True), sql.Int(18)]
}

pub fn select_where_not_is_null_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id")])
    |> sql.where([sql.not(sql.column("email") |> sql.is_null)])
    |> sql.to_query(a())

  assert q.sql == "SELECT id FROM users WHERE NOT (email IS NULL)"
  assert q.values == []
}

pub fn select_multiple_joins_right_full_test() {
  let users = sql.table("users") |> sql.table_as("u")
  let orders = sql.table("orders") |> sql.table_as("o")
  let products = sql.table("products") |> sql.table_as("p")

  let q =
    sql.from(users)
    |> sql.select([
      sql.column("id") |> sql.column_for(users),
      sql.column("oid") |> sql.column_for(orders),
      sql.column("pid") |> sql.column_for(products),
    ])
    |> sql.right_join(table: orders, on: [
      sql.column("id")
      |> sql.column_for(users)
      |> sql.eq(sql.column("user_id") |> sql.column_for(orders), of: sql.col),
    ])
    |> sql.full_join(table: products, on: [
      sql.column("product_id")
      |> sql.column_for(orders)
      |> sql.eq(sql.column("id") |> sql.column_for(products), of: sql.col),
    ])
    |> sql.to_query(a())

  assert q.sql
    == "SELECT u.id, o.oid, p.pid FROM users AS u RIGHT JOIN orders AS o ON u.id = o.user_id FULL JOIN products AS p ON o.product_id = p.id"
  assert q.values == []
}

pub fn insert_with_null_test() {
  let inserter =
    sql.rows([#("Alice", sql.null)])
    |> sql.value("name", fn(r) { sql.text(r.0) })
    |> sql.value("email", fn(r) { r.1 })
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values(inserter)
    |> sql.to_query(a())

  assert q.sql == "INSERT INTO users (name, email) VALUES ($1, $2)"
  assert q.values == [sql.Text("Alice"), sql.Null]
}

pub fn insert_with_null_to_string_test() {
  let inserter =
    sql.rows([#("Alice", None)])
    |> sql.value("name", fn(r) { sql.text(r.0) })
    |> sql.value("email", fn(r: #(String, Option(String))) {
      sql.nullable(r.1, of: sql.text)
    })
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values(inserter)
    |> sql.to_string(a())

  assert q == "INSERT INTO users (name, email) VALUES ('Alice', NULL)"
}

pub fn insert_mixed_types_test() {
  let inserter =
    sql.rows([#("Alice", 30, 9.5, True)])
    |> sql.value("name", fn(r) { sql.text(r.0) })
    |> sql.value("age", fn(r) { sql.int(r.1) })
    |> sql.value("score", fn(r) { sql.float(r.2) })
    |> sql.value("active", fn(r) { sql.bool(r.3) })
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values(inserter)
    |> sql.to_query(a())

  assert q.sql
    == "INSERT INTO users (name, age, score, active) VALUES ($1, $2, $3, $4)"
  assert q.values
    == [
      sql.Text("Alice"),
      sql.Int(30),
      sql.Float(9.5),
      sql.Bool(True),
    ]
}

pub fn insert_mixed_types_to_string_test() {
  let inserter =
    sql.rows([#("Alice", 30, 9.5, True)])
    |> sql.value("name", fn(r) { sql.text(r.0) })
    |> sql.value("age", fn(r) { sql.int(r.1) })
    |> sql.value("score", fn(r) { sql.float(r.2) })
    |> sql.value("active", fn(r) { sql.bool(r.3) })
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values(inserter)
    |> sql.to_string(a())

  assert q
    == "INSERT INTO users (name, age, score, active) VALUES ('Alice', 30, 9.5, TRUE)"
}

pub fn insert_multiple_rows_to_string_test() {
  let inserter =
    sql.rows([#("Alice", 30), #("Bob", 25)])
    |> sql.value("name", fn(r) { sql.text(r.0) })
    |> sql.value("age", fn(r) { sql.int(r.1) })
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values(inserter)
    |> sql.to_string(a())

  assert q == "INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25)"
}

pub fn insert_on_conflict_do_nothing_backtick_test() {
  let inserter =
    sql.rows([#("Alice", "alice@example.com")])
    |> sql.value("name", fn(r) { sql.text(r.0) })
    |> sql.value("email", fn(r) { sql.text(r.1) })
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values(inserter)
    |> sql.on_conflict(target: "email", action: sql.DoNothing, where: [])
    |> sql.to_query(backtick_a())

  assert q.sql
    == "INSERT INTO `users` (`name`, `email`) VALUES (?, ?) ON CONFLICT (`email`) DO NOTHING"
  assert q.values == [sql.Text("Alice"), sql.Text("alice@example.com")]
}

pub fn insert_returning_backtick_test() {
  let inserter =
    sql.rows([#("Alice", "alice@example.com")])
    |> sql.value("name", fn(r) { sql.text(r.0) })
    |> sql.value("email", fn(r) { sql.text(r.1) })
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values(inserter)
    |> sql.returning([sql.column("id")])
    |> sql.to_query(backtick_a())

  assert q.sql
    == "INSERT INTO `users` (`name`, `email`) VALUES (?, ?) RETURNING `id`"
  assert q.values == [sql.Text("Alice"), sql.Text("alice@example.com")]
}

pub fn update_where_not_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("active", sql.false, of: sql.val)
    |> sql.where([
      sql.not(sql.column("role") |> sql.eq(sql.text("admin"), of: sql.val)),
    ])
    |> sql.to_query(a())

  assert q.sql == "UPDATE users SET active = $1 WHERE NOT (role = $2)"
  assert q.values == [sql.Bool(False), sql.Text("admin")]
}

pub fn update_where_like_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("category", sql.text("vip"), of: sql.val)
    |> sql.where([
      sql.column("email") |> sql.like(sql.text("%@company.com"), of: sql.val),
    ])
    |> sql.to_query(a())

  assert q.sql == "UPDATE users SET category = $1 WHERE email LIKE $2"
  assert q.values == [sql.Text("vip"), sql.Text("%@company.com")]
}

pub fn update_where_not_like_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("category", sql.text("standard"), of: sql.val)
    |> sql.where([
      sql.column("email")
      |> sql.not_like(sql.text("%@company.com"), of: sql.val),
    ])
    |> sql.to_query(a())

  assert q.sql == "UPDATE users SET category = $1 WHERE email NOT LIKE $2"
  assert q.values == [sql.Text("standard"), sql.Text("%@company.com")]
}

pub fn update_multiple_sets_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("name", sql.text("Bob"), of: sql.val)
    |> sql.set("age", sql.int(30), of: sql.val)
    |> sql.set("active", sql.true, of: sql.val)
    |> sql.where([sql.column("id") |> sql.eq(sql.int(1), of: sql.val)])
    |> sql.to_query(a())

  assert q.sql
    == "UPDATE users SET name = $1, age = $2, active = $3 WHERE id = $4"
  assert q.values
    == [
      sql.Text("Bob"),
      sql.Int(30),
      sql.Bool(True),
      sql.Int(1),
    ]
}

pub fn update_multiple_sets_to_string_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("name", sql.text("Bob"), of: sql.val)
    |> sql.set("age", sql.int(30), of: sql.val)
    |> sql.where([sql.column("id") |> sql.eq(sql.int(1), of: sql.val)])
    |> sql.to_string(a())

  assert q == "UPDATE users SET name = 'Bob', age = 30 WHERE id = 1"
}

pub fn update_where_not_to_string_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("active", sql.false, of: sql.val)
    |> sql.where([
      sql.not(sql.column("role") |> sql.eq(sql.text("admin"), of: sql.val)),
    ])
    |> sql.to_string(a())

  assert q == "UPDATE users SET active = FALSE WHERE NOT (role = 'admin')"
}

pub fn update_returning_to_string_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("active", sql.true, of: sql.val)
    |> sql.where([sql.column("id") |> sql.eq(sql.int(1), of: sql.val)])
    |> sql.returning([sql.column("id"), sql.column("active")])
    |> sql.to_string(a())

  assert q == "UPDATE users SET active = TRUE WHERE id = 1 RETURNING id, active"
}

pub fn delete_where_not_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.delete()
    |> sql.where([
      sql.not(sql.column("role") |> sql.eq(sql.text("admin"), of: sql.val)),
    ])
    |> sql.to_query(a())

  assert q.sql == "DELETE FROM users WHERE NOT (role = $1)"
  assert q.values == [sql.Text("admin")]
}

pub fn delete_where_like_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.delete()
    |> sql.where([
      sql.column("email") |> sql.like(sql.text("%@spam.com"), of: sql.val),
    ])
    |> sql.to_query(a())

  assert q.sql == "DELETE FROM users WHERE email LIKE $1"
  assert q.values == [sql.Text("%@spam.com")]
}

pub fn delete_where_not_like_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.delete()
    |> sql.where([
      sql.column("email")
      |> sql.not_like(sql.text("%@company.com"), of: sql.val),
    ])
    |> sql.to_query(a())

  assert q.sql == "DELETE FROM users WHERE email NOT LIKE $1"
  assert q.values == [sql.Text("%@company.com")]
}

pub fn delete_chained_wheres_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.delete()
    |> sql.where([
      sql.column("active") |> sql.eq(sql.false, of: sql.val),
      sql.column("age") |> sql.lt(sql.int(18), of: sql.val),
    ])
    |> sql.to_query(a())

  assert q.sql == "DELETE FROM users WHERE (active = $1 AND age < $2)"
  assert q.values == [sql.Bool(False), sql.Int(18)]
}

pub fn delete_to_string_with_date_test() {
  let d = calendar.Date(2024, calendar.January, 1)
  let q =
    sql.from(sql.table("events"))
    |> sql.delete()
    |> sql.where([sql.column("event_date") |> sql.lt(sql.date(d), of: sql.val)])
    |> sql.to_string(sql.adapter())

  assert q == "DELETE FROM events WHERE event_date < '2024-01-01'"
}

pub fn delete_where_not_to_string_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.delete()
    |> sql.where([
      sql.not(sql.column("role") |> sql.eq(sql.text("admin"), of: sql.val)),
    ])
    |> sql.to_string(a())

  assert q == "DELETE FROM users WHERE NOT (role = 'admin')"
}

pub fn delete_returning_to_string_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.delete()
    |> sql.where([sql.column("id") |> sql.eq(sql.int(1), of: sql.val)])
    |> sql.returning([sql.column("id")])
    |> sql.to_string(a())

  assert q == "DELETE FROM users WHERE id = 1 RETURNING id"
}

pub fn delete_backtick_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.delete()
    |> sql.where([sql.column("id") |> sql.eq(sql.int(1), of: sql.val)])
    |> sql.to_query(backtick_a())

  assert q.sql == "DELETE FROM `users` WHERE `id` = ?"
  assert q.values == [sql.Int(1)]
}

pub fn union_all_backtick_test() {
  let q =
    sql.union_all([
      sql.from(sql.table("admins")) |> sql.select([sql.column("id")]),
      sql.from(sql.table("users")) |> sql.select([sql.column("id")]),
    ])
    |> sql.to_query(backtick_a())

  assert q.sql == "SELECT `id` FROM `users` UNION ALL SELECT `id` FROM `admins`"
  assert q.values == []
}

pub fn union_with_limit_offset_test() {
  // Note: union currently doesn't support limit/offset on the combined result,
  // but individual selects can have them
  let q1 =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id")])
    |> sql.limit(5)

  let q2 =
    sql.from(sql.table("admins"))
    |> sql.select([sql.column("id")])
    |> sql.limit(3)

  let q =
    sql.union([q2, q1])
    |> sql.to_query(a())

  assert q.sql
    == "SELECT id FROM users LIMIT 5 UNION SELECT id FROM admins LIMIT 3"
  assert q.values == []
}

pub fn union_to_string_with_values_test() {
  let q1 =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.where([sql.column("active") |> sql.eq(sql.true, of: sql.val)])

  let q2 =
    sql.from(sql.table("admins"))
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.where([sql.column("active") |> sql.eq(sql.true, of: sql.val)])

  let q =
    sql.union([q2, q1])
    |> sql.to_string(a())

  assert q
    == "SELECT id, name FROM users WHERE active = TRUE UNION SELECT id, name FROM admins WHERE active = TRUE"
}

pub fn cte_with_join_test() {
  let cte_query =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.where([sql.column("active") |> sql.eq(sql.true, of: sql.val)])

  let active_users = sql.table("active_users") |> sql.table_as("au")
  let orders = sql.table("orders") |> sql.table_as("o")

  let q =
    sql.from(active_users)
    |> sql.select([
      sql.column("id") |> sql.column_for(active_users),
      sql.column("total") |> sql.column_for(orders),
    ])
    |> sql.inner_join(table: orders, on: [
      sql.column("id")
      |> sql.column_for(active_users)
      |> sql.eq(sql.column("user_id") |> sql.column_for(orders), of: sql.col),
    ])
    |> sql.with([sql.cte(name: "active_users", query: cte_query)])
    |> sql.to_query(a())

  assert q.sql
    == "WITH active_users AS (SELECT id, name FROM users WHERE active = $1) SELECT au.id, o.total FROM active_users AS au INNER JOIN orders AS o ON au.id = o.user_id;"
  assert q.values == [sql.Bool(True)]
}

pub fn cte_with_union_body_test() {
  let cte_query1 =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.where([sql.column("active") |> sql.eq(sql.true, of: sql.val)])

  let main1 =
    sql.from(sql.table("active_users"))
    |> sql.select([sql.column("id"), sql.column("name")])

  let main2 =
    sql.from(sql.table("admins"))
    |> sql.select([sql.column("id"), sql.column("name")])

  let q =
    sql.union([main2, main1])
    |> sql.with([sql.cte(name: "active_users", query: cte_query1)])
    |> sql.to_query(a())

  assert q.sql
    == "WITH active_users AS (SELECT id, name FROM users WHERE active = $1) SELECT id, name FROM active_users UNION SELECT id, name FROM admins;"
  assert q.values == [sql.Bool(True)]
}

pub fn cte_basic_to_string_with_values_test() {
  let cte_query =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.where([sql.column("active") |> sql.eq(sql.true, of: sql.val)])

  let q =
    sql.from(sql.table("active_users"))
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.with([sql.cte(name: "active_users", query: cte_query)])
    |> sql.to_string(a())

  assert q
    == "WITH active_users AS (SELECT id, name FROM users WHERE active = TRUE) SELECT id, name FROM active_users;"
}

pub fn cte_recursive_union_all_test() {
  // Recursive CTE: WITH RECURSIVE cte AS (base UNION ALL recursive_part)
  // Since CTE body must be a single SelectBuilder, we test the recursive
  // keyword with a simple select as the CTE body
  let base_query =
    sql.from(sql.table("categories"))
    |> sql.select([
      sql.column("id"),
      sql.column("parent_id"),
      sql.column("name"),
    ])
    |> sql.where([sql.column("parent_id") |> sql.is_null])

  let q =
    sql.from(sql.table("category_tree"))
    |> sql.select([
      sql.column("id"),
      sql.column("parent_id"),
      sql.column("name"),
    ])
    |> sql.with([sql.cte(name: "category_tree", query: base_query)])
    |> sql.recursive
    |> sql.to_query(a())

  assert q.sql
    == "WITH RECURSIVE category_tree AS (SELECT id, parent_id, name FROM categories WHERE parent_id IS NULL) SELECT id, parent_id, name FROM category_tree;"
  assert q.values == []
}

pub fn cte_multiple_to_string_test() {
  let cte1 =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.where([sql.column("active") |> sql.eq(sql.true, of: sql.val)])

  let cte2 =
    sql.from(sql.table("orders"))
    |> sql.select([
      sql.column("user_id"),
      sql.sum("amount") |> sql.column_as("total"),
    ])
    |> sql.group_by([sql.column("user_id")])

  let active_users = sql.table("active_users") |> sql.table_as("au")
  let user_orders = sql.table("user_orders") |> sql.table_as("uo")

  let q =
    sql.from(active_users)
    |> sql.select([
      sql.column("name") |> sql.column_for(active_users),
      sql.column("total") |> sql.column_for(user_orders),
    ])
    |> sql.inner_join(table: user_orders, on: [
      sql.column("id")
      |> sql.column_for(active_users)
      |> sql.eq(
        sql.column("user_id") |> sql.column_for(user_orders),
        of: sql.col,
      ),
    ])
    |> sql.with([
      sql.cte(name: "active_users", query: cte1),
      sql.cte(name: "user_orders", query: cte2),
    ])
    |> sql.to_string(a())

  assert q
    == "WITH active_users AS (SELECT id, name FROM users WHERE active = TRUE), user_orders AS (SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id) SELECT au.name, uo.total FROM active_users AS au INNER JOIN user_orders AS uo ON au.id = uo.user_id;"
}

pub fn select_all_with_where_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where([sql.column("id") |> sql.eq(sql.int(1), of: sql.val)])
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE id = $1"
  assert q.values == [sql.Int(1)]
}

pub fn insert_single_row_to_string_test() {
  let inserter =
    sql.rows([#("Alice", "alice@example.com")])
    |> sql.value("name", fn(r) { sql.text(r.0) })
    |> sql.value("email", fn(r) { sql.text(r.1) })

  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values(inserter)
    |> sql.to_string(a())

  assert q
    == "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')"
}

pub fn select_from_aliased_table_test() {
  let users = sql.table("users") |> sql.table_as("u")

  let q =
    sql.from(users)
    |> sql.select([sql.column("id") |> sql.column_for(users)])
    |> sql.to_query(a())

  assert q.sql == "SELECT u.id FROM users AS u"
  assert q.values == []
}

pub fn select_where_in_multiple_values_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.where([
      sql.column("status")
      |> sql.in(
        [
          sql.text("active"),
          sql.text("pending"),
          sql.text("verified"),
        ],
        of: sql.val,
      ),
    ])
    |> sql.to_query(a())

  assert q.sql == "SELECT id, name FROM users WHERE status IN ($1, $2, $3)"
  assert q.values
    == [
      sql.Text("active"),
      sql.Text("pending"),
      sql.Text("verified"),
    ]
}

pub fn on_conflict_do_update_to_string_test() {
  let inserter =
    sql.rows([#("Alice", "alice@example.com")])
    |> sql.value("name", fn(r) { sql.text(r.0) })
    |> sql.value("email", fn(r) { sql.text(r.1) })
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values(inserter)
    |> sql.on_conflict(
      target: "email",
      action: sql.DoUpdate(sets: [#("name", "EXCLUDED.name")]),
      where: [],
    )
    |> sql.to_string(a())

  assert q
    == "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com') ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name"
}

pub fn on_conflict_do_nothing_to_string_test() {
  let inserter =
    sql.rows([#("Alice", "alice@example.com")])
    |> sql.value("name", fn(r) { sql.text(r.0) })
    |> sql.value("email", fn(r) { sql.text(r.1) })
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values(inserter)
    |> sql.on_conflict(target: "email", action: sql.DoNothing, where: [])
    |> sql.to_string(a())

  assert q
    == "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com') ON CONFLICT (email) DO NOTHING"
}

pub fn select_where_subquery_test() {
  let sub =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id")])
    |> sql.where([sql.column("name") |> sql.eq(sql.text("Alice"), of: sql.val)])

  let q =
    sql.from(sql.table("orders"))
    |> sql.select([sql.star])
    |> sql.where([sql.column("user_id") |> sql.eq(sub, of: sql.subquery)])
    |> sql.to_query(a())

  assert q.sql
    == "SELECT * FROM orders WHERE user_id = (SELECT id FROM users WHERE name = $1)"
  assert q.values == [sql.Text("Alice")]
}

pub fn select_from_subquery_test() {
  let sub =
    sql.from(sql.table("orders"))
    |> sql.select([
      sql.column("user_id"),
      sql.sum("amount") |> sql.column_as("total"),
    ])
    |> sql.group_by([sql.column("user_id")])

  let q =
    sql.from_subquery(sub, "order_totals")
    |> sql.select([sql.column("user_id"), sql.column("total")])
    |> sql.where([sql.column("total") |> sql.gt(sql.int(100), of: sql.val)])
    |> sql.to_query(a())

  assert q.sql
    == "SELECT user_id, total FROM (SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id) AS order_totals WHERE total > $1"
  assert q.values == [sql.Int(100)]
}

pub fn select_where_exists_test() {
  let orders = sql.table("orders")
  let users = sql.table("users")

  let sub =
    sql.from(orders)
    |> sql.select([sql.column("id")])
    |> sql.where([
      sql.column("user_id")
      |> sql.column_for(orders)
      |> sql.eq(sql.column("id") |> sql.column_for(users), of: sql.col),
    ])

  let q =
    sql.from(users)
    |> sql.select([sql.star])
    |> sql.where([sql.exists(sub)])
    |> sql.to_query(a())

  assert q.sql
    == "SELECT * FROM users WHERE EXISTS (SELECT id FROM orders WHERE orders.user_id = users.id)"
}

pub fn select_where_any_test() {
  let sub =
    sql.from(sql.table("orders"))
    |> sql.select([sql.column("user_id")])

  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where([sql.column("id") |> sql.eq(sub, of: sql.any)])
    |> sql.to_query(a())

  assert q.sql
    == "SELECT * FROM users WHERE id = ANY (SELECT user_id FROM orders)"
}

pub fn select_where_all_test() {
  let sub =
    sql.from(sql.table("requirements"))
    |> sql.select([sql.column("min_age")])

  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where([sql.column("age") |> sql.gt(sub, of: sql.all)])
    |> sql.to_query(a())

  assert q.sql
    == "SELECT * FROM users WHERE age > ALL (SELECT min_age FROM requirements)"
}

pub fn select_where_is_true_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id")])
    |> sql.where([sql.column("active") |> sql.is_true])
    |> sql.to_query(a())

  assert q.sql == "SELECT id FROM users WHERE active IS TRUE"
}

pub fn select_where_is_false_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id")])
    |> sql.where([sql.column("active") |> sql.is_false])
    |> sql.to_query(a())

  assert q.sql == "SELECT id FROM users WHERE active IS FALSE"
}

pub fn select_where_raw_sql_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where([sql.raw("age > 18 AND active = true")])
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE age > 18 AND active = true"
  assert q.values == []
}

pub fn select_where_raw_sql_to_string_test() {
  let s =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where([sql.raw("age > 18 AND active = true")])
    |> sql.to_string(a())

  assert s == "SELECT * FROM users WHERE age > 18 AND active = true"
}

pub fn select_where_raw_combined_with_regular_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where([
      sql.column("name") |> sql.eq(sql.text("Alice"), of: sql.val),
      sql.raw("age > 18"),
    ])
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE (name = $1 AND age > 18)"
  assert q.values == [sql.Text("Alice")]
}

pub fn delete_where_raw_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.delete()
    |> sql.where([
      sql.raw("active = FALSE AND age > 21"),
    ])
    |> sql.to_query(a())

  assert q.sql == "DELETE FROM users WHERE active = FALSE AND age > 21"
  assert q.values == []
}

pub fn update_where_raw_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("status", sql.text("inactive"), of: sql.val)
    |> sql.where([
      sql.raw("age > 65 AND active = TRUE"),
    ])
    |> sql.to_query(a())

  assert q.sql
    == "UPDATE users SET status = $1 WHERE age > 65 AND active = TRUE"
  assert q.values == [sql.Text("inactive")]
}

pub fn select_where_not_convenience_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where([
      sql.not(sql.column("active") |> sql.eq(sql.false, of: sql.val)),
    ])
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE NOT (active = $1)"
  assert q.values == [sql.Bool(False)]
}

pub fn select_where_not_to_string_convenience_test() {
  let s =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where([
      sql.not(sql.column("active") |> sql.eq(sql.false, of: sql.val)),
    ])
    |> sql.to_string(a())

  assert s == "SELECT * FROM users WHERE NOT (active = FALSE)"
}

pub fn select_where_exists_convenience_test() {
  let orders = sql.table("orders")
  let users = sql.table("users")

  let subquery =
    sql.from(orders)
    |> sql.select([sql.column("id")])
    |> sql.where([
      sql.column("user_id")
      |> sql.column_for(orders)
      |> sql.eq(sql.column("id") |> sql.column_for(users), of: sql.col),
    ])

  let q =
    sql.from(users)
    |> sql.select([sql.star])
    |> sql.where([sql.exists(subquery)])
    |> sql.to_query(a())

  assert q.sql
    == "SELECT * FROM users WHERE EXISTS (SELECT id FROM orders WHERE orders.user_id = users.id)"
  assert q.values == []
}

pub fn select_where_nullable_some_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where([
      sql.column("age")
      |> sql.eq(sql.nullable(Some(25), of: sql.int), of: sql.val),
    ])
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE age = $1"
  assert q.values == [sql.Int(25)]
}

pub fn select_where_nullable_none_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where([
      sql.column("age") |> sql.eq(sql.nullable(None, of: sql.int), of: sql.val),
    ])
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE age = $1"
  assert q.values == [sql.Null]
}

pub fn select_where_nullable_none_to_string_test() {
  let s =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where([
      sql.column("age") |> sql.eq(sql.nullable(None, of: sql.int), of: sql.val),
    ])
    |> sql.to_string(a())

  assert s == "SELECT * FROM users WHERE age = NULL"
}

pub fn select_where_in_list_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where([
      sql.column("id")
      |> sql.in([1, 2, 3], of: sql.list(of: fn(i) { sql.int(i) })),
    ])
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE id IN ($1, $2, $3)"
  assert q.values == [sql.Int(1), sql.Int(2), sql.Int(3)]
}

pub fn select_where_in_list_strings_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where([
      sql.column("name")
      |> sql.in(["Alice", "Bob"], of: sql.list(of: fn(s) { sql.text(s) })),
    ])
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE name IN ($1, $2)"
  assert q.values == [sql.Text("Alice"), sql.Text("Bob")]
}

pub fn select_where_in_list_to_string_test() {
  let s =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where([
      sql.column("id")
      |> sql.in([1, 2, 3], of: sql.list(of: fn(i) { sql.int(i) })),
    ])
    |> sql.to_string(a())

  assert s == "SELECT * FROM users WHERE id IN (1, 2, 3)"
}

pub fn adapter_test() {
  let r =
    sql.adapter()
    |> sql.on_placeholder(fn(idx) { "$" <> int.to_string(idx) })

  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("name")])
    |> sql.where([sql.column("active") |> sql.eq(sql.true, of: sql.val)])
    |> sql.to_query(r)

  assert q.sql == "SELECT name FROM users WHERE active = $1"
  assert q.values == [sql.true]
}

pub fn mapper_handle_null_test() {
  let r =
    sql.adapter()
    |> sql.on_placeholder(fn(idx) { "$" <> int.to_string(idx) })
    |> sql.on_null(with: fn() { sql.null })
    |> sql.on_int(with: fn(i) { sql.int(i) })
    |> sql.on_text(with: fn(s) { sql.text(s) })
    |> sql.on_value(with: sql.value_to_string)

  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where([
      sql.column("name")
      |> sql.eq(sql.nullable(None, of: sql.text), of: sql.val),
    ])
    |> sql.to_query(r)

  assert q.sql == "SELECT * FROM users WHERE name = $1"
  assert q.values == [sql.Null]
}

pub fn update_with_order_by_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("active", sql.false, of: sql.val)
    |> sql.order_by([sql.column("created_at") |> sql.asc])
    |> sql.to_query(a())

  assert q.sql == "UPDATE users SET active = $1 ORDER BY created_at ASC"
  assert q.values == [sql.Bool(False)]
}

pub fn update_with_limit_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("active", sql.false, of: sql.val)
    |> sql.limit(10)
    |> sql.to_query(a())

  assert q.sql == "UPDATE users SET active = $1 LIMIT 10"
  assert q.values == [sql.Bool(False)]
}

pub fn update_with_order_by_limit_returning_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("active", sql.false, of: sql.val)
    |> sql.order_by([sql.column("created_at") |> sql.asc])
    |> sql.limit(10)
    |> sql.returning([sql.column("id")])
    |> sql.to_query(a())

  assert q.sql
    == "UPDATE users SET active = $1 ORDER BY created_at ASC LIMIT 10 RETURNING id"
  assert q.values == [sql.Bool(False)]
}

pub fn update_with_limit_offset_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("active", sql.false, of: sql.val)
    |> sql.limit(10)
    |> sql.offset(20)
    |> sql.to_query(a())

  assert q.sql == "UPDATE users SET active = $1 LIMIT 10 OFFSET 20"
  assert q.values == [sql.Bool(False)]
}

pub fn update_offset_without_limit_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("active", sql.false, of: sql.val)
    |> sql.offset(5)
    |> sql.to_query(a())

  assert q.sql == "UPDATE users SET active = $1 OFFSET 5"
  assert q.values == [sql.Bool(False)]
}

pub fn update_set_from_subquery_test() {
  let new_emails = sql.table("new_emails")
  let users = sql.table("users")

  let sub =
    sql.from(new_emails)
    |> sql.select([sql.column("email")])
    |> sql.where([
      sql.column("user_id")
      |> sql.column_for(new_emails)
      |> sql.eq(sql.column("id") |> sql.column_for(users), of: sql.col),
    ])

  let q =
    sql.update(table: users)
    |> sql.set("email", sub, of: sql.subquery)
    |> sql.to_query(a())

  assert q.sql
    == "UPDATE users SET email = (SELECT email FROM new_emails WHERE new_emails.user_id = users.id)"
  assert q.values == []
}

pub fn update_set_from_subquery_and_value_test() {
  let new_emails = sql.table("new_emails")
  let users = sql.table("users")

  let sub =
    sql.from(new_emails)
    |> sql.select([sql.column("email")])
    |> sql.where([
      sql.column("user_id")
      |> sql.column_for(new_emails)
      |> sql.eq(sql.column("id") |> sql.column_for(users), of: sql.col),
    ])

  let ts =
    calendar.Date(year: 2026, month: calendar.April, day: 5)
    |> timestamp.from_calendar(
      calendar.TimeOfDay(23, 59, 50, 0),
      duration.seconds(0),
    )

  let q =
    sql.update(table: users)
    |> sql.set("email", sub, of: sql.subquery)
    |> sql.set("updated_at", sql.timestamp(ts), of: sql.val)
    |> sql.to_query(a())

  assert q.sql
    == "UPDATE users SET email = (SELECT email FROM new_emails WHERE new_emails.user_id = users.id), updated_at = $1"

  assert q.values == [sql.timestamp(ts)]
}

pub fn update_set_from_subquery_and_value_string_test() {
  let new_emails = sql.table("new_emails")
  let users = sql.table("users")

  let sub =
    sql.from(new_emails)
    |> sql.select([sql.column("email")])
    |> sql.where([
      sql.column("user_id")
      |> sql.column_for(new_emails)
      |> sql.eq(sql.column("id") |> sql.column_for(users), of: sql.col),
    ])

  let ts =
    calendar.Date(year: 2026, month: calendar.April, day: 5)
    |> timestamp.from_calendar(
      calendar.TimeOfDay(23, 59, 50, 0),
      duration.seconds(0),
    )

  let ts_string = ts |> sql.timestamp |> sql.value_to_string

  let q =
    sql.update(table: users)
    |> sql.set("email", sub, of: sql.subquery)
    |> sql.set("updated_at", sql.timestamp(ts), of: sql.val)
    |> sql.to_string(a())

  assert q
    == "UPDATE users SET email = (SELECT email FROM new_emails WHERE new_emails.user_id = users.id), updated_at = "
    <> ts_string
}

pub fn update_set_from_subquery_with_scalar_test() {
  let new_emails = sql.table("new_emails")
  let users = sql.table("users")

  let sub =
    sql.from(new_emails)
    |> sql.select([sql.column("email")])
    |> sql.where([
      sql.column("user_id")
      |> sql.column_for(new_emails)
      |> sql.eq(sql.column("id") |> sql.column_for(users), of: sql.col),
    ])

  let q =
    sql.update(table: users)
    |> sql.set("email", sub, of: sql.subquery)
    |> sql.set("name", sql.text("Alice"), of: sql.val)
    |> sql.to_query(a())

  assert q.sql
    == "UPDATE users SET email = (SELECT email FROM new_emails WHERE new_emails.user_id = users.id), name = $1"
  assert q.values == [sql.text("Alice")]
}

pub fn update_set_from_column_test() {
  let q =
    sql.update(sql.table("accounts"))
    |> sql.set("balance", sql.column("balance + 10"), of: sql.col)
    |> sql.where([sql.column("id") |> sql.eq(sql.int(1), of: sql.val)])
    |> sql.to_query(a())

  assert q.sql == "UPDATE accounts SET balance = balance + 10 WHERE id = $1"
  assert q.values == [sql.int(1)]
}

pub fn cte_basic_with_semicolon_test() {
  let cte_query =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.where([sql.column("active") |> sql.eq(sql.true, of: sql.val)])

  let q =
    sql.from(sql.table("active_users"))
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.with([sql.cte(name: "active_users", query: cte_query)])
    |> sql.to_query(a())

  assert q.sql
    == "WITH active_users AS (SELECT id, name FROM users WHERE active = $1) SELECT id, name FROM active_users;"
}

pub fn cte_multiple_with_semicolon_test() {
  let cte1 =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id"), sql.column("name")])
    |> sql.where([sql.column("active") |> sql.eq(sql.true, of: sql.val)])

  let cte2 =
    sql.from(sql.table("orders"))
    |> sql.select([
      sql.column("user_id"),
      sql.sum("amount") |> sql.column_as("total"),
    ])
    |> sql.group_by([sql.column("user_id")])

  let active_users = sql.table("active_users") |> sql.table_as("au")
  let user_orders = sql.table("user_orders") |> sql.table_as("uo")

  let q =
    sql.from(active_users)
    |> sql.select([
      sql.column("name") |> sql.column_for(active_users),
      sql.column("total") |> sql.column_for(user_orders),
    ])
    |> sql.inner_join(table: user_orders, on: [
      sql.column("id")
      |> sql.column_for(active_users)
      |> sql.eq(
        sql.column("user_id") |> sql.column_for(user_orders),
        of: sql.col,
      ),
    ])
    |> sql.with([
      sql.cte(name: "active_users", query: cte1),
      sql.cte(name: "user_orders", query: cte2),
    ])
    |> sql.to_query(a())

  assert q.sql
    == "WITH active_users AS (SELECT id, name FROM users WHERE active = $1), user_orders AS (SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id) SELECT au.name, uo.total FROM active_users AS au INNER JOIN user_orders AS uo ON au.id = uo.user_id;"
}

pub fn cte_with_column_aliases_semicolon_test() {
  let cte_query =
    sql.from(sql.table("users"))
    |> sql.select([sql.column("id"), sql.column("name")])

  let q =
    sql.from(sql.table("u"))
    |> sql.select([sql.column("user_id"), sql.column("user_name")])
    |> sql.with([
      sql.cte(name: "u", query: cte_query)
      |> sql.cte_columns(columns: ["user_id", "user_name"]),
    ])
    |> sql.to_query(a())

  assert q.sql
    == "WITH u(user_id, user_name) AS (SELECT id, name FROM users) SELECT user_id, user_name FROM u;"
}

pub fn update_where_is_false_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("status", sql.text("inactive"), of: sql.val)
    |> sql.where([sql.column("active") |> sql.is_false])
    |> sql.to_query(a())

  assert q.sql == "UPDATE users SET status = $1 WHERE active IS FALSE"
  assert q.values == [sql.Text("inactive")]
}

pub fn bytea_to_string_test() {
  let s =
    sql.from(sql.table("data"))
    |> sql.select([sql.star])
    |> sql.where([
      sql.column("blob") |> sql.eq(sql.bytea(<<1, 2, 3>>), of: sql.val),
    ])
    |> sql.to_string(default_a())

  assert s == "SELECT * FROM data WHERE blob = '\\x010203'"
}

pub fn bytea_empty_to_string_test() {
  let s =
    sql.from(sql.table("data"))
    |> sql.select([sql.star])
    |> sql.where([sql.column("blob") |> sql.eq(sql.bytea(<<>>), of: sql.val)])
    |> sql.to_string(default_a())

  assert s == "SELECT * FROM data WHERE blob = '\\x'"
}

pub fn bytea_to_query_test() {
  let q =
    sql.from(sql.table("data"))
    |> sql.select([sql.star])
    |> sql.where([
      sql.column("blob") |> sql.eq(sql.bytea(<<1, 2, 3>>), of: sql.val),
    ])
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM data WHERE blob = $1"
  assert q.values == [sql.Bytea(<<1, 2, 3>>)]
}

pub fn timestamp_to_string_test() {
  let ts = timestamp.from_unix_seconds(0)
  let s =
    sql.from(sql.table("events"))
    |> sql.select([sql.star])
    |> sql.where([
      sql.column("created_at") |> sql.eq(sql.timestamp(ts), of: sql.val),
    ])
    |> sql.to_string(default_a())

  // Epoch timestamp rendered as RFC3339 UTC
  assert string.contains(s, "'1970-01-01T00:00:00")
}

pub fn timestamp_to_query_test() {
  let ts = timestamp.from_unix_seconds(0)
  let q =
    sql.from(sql.table("events"))
    |> sql.select([sql.star])
    |> sql.where([
      sql.column("created_at") |> sql.eq(sql.timestamp(ts), of: sql.val),
    ])
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM events WHERE created_at = $1"
  assert q.values == [sql.Timestamp(ts)]
}

pub fn timestamptz_zero_offset_to_string_test() {
  let ts = timestamp.from_unix_seconds(0)
  let offset = sql.utc_offset(0)
  let s =
    sql.from(sql.table("events"))
    |> sql.select([sql.star])
    |> sql.where([
      sql.column("ts") |> sql.eq(sql.timestamptz(ts, offset), of: sql.val),
    ])
    |> sql.to_string(default_a())

  // Zero offset should be identity — same as plain timestamp at epoch
  assert string.contains(s, "'1970-01-01T00:00:00")
}

pub fn timestamptz_positive_offset_to_string_test() {
  // 5 hours in seconds = 18000; timestamp at 18000 UTC with +5 offset
  // should subtract 5h to produce epoch
  let ts = timestamp.from_unix_seconds(18_000)
  let offset = sql.utc_offset(5)
  let s =
    sql.from(sql.table("events"))
    |> sql.select([sql.star])
    |> sql.where([
      sql.column("ts") |> sql.eq(sql.timestamptz(ts, offset), of: sql.val),
    ])
    |> sql.to_string(default_a())

  // +5 offset subtracts 5 hours → back to epoch
  assert string.contains(s, "'1970-01-01T00:00:00")
}

pub fn timestamptz_to_query_test() {
  let ts = timestamp.from_unix_seconds(0)
  let offset = sql.utc_offset(5)
  let q =
    sql.from(sql.table("events"))
    |> sql.select([sql.star])
    |> sql.where([
      sql.column("ts") |> sql.eq(sql.timestamptz(ts, offset), of: sql.val),
    ])
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM events WHERE ts = $1"
  assert q.values == [sql.Timestamptz(ts, sql.Offset(hours: 5, minutes: 0))]
}

pub fn interval_to_string_test() {
  let s =
    sql.from(sql.table("tasks"))
    |> sql.select([sql.star])
    |> sql.where([
      sql.column("duration")
      |> sql.eq(sql.interval(interval.months(2)), of: sql.val),
    ])
    |> sql.to_string(default_a())

  assert s == "SELECT * FROM tasks WHERE duration = P2M"
}

pub fn interval_to_query_test() {
  let iv = interval.months(2)
  let q =
    sql.from(sql.table("tasks"))
    |> sql.select([sql.star])
    |> sql.where([
      sql.column("duration") |> sql.eq(sql.interval(iv), of: sql.val),
    ])
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM tasks WHERE duration = $1"
  assert q.values == [sql.Interval(iv)]
}

pub fn uuid_nil_to_string_test() {
  let s =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where([sql.column("id") |> sql.eq(sql.uuid(uuid.nil), of: sql.val)])
    |> sql.to_string(default_a())

  assert s
    == "SELECT * FROM users WHERE id = 00000000-0000-0000-0000-000000000000"
}

pub fn uuid_to_query_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where([sql.column("id") |> sql.eq(sql.uuid(uuid.nil), of: sql.val)])
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE id = $1"
  assert q.values == [sql.Uuid(uuid.nil)]
}

pub fn array_to_string_test() {
  let s =
    sql.from(sql.table("data"))
    |> sql.select([sql.star])
    |> sql.where([
      sql.column("tags")
      |> sql.eq(sql.array([1, 2, 3], of: sql.int), of: sql.val),
    ])
    |> sql.to_string(default_a())

  assert s == "SELECT * FROM data WHERE tags = [1, 2, 3]"
}

pub fn array_empty_to_string_test() {
  let s =
    sql.from(sql.table("data"))
    |> sql.select([sql.star])
    |> sql.where([
      sql.column("tags") |> sql.eq(sql.array([], of: sql.int), of: sql.val),
    ])
    |> sql.to_string(default_a())

  assert s == "SELECT * FROM data WHERE tags = []"
}

pub fn array_single_to_string_test() {
  let s =
    sql.from(sql.table("data"))
    |> sql.select([sql.star])
    |> sql.where([
      sql.column("tags")
      |> sql.eq(sql.array(["hello"], of: sql.text), of: sql.val),
    ])
    |> sql.to_string(default_a())

  assert s == "SELECT * FROM data WHERE tags = ['hello']"
}

pub fn array_to_query_test() {
  let q =
    sql.from(sql.table("data"))
    |> sql.select([sql.star])
    |> sql.where([
      sql.column("tags") |> sql.eq(sql.array([1, 2], of: sql.int), of: sql.val),
    ])
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM data WHERE tags = $1"
  assert q.values == [sql.Array([sql.Int(1), sql.Int(2)])]
}

pub fn utc_offset_test() {
  assert sql.utc_offset(5) == sql.Offset(hours: 5, minutes: 0)
}

pub fn utc_offset_negative_test() {
  assert sql.utc_offset(-3) == sql.Offset(hours: -3, minutes: 0)
}

pub fn utc_offset_zero_test() {
  assert sql.utc_offset(0) == sql.Offset(hours: 0, minutes: 0)
}

pub fn utc_offset_with_minutes_test() {
  assert sql.utc_offset(5) |> sql.minutes(30)
    == sql.Offset(hours: 5, minutes: 30)
}

pub fn in_empty_list_to_string_test() {
  let s =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where([sql.column("id") |> sql.in([], of: sql.val)])
    |> sql.to_string(a())

  assert s == "SELECT * FROM users WHERE id IN ()"
}

pub fn in_empty_list_to_query_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where([sql.column("id") |> sql.in([], of: sql.val)])
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE id IN ()"
  assert q.values == []
}

pub fn insert_empty_values_to_string_test() {
  let s =
    sql.insert(into: sql.table("users"))
    |> sql.values(sql.rows([]))
    |> sql.to_string(a())

  assert s == "INSERT INTO users () VALUES "
}

pub fn insert_empty_values_to_query_test() {
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values(sql.rows([]))
    |> sql.to_query(a())

  assert q.sql == "INSERT INTO users () VALUES "
  assert q.values == []
}

pub fn update_set_nullable_some_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("name", sql.nullable(Some("Jane"), of: sql.text), of: sql.val)
    |> sql.where([sql.column("id") |> sql.eq(sql.int(1), of: sql.val)])
    |> sql.to_query(a())

  assert q.sql == "UPDATE users SET name = $1 WHERE id = $2"
  assert q.values == [sql.Text("Jane"), sql.Int(1)]
}

pub fn update_set_nullable_none_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("name", sql.nullable(None, of: sql.text), of: sql.val)
    |> sql.where([sql.column("id") |> sql.eq(sql.int(1), of: sql.val)])
    |> sql.to_query(a())

  assert q.sql == "UPDATE users SET name = $1 WHERE id = $2"
  assert q.values == [sql.Null, sql.Int(1)]
}
