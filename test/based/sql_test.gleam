import based/sql
import gleam/float
import gleam/int
import gleam/option.{None, Some}
import gleam/time/calendar

fn sql_value_to_string(value: sql.Value) -> String {
  case value {
    sql.Int(n) -> int.to_string(n)
    sql.Float(f) -> float.to_string(f)
    sql.Text(s) -> "'" <> s <> "'"
    sql.Bool(True) -> "TRUE"
    sql.Bool(False) -> "FALSE"
    sql.Null -> "NULL"
    _ -> "?"
  }
}

fn a() -> sql.Adapter(sql.Value) {
  sql.adapter()
  |> sql.on_null(with: fn() { sql.null })
  |> sql.on_int(with: fn(i) { sql.int(i) })
  |> sql.on_text(with: fn(s) { sql.text(s) })
  |> sql.on_value(with: sql_value_to_string)
}

fn backtick_a() -> sql.Adapter(sql.Value) {
  sql.adapter()
  |> sql.on_null(with: fn() { sql.null })
  |> sql.on_int(with: fn(i) { sql.int(i) })
  |> sql.on_text(with: fn(s) { sql.text(s) })
  |> sql.on_placeholder(with: fn(_i) { "?" })
  |> sql.on_value(with: sql_value_to_string)
  |> sql.on_identifier(with: fn(name) { "`" <> name <> "`" })
}

// ---- Table Constructor Tests ----

pub fn table_test() {
  let t = sql.table("users")
  assert t == sql.table("users")
}

pub fn table_alias_test() {
  let t = sql.table("users") |> sql.table_as("u")
  assert t == sql.table("users") |> sql.table_as("u")
}

// ---- Column Constructor Tests ----

pub fn col_test() {
  let c = sql.col("email")
  assert c == sql.col("email")
}

pub fn col_of_table_test() {
  let c = sql.col("email") |> sql.col_for("users")
  assert c == sql.col("email") |> sql.col_for("users")
}

pub fn col_alias_test() {
  let c = sql.col("email") |> sql.col_for("users") |> sql.col_as("e")
  assert c == sql.col("email") |> sql.col_for("users") |> sql.col_as("e")
}

// ---- Simple SELECT Tests ----

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
    |> sql.select([sql.col("name"), sql.col("age")])
    |> sql.to_query(a())

  assert q.sql == "SELECT name, age FROM users"
  assert q.values == []
}

pub fn select_qualified_columns_test() {
  let q =
    sql.from(sql.table("users") |> sql.table_as("u"))
    |> sql.select([
      sql.col("name") |> sql.col_for("u"),
      sql.col("email") |> sql.col_for("u"),
    ])
    |> sql.to_query(a())

  assert q.sql == "SELECT u.name, u.email FROM users AS u"
}

pub fn select_aliased_column_test() {
  let q =
    sql.from(sql.table("users") |> sql.table_as("u"))
    |> sql.select([
      sql.col("email") |> sql.col_for("u") |> sql.col_as("user_email"),
    ])
    |> sql.to_query(a())

  assert q.sql == "SELECT u.email AS user_email FROM users AS u"
}

// ---- SELECT with WHERE Tests ----

pub fn select_where_eq_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where(sql.eq(sql.col("age"), sql.int(21), of: sql.value))
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE age = $1"
  assert q.values == [sql.int(21)]
}

pub fn select_multiple_wheres_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where(sql.eq(sql.col("active"), sql.true, of: sql.value))
    |> sql.where(sql.gt(sql.col("age"), sql.int(18), of: sql.value))
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE (active = $1 AND age > $2)"
  assert q.values == [sql.true, sql.int(18)]
}

pub fn select_or_where_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where(sql.eq(sql.col("role"), sql.text("admin"), of: sql.value))
    |> sql.or_where(sql.eq(
      sql.col("role"),
      sql.text("superadmin"),
      of: sql.value,
    ))
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE (role = $1 OR role = $2)"
  assert q.values == [sql.text("admin"), sql.text("superadmin")]
}

pub fn select_where_between_test() {
  let q =
    sql.from(sql.table("products"))
    |> sql.select([sql.star])
    |> sql.where(sql.between(
      sql.col("price"),
      sql.float(10.0),
      sql.float(100.0),
      of: sql.value,
    ))
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM products WHERE price BETWEEN $1 AND $2"
  assert q.values == [sql.float(10.0), sql.float(100.0)]
}

pub fn select_where_in_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where(sql.in(
      sql.col("id"),
      [
        sql.int(1),
        sql.int(2),
        sql.int(3),
      ],
      of: sql.value,
    ))
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE id IN ($1, $2, $3)"
  assert q.values == [sql.int(1), sql.int(2), sql.int(3)]
}

pub fn select_where_is_null_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where(sql.is_null(sql.col("deleted_at")))
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE deleted_at IS NULL"
  assert q.values == []
}

pub fn select_where_is_not_null_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where(sql.is_not_null(sql.col("email")))
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE email IS NOT NULL"
  assert q.values == []
}

pub fn select_where_not_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where(sql.not(sql.eq(sql.col("active"), sql.true, of: sql.value)))
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE NOT (active = $1)"
  assert q.values == [sql.true]
}

pub fn select_where_like_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where(sql.like(sql.col("name"), sql.text("%john%"), of: sql.value))
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE name LIKE $1"
  assert q.values == [sql.text("%john%")]
}

// ---- SELECT with JOIN Tests ----

pub fn select_inner_join_test() {
  let q =
    sql.from(sql.table("users") |> sql.table_as("u"))
    |> sql.select([sql.col("name"), sql.col("total") |> sql.col_for("o")])
    |> sql.join(sql.inner_join(
      table: sql.table("orders") |> sql.table_as("o"),
      on: sql.eq(
        sql.col("id") |> sql.col_for("u"),
        sql.col("user_id") |> sql.col_for("o"),
        of: sql.column,
      ),
    ))
    |> sql.to_query(a())

  assert q.sql
    == "SELECT name, o.total FROM users AS u INNER JOIN orders AS o ON u.id = o.user_id"
  assert q.values == []
}

pub fn select_left_join_test() {
  let q =
    sql.from(sql.table("users") |> sql.table_as("u"))
    |> sql.select([sql.star])
    |> sql.join(sql.left_join(
      table: sql.table("profiles") |> sql.table_as("p"),
      on: sql.eq(
        sql.col("id") |> sql.col_for("u"),
        sql.col("user_id") |> sql.col_for("p"),
        of: sql.column,
      ),
    ))
    |> sql.to_query(a())

  assert q.sql
    == "SELECT * FROM users AS u LEFT JOIN profiles AS p ON u.id = p.user_id"
}

// ---- SELECT with ORDER BY, LIMIT, OFFSET, GROUP BY ----

pub fn select_order_by_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.order_by(sql.col("name"), sql.asc)
    |> sql.order_by(sql.col("age"), sql.desc)
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
    |> sql.select([sql.col("department"), sql.col("count")])
    |> sql.group_by([sql.col("department")])
    |> sql.to_query(a())

  assert q.sql == "SELECT department, count FROM employees GROUP BY department"
}

// ---- INSERT Tests ----

pub fn insert_single_row_test() {
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values([
      {
        use <- sql.field(column: "name", value: sql.text("Alice"))
        sql.final(column: "age", value: sql.int(30))
      },
    ])
    |> sql.to_query(a())

  assert q.sql == "INSERT INTO users (name, age) VALUES ($1, $2)"
  assert q.values == [sql.text("Alice"), sql.int(30)]
}

pub fn insert_multiple_rows_test() {
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values([
      {
        use <- sql.field(column: "name", value: sql.text("Alice"))
        sql.final(column: "age", value: sql.int(30))
      },
      {
        use <- sql.field(column: "name", value: sql.text("Bob"))
        sql.final(column: "age", value: sql.int(25))
      },
    ])
    |> sql.to_query(a())

  assert q.sql == "INSERT INTO users (name, age) VALUES ($1, $2), ($3, $4)"
  assert q.values
    == [sql.text("Alice"), sql.int(30), sql.text("Bob"), sql.int(25)]
}

// ---- UPDATE Tests ----

pub fn update_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("name", sql.text("Alice"), of: sql.value)
    |> sql.set("age", sql.int(31), of: sql.value)
    |> sql.where(sql.eq(sql.col("id"), sql.int(1), of: sql.value))
    |> sql.to_query(a())

  assert q.sql == "UPDATE users SET name = $1, age = $2 WHERE id = $3"
  assert q.values == [sql.text("Alice"), sql.int(31), sql.int(1)]
}

pub fn update_no_where_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("active", sql.false, of: sql.value)
    |> sql.to_query(a())

  assert q.sql == "UPDATE users SET active = $1"
  assert q.values == [sql.false]
}

// ---- DELETE Tests ----

pub fn delete_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.delete()
    |> sql.where(sql.eq(sql.col("id"), sql.int(42), of: sql.value))
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

// ---- to_string Tests ----

pub fn to_string_select_test() {
  let s =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("name"), sql.col("age")])
    |> sql.where(sql.eq(sql.col("age"), sql.int(21), of: sql.value))
    |> sql.to_string(a())

  assert s == "SELECT name, age FROM users WHERE age = 21"
}

pub fn to_string_insert_test() {
  let s =
    sql.insert(into: sql.table("users"))
    |> sql.values([
      {
        use <- sql.field(column: "name", value: sql.text("Alice"))
        sql.final(column: "active", value: sql.true)
      },
    ])
    |> sql.to_string(a())

  assert s == "INSERT INTO users (name, active) VALUES ('Alice', TRUE)"
}

pub fn to_string_update_test() {
  let s =
    sql.update(table: sql.table("users"))
    |> sql.set("name", sql.text("Bob"), of: sql.value)
    |> sql.where(sql.eq(sql.col("id"), sql.int(1), of: sql.value))
    |> sql.to_string(a())

  assert s == "UPDATE users SET name = 'Bob' WHERE id = 1"
}

pub fn to_string_delete_test() {
  let s =
    sql.from(sql.table("users"))
    |> sql.delete()
    |> sql.where(sql.lt(sql.col("age"), sql.int(18), of: sql.value))
    |> sql.to_string(a())

  assert s == "DELETE FROM users WHERE age < 18"
}

// ---- default_formatter Tests ----

pub fn default_formatter_placeholder_test() {
  let r = sql.default_adapter()
  let q =
    sql.from(sql.table("users"))
    |> sql.select([
      sql.col("a"),
      sql.col("b"),
      sql.col("c"),
      sql.col("d"),
      sql.col("e"),
    ])
    |> sql.where(sql.eq(sql.col("x"), sql.int(1), of: sql.value))
    |> sql.to_query(r)
  // Verifies placeholder starts at $1
  assert q.sql == "SELECT a, b, c, d, e FROM users WHERE x = $1"
}

pub fn default_formatter_quote_test() {
  let r = sql.default_adapter()
  // default_formatter uses identity for quote_identifier, so names are unquoted
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("name")])
    |> sql.to_query(r)
  assert q.sql == "SELECT name FROM users"
}

pub fn default_formatter_value_to_string_test() {
  let r = sql.default_adapter()
  let base =
    sql.from(sql.table("t"))
    |> sql.select([sql.col("x")])

  assert sql.to_string(
      base |> sql.where(sql.eq(sql.col("x"), sql.int(42), of: sql.value)),
      r,
    )
    == "SELECT x FROM t WHERE x = 42"
  assert sql.to_string(
      base |> sql.where(sql.eq(sql.col("x"), sql.text("hello"), of: sql.value)),
      r,
    )
    == "SELECT x FROM t WHERE x = 'hello'"
  assert sql.to_string(
      base |> sql.where(sql.eq(sql.col("x"), sql.true, of: sql.value)),
      r,
    )
    == "SELECT x FROM t WHERE x = TRUE"
  assert sql.to_string(
      base |> sql.where(sql.eq(sql.col("x"), sql.false, of: sql.value)),
      r,
    )
    == "SELECT x FROM t WHERE x = FALSE"
  assert sql.to_string(
      base |> sql.where(sql.eq(sql.col("x"), sql.null, of: sql.value)),
      r,
    )
    == "SELECT x FROM t WHERE x = NULL"
}

pub fn default_formatter_escapes_quotes_test() {
  let r = sql.default_adapter()
  let s =
    sql.from(sql.table("t"))
    |> sql.select([sql.col("x")])
    |> sql.where(sql.eq(sql.col("x"), sql.text("it's"), of: sql.value))
    |> sql.to_string(r)
  assert s == "SELECT x FROM t WHERE x = 'it''s'"
}

// ---- Full default_formatter Integration Test ----

pub fn default_formatter_to_query_test() {
  let r = sql.default_adapter()
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("name"), sql.col("email")])
    |> sql.where(sql.eq(sql.col("active"), sql.true, of: sql.value))
    |> sql.limit(5)
    |> sql.to_query(r)

  assert q.sql == "SELECT name, email FROM users WHERE active = $1 LIMIT 5"
  assert q.values == [sql.true]
}

// ---- Custom Formatter Test (Backtick style) ----

pub fn custom_backtick_formatter_test() {
  let backtick_r =
    sql.adapter()
    |> sql.on_null(with: fn() { sql.null })
    |> sql.on_int(with: fn(i) { sql.int(i) })
    |> sql.on_text(with: fn(s) { sql.text(s) })
    |> sql.on_placeholder(with: fn(_) { "?" })
    |> sql.on_value(with: sql_value_to_string)
    |> sql.on_identifier(with: fn(name) { "`" <> name <> "`" })

  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("name")])
    |> sql.where(sql.eq(sql.col("id"), sql.int(1), of: sql.value))
    |> sql.to_query(backtick_r)

  assert q.sql == "SELECT `name` FROM `users` WHERE `id` = ?"
  assert q.values == [sql.int(1)]
}

// ---- Identifier Quoting Style Tests ----

pub fn backtick_quote_identifier_test() {
  let f =
    sql.adapter()
    |> sql.on_null(with: fn() { sql.null })
    |> sql.on_int(with: fn(i) { sql.int(i) })
    |> sql.on_text(with: fn(s) { sql.text(s) })
    |> sql.on_placeholder(with: fn(_) { "?" })
    |> sql.on_value(with: sql_value_to_string)
    |> sql.on_identifier(with: fn(name) { "`" <> name <> "`" })

  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("name")])
    |> sql.where(sql.eq(sql.col("id"), sql.int(1), of: sql.value))
    |> sql.to_query(f)

  assert q.sql == "SELECT `name` FROM `users` WHERE `id` = ?"

  let s =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("name")])
    |> sql.where(sql.eq(sql.col("id"), sql.int(1), of: sql.value))
    |> sql.to_string(f)

  assert s == "SELECT `name` FROM `users` WHERE `id` = 1"
}

pub fn double_quote_quote_identifier_test() {
  let f =
    sql.adapter()
    |> sql.on_null(with: fn() { sql.null })
    |> sql.on_int(with: fn(i) { sql.int(i) })
    |> sql.on_text(with: fn(s) { sql.text(s) })
    |> sql.on_value(with: sql_value_to_string)
    |> sql.on_identifier(with: fn(name) { "\"" <> name <> "\"" })

  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("name")])
    |> sql.where(sql.eq(sql.col("id"), sql.int(1), of: sql.value))
    |> sql.to_query(f)

  assert q.sql == "SELECT \"name\" FROM \"users\" WHERE \"id\" = $1"

  let s =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("name")])
    |> sql.where(sql.eq(sql.col("id"), sql.int(1), of: sql.value))
    |> sql.to_string(f)

  assert s == "SELECT \"name\" FROM \"users\" WHERE \"id\" = 1"
}

pub fn double_quote_aliased_identifiers_test() {
  let f =
    sql.adapter()
    |> sql.on_null(with: fn() { sql.null })
    |> sql.on_int(with: fn(i) { sql.int(i) })
    |> sql.on_text(with: fn(s) { sql.text(s) })
    |> sql.on_value(with: sql_value_to_string)
    |> sql.on_identifier(with: fn(name) { "\"" <> name <> "\"" })

  let q =
    sql.from(sql.table("users") |> sql.table_as("u"))
    |> sql.select([
      sql.col("name") |> sql.col_for("u"),
      sql.col("total") |> sql.col_for("o") |> sql.col_as("order_total"),
    ])
    |> sql.join(sql.inner_join(
      table: sql.table("orders") |> sql.table_as("o"),
      on: sql.eq(
        sql.col("id") |> sql.col_for("u"),
        sql.col("user_id") |> sql.col_for("o"),
        of: sql.column,
      ),
    ))
    |> sql.where(sql.eq(
      sql.col("active") |> sql.col_for("u"),
      sql.true,
      of: sql.value,
    ))
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
    |> sql.on_value(with: sql_value_to_string)
    |> sql.on_identifier(with: fn(name) { "\"" <> name <> "\"" })

  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("name")])
    |> sql.where(sql.eq(sql.col("id"), sql.int(1), of: sql.value))
    |> sql.to_query(f)

  assert q.sql == "SELECT \"name\" FROM \"users\" WHERE \"id\" = ?"

  let s =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("name")])
    |> sql.where(sql.eq(sql.col("id"), sql.int(1), of: sql.value))
    |> sql.to_string(f)

  assert s == "SELECT \"name\" FROM \"users\" WHERE \"id\" = 1"
}

// ---- Generic Value Type Test ----

pub type MyValue {
  MyInt(Int)
  MyStr(String)
}

pub fn generic_value_type_test() {
  let my_adapter =
    sql.adapter()
    |> sql.on_null(with: fn() { MyStr("NULL") })
    |> sql.on_int(with: fn(i) { MyInt(i) })
    |> sql.on_text(with: fn(s) { MyStr(s) })
    |> sql.on_placeholder(with: fn(i) { "?" <> int.to_string(i + 1) })
    |> sql.on_value(with: fn(v: MyValue) {
      case v {
        MyInt(n) -> int.to_string(n)
        MyStr(s) -> "'" <> s <> "'"
      }
    })
    |> sql.on_identifier(with: fn(name) { "[" <> name <> "]" })

  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("name")])
    |> sql.where(sql.eq(sql.col("id"), MyInt(42), of: sql.value))
    |> sql.to_query(my_adapter)

  assert q.sql == "SELECT [name] FROM [users] WHERE [id] = ?1"
  assert q.values == [MyInt(42)]

  let s =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("name")])
    |> sql.where(sql.eq(sql.col("id"), MyInt(42), of: sql.value))
    |> sql.to_string(my_adapter)

  assert s == "SELECT [name] FROM [users] WHERE [id] = 42"
}

// ---- Complex Query Test ----

pub fn complex_query_test() {
  let q =
    sql.from(sql.table("users") |> sql.table_as("u"))
    |> sql.select([
      sql.col("name") |> sql.col_for("u"),
      sql.col("email") |> sql.col_for("u"),
      sql.col("total") |> sql.col_for("o") |> sql.col_as("order_total"),
    ])
    |> sql.join(sql.inner_join(
      table: sql.table("orders") |> sql.table_as("o"),
      on: sql.eq(
        sql.col("id") |> sql.col_for("u"),
        sql.col("user_id") |> sql.col_for("o"),
        of: sql.column,
      ),
    ))
    |> sql.where(sql.gt(
      sql.col("total") |> sql.col_for("o"),
      sql.float(50.0),
      of: sql.value,
    ))
    |> sql.where(sql.eq(
      sql.col("active") |> sql.col_for("u"),
      sql.true,
      of: sql.value,
    ))
    |> sql.order_by(sql.col("total") |> sql.col_for("o"), sql.desc)
    |> sql.limit(10)
    |> sql.offset(0)
    |> sql.to_query(a())

  assert q.sql
    == "SELECT u.name, u.email, o.total AS order_total FROM users AS u INNER JOIN orders AS o ON u.id = o.user_id WHERE (o.total > $1 AND u.active = $2) ORDER BY o.total DESC LIMIT 10 OFFSET 0"
  assert q.values == [sql.float(50.0), sql.true]
}

// ---- Column-to-Column in WHERE Test ----

pub fn column_to_column_where_test() {
  let q =
    sql.from(sql.table("products"))
    |> sql.select([sql.star])
    |> sql.where(sql.gt(sql.col("price"), sql.col("cost"), of: sql.column))
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM products WHERE price > cost"
  assert q.values == []
}

// ---- DISTINCT Tests ----

pub fn select_distinct_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("name")])
    |> sql.distinct
    |> sql.to_query(a())

  assert q.sql == "SELECT DISTINCT name FROM users"
  assert q.values == []
}

pub fn select_distinct_multiple_columns_test() {
  let q =
    sql.from(sql.table("employees"))
    |> sql.select([sql.col("department"), sql.col("role")])
    |> sql.distinct
    |> sql.to_query(a())

  assert q.sql == "SELECT DISTINCT department, role FROM employees"
}

pub fn select_distinct_to_string_test() {
  let s =
    sql.from(sql.table("readings"))
    |> sql.select([sql.col("value")])
    |> sql.distinct
    |> sql.to_string(a())

  assert s == "SELECT DISTINCT value FROM readings"
}

// ---- Aggregate Function Tests ----

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
    |> sql.select([sql.count("*") |> sql.col_as("total")])
    |> sql.to_query(a())

  assert q.sql == "SELECT COUNT(*) AS total FROM users"
}

pub fn aggregate_with_table_test() {
  let q =
    sql.from(sql.table("orders") |> sql.table_as("o"))
    |> sql.select([sql.sum("amount") |> sql.col_for("o")])
    |> sql.to_query(a())

  assert q.sql == "SELECT SUM(o.amount) FROM orders AS o"
}

pub fn multiple_aggregates_test() {
  let q =
    sql.from(sql.table("employees"))
    |> sql.select([
      sql.col("department"),
      sql.count("*") |> sql.col_as("cnt"),
      sql.avg("salary") |> sql.col_as("avg_salary"),
    ])
    |> sql.group_by([sql.col("department")])
    |> sql.to_query(a())

  assert q.sql
    == "SELECT department, COUNT(*) AS cnt, AVG(salary) AS avg_salary FROM employees GROUP BY department"
}

// ---- HAVING Tests ----

pub fn having_test() {
  let q =
    sql.from(sql.table("employees"))
    |> sql.select([sql.col("department"), sql.count("*") |> sql.col_as("cnt")])
    |> sql.group_by([sql.col("department")])
    |> sql.having(sql.gt(sql.count("*"), sql.int(5), of: sql.value))
    |> sql.to_query(a())

  assert q.sql
    == "SELECT department, COUNT(*) AS cnt FROM employees GROUP BY department HAVING COUNT(*) > $1"
  assert q.values == [sql.int(5)]
}

pub fn having_multiple_test() {
  let q =
    sql.from(sql.table("employees"))
    |> sql.select([sql.col("department")])
    |> sql.group_by([sql.col("department")])
    |> sql.having(sql.gt(sql.count("*"), sql.int(5), of: sql.value))
    |> sql.having(sql.gt(sql.avg("salary"), sql.float(50_000.0), of: sql.value))
    |> sql.to_query(a())

  assert q.sql
    == "SELECT department FROM employees GROUP BY department HAVING (COUNT(*) > $1 AND AVG(salary) > $2)"
  assert q.values == [sql.int(5), sql.float(50_000.0)]
}

pub fn having_to_string_test() {
  let s =
    sql.from(sql.table("employees"))
    |> sql.select([sql.col("department"), sql.count("*") |> sql.col_as("cnt")])
    |> sql.group_by([sql.col("department")])
    |> sql.having(sql.gt(sql.count("*"), sql.int(5), of: sql.value))
    |> sql.to_string(a())

  assert s
    == "SELECT department, COUNT(*) AS cnt FROM employees GROUP BY department HAVING COUNT(*) > 5"
}

// ---- RETURNING Tests ----

pub fn insert_returning_test() {
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values([
      sql.final(column: "name", value: sql.text("Alice")),
    ])
    |> sql.returning([sql.col("id"), sql.col("name")])
    |> sql.to_query(a())

  assert q.sql == "INSERT INTO users (name) VALUES ($1) RETURNING id, name"
  assert q.values == [sql.text("Alice")]
}

pub fn update_returning_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("name", sql.text("Bob"), of: sql.value)
    |> sql.where(sql.eq(sql.col("id"), sql.int(1), of: sql.value))
    |> sql.returning([sql.col("id"), sql.col("name")])
    |> sql.to_query(a())

  assert q.sql == "UPDATE users SET name = $1 WHERE id = $2 RETURNING id, name"
  assert q.values == [sql.text("Bob"), sql.int(1)]
}

pub fn delete_returning_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.delete()
    |> sql.where(sql.eq(sql.col("id"), sql.int(42), of: sql.value))
    |> sql.returning([sql.col("id")])
    |> sql.to_query(a())

  assert q.sql == "DELETE FROM users WHERE id = $1 RETURNING id"
  assert q.values == [sql.int(42)]
}

pub fn returning_to_string_test() {
  let s =
    sql.insert(into: sql.table("users"))
    |> sql.values([
      sql.final(column: "name", value: sql.text("Alice")),
    ])
    |> sql.returning([sql.col("id")])
    |> sql.to_string(a())

  assert s == "INSERT INTO users (name) VALUES ('Alice') RETURNING id"
}

// ---- ON CONFLICT Tests ----

pub fn on_conflict_do_nothing_test() {
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values([
      {
        use <- sql.field(column: "id", value: sql.int(1))
        sql.final(column: "name", value: sql.text("Alice"))
      },
    ])
    |> sql.on_conflict(target: "id", action: sql.DoNothing, where: [])
    |> sql.to_query(a())

  assert q.sql
    == "INSERT INTO users (id, name) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING"
  assert q.values == [sql.int(1), sql.text("Alice")]
}

pub fn on_conflict_do_update_test() {
  let q =
    sql.insert(into: sql.table("counts"))
    |> sql.values([
      {
        use <- sql.field(column: "id", value: sql.int(1))
        sql.final(column: "quantity", value: sql.int(10))
      },
    ])
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
  let q =
    sql.insert(into: sql.table("counts"))
    |> sql.values([
      {
        use <- sql.field(column: "id", value: sql.int(1))
        sql.final(column: "quantity", value: sql.int(10))
      },
    ])
    |> sql.on_conflict(
      target: "id",
      action: sql.DoUpdate(sets: [#("quantity", "excluded.quantity")]),
      where: [sql.gt(sql.col("quantity"), sql.int(5), of: sql.value)],
    )
    |> sql.to_query(a())

  assert q.sql
    == "INSERT INTO counts (id, quantity) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET quantity = excluded.quantity WHERE quantity > $3"
  assert q.values == [sql.int(1), sql.int(10), sql.int(5)]
}

pub fn on_conflict_do_nothing_returning_test() {
  let q =
    sql.insert(into: sql.table("counts"))
    |> sql.values([
      {
        use <- sql.field(column: "id", value: sql.int(1))
        sql.final(column: "quantity", value: sql.int(10))
      },
    ])
    |> sql.on_conflict(target: "id", action: sql.DoNothing, where: [])
    |> sql.returning([sql.col("id")])
    |> sql.to_query(a())

  assert q.sql
    == "INSERT INTO counts (id, quantity) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING RETURNING id"
  assert q.values == [sql.int(1), sql.int(10)]
}

pub fn on_conflict_to_string_test() {
  let s =
    sql.insert(into: sql.table("counts"))
    |> sql.values([
      {
        use <- sql.field(column: "id", value: sql.int(1))
        sql.final(column: "quantity", value: sql.int(10))
      },
    ])
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
  let s =
    sql.insert(into: sql.table("counts"))
    |> sql.values([
      {
        use <- sql.field(column: "id", value: sql.int(1))
        sql.final(column: "quantity", value: sql.int(10))
      },
    ])
    |> sql.on_conflict(
      target: "id",
      action: sql.DoUpdate(sets: [#("quantity", "excluded.quantity")]),
      where: [sql.gt(sql.col("quantity"), sql.int(5), of: sql.value)],
    )
    |> sql.to_string(a())

  assert s
    == "INSERT INTO counts (id, quantity) VALUES (1, 10) ON CONFLICT (id) DO UPDATE SET quantity = excluded.quantity WHERE quantity > 5"
}

// ---- Complex Query with New Features Test ----

pub fn complex_query_with_aggregates_having_test() {
  let q =
    sql.from(sql.table("employees"))
    |> sql.select([
      sql.col("department"),
      sql.count("*") |> sql.col_as("emp_count"),
      sql.sum("salary") |> sql.col_as("total_salary"),
    ])
    |> sql.where(sql.eq(sql.col("active"), sql.true, of: sql.value))
    |> sql.group_by([sql.col("department")])
    |> sql.having(sql.gt(sql.count("*"), sql.int(3), of: sql.value))
    |> sql.order_by(sql.col("department"), sql.asc)
    |> sql.limit(10)
    |> sql.to_query(a())

  assert q.sql
    == "SELECT department, COUNT(*) AS emp_count, SUM(salary) AS total_salary FROM employees WHERE active = $1 GROUP BY department HAVING COUNT(*) > $2 ORDER BY department ASC LIMIT 10"
  assert q.values == [sql.true, sql.int(3)]
}

// ---- UNION Tests ----

pub fn union_basic_test() {
  let q =
    sql.from(sql.table("employees"))
    |> sql.select([sql.col("name")])
    |> sql.union(
      sql.from(sql.table("contractors"))
      |> sql.select([sql.col("name")]),
    )
    |> sql.to_query(a())

  assert q.sql
    == "SELECT name FROM employees UNION SELECT name FROM contractors"
  assert q.values == []
}

pub fn union_all_basic_test() {
  let q =
    sql.from(sql.table("employees"))
    |> sql.select([sql.col("name")])
    |> sql.union_all(
      sql.from(sql.table("contractors"))
      |> sql.select([sql.col("name")]),
    )
    |> sql.to_query(a())

  assert q.sql
    == "SELECT name FROM employees UNION ALL SELECT name FROM contractors"
  assert q.values == []
}

pub fn union_three_way_test() {
  let q =
    sql.from(sql.table("employees"))
    |> sql.select([sql.col("name")])
    |> sql.union(
      sql.from(sql.table("contractors"))
      |> sql.select([sql.col("name")]),
    )
    |> sql.union(
      sql.from(sql.table("interns"))
      |> sql.select([sql.col("name")]),
    )
    |> sql.to_query(a())

  assert q.sql
    == "SELECT name FROM employees UNION SELECT name FROM contractors UNION SELECT name FROM interns"
  assert q.values == []
}

pub fn union_with_where_sequential_placeholders_test() {
  let q =
    sql.from(sql.table("employees"))
    |> sql.select([sql.col("name")])
    |> sql.where(sql.eq(
      sql.col("department"),
      sql.text("Engineering"),
      of: sql.value,
    ))
    |> sql.union(
      sql.from(sql.table("contractors"))
      |> sql.select([sql.col("name")])
      |> sql.where(sql.eq(
        sql.col("department"),
        sql.text("Engineering"),
        of: sql.value,
      )),
    )
    |> sql.to_query(a())

  assert q.sql
    == "SELECT name FROM employees WHERE department = $1 UNION SELECT name FROM contractors WHERE department = $2"
  assert q.values == [sql.text("Engineering"), sql.text("Engineering")]
}

pub fn union_multi_params_sequential_test() {
  let q =
    sql.from(sql.table("employees"))
    |> sql.select([sql.col("name")])
    |> sql.where(sql.eq(
      sql.col("department"),
      sql.text("Engineering"),
      of: sql.value,
    ))
    |> sql.where(sql.gt(sql.col("salary"), sql.int(50_000), of: sql.value))
    |> sql.union(
      sql.from(sql.table("contractors"))
      |> sql.select([sql.col("name")])
      |> sql.where(sql.eq(
        sql.col("department"),
        sql.text("Sales"),
        of: sql.value,
      )),
    )
    |> sql.to_query(a())

  assert q.sql
    == "SELECT name FROM employees WHERE (department = $1 AND salary > $2) UNION SELECT name FROM contractors WHERE department = $3"
  assert q.values
    == [sql.text("Engineering"), sql.int(50_000), sql.text("Sales")]
}

pub fn union_three_way_params_test() {
  let q =
    sql.from(sql.table("a"))
    |> sql.select([sql.star])
    |> sql.where(sql.eq(sql.col("x"), sql.int(1), of: sql.value))
    |> sql.union(
      sql.from(sql.table("b"))
      |> sql.select([sql.star])
      |> sql.where(sql.eq(sql.col("x"), sql.int(2), of: sql.value))
      |> sql.where(sql.eq(sql.col("y"), sql.int(3), of: sql.value)),
    )
    |> sql.union(
      sql.from(sql.table("c"))
      |> sql.select([sql.star])
      |> sql.where(sql.eq(sql.col("x"), sql.int(4), of: sql.value)),
    )
    |> sql.to_query(a())

  assert q.sql
    == "SELECT * FROM a WHERE x = $1 UNION SELECT * FROM b WHERE (x = $2 AND y = $3) UNION SELECT * FROM c WHERE x = $4"
  assert q.values == [sql.int(1), sql.int(2), sql.int(3), sql.int(4)]
}

pub fn union_to_string_test() {
  let s =
    sql.from(sql.table("employees"))
    |> sql.select([sql.col("name")])
    |> sql.where(sql.eq(
      sql.col("department"),
      sql.text("Engineering"),
      of: sql.value,
    ))
    |> sql.union(
      sql.from(sql.table("contractors"))
      |> sql.select([sql.col("name")])
      |> sql.where(sql.eq(
        sql.col("department"),
        sql.text("Engineering"),
        of: sql.value,
      )),
    )
    |> sql.to_string(a())

  assert s
    == "SELECT name FROM employees WHERE department = 'Engineering' UNION SELECT name FROM contractors WHERE department = 'Engineering'"
}

pub fn union_all_to_string_test() {
  let s =
    sql.from(sql.table("employees"))
    |> sql.select([sql.col("name")])
    |> sql.union_all(
      sql.from(sql.table("contractors"))
      |> sql.select([sql.col("name")]),
    )
    |> sql.to_string(a())

  assert s
    == "SELECT name FROM employees UNION ALL SELECT name FROM contractors"
}

pub fn union_three_way_to_string_test() {
  let s =
    sql.from(sql.table("a"))
    |> sql.select([sql.col("name")])
    |> sql.union(
      sql.from(sql.table("b"))
      |> sql.select([sql.col("name")]),
    )
    |> sql.union(
      sql.from(sql.table("c"))
      |> sql.select([sql.col("name")]),
    )
    |> sql.to_string(a())

  assert s
    == "SELECT name FROM a UNION SELECT name FROM b UNION SELECT name FROM c"
}

// ---- CTE Tests ----

pub fn cte_basic_to_query_test() {
  let active_users =
    sql.cte(
      name: "active_users",
      query: sql.from(sql.table("users"))
        |> sql.select([sql.col("id"), sql.col("name")])
        |> sql.where(sql.eq(sql.col("active"), sql.true, of: sql.value)),
    )

  let q =
    sql.from(sql.table("active_users"))
    |> sql.select([sql.col("name")])
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
        |> sql.select([sql.col("id"), sql.col("name")])
        |> sql.where(sql.eq(sql.col("active"), sql.true, of: sql.value)),
    )

  let s =
    sql.from(sql.table("active_users"))
    |> sql.select([sql.col("name")])
    |> sql.with(ctes: [active_users])
    |> sql.to_string(a())

  assert s
    == "WITH active_users AS (SELECT id, name FROM users WHERE active = TRUE) SELECT name FROM active_users;"
}

pub fn cte_multiple_test() {
  let active_users =
    sql.cte(
      name: "active_users",
      query: sql.from(sql.table("users"))
        |> sql.select([sql.col("id")])
        |> sql.where(sql.eq(sql.col("active"), sql.true, of: sql.value)),
    )

  let recent_orders =
    sql.cte(
      name: "recent_orders",
      query: sql.from(sql.table("orders"))
        |> sql.select([sql.col("user_id"), sql.col("total")])
        |> sql.where(sql.gt(sql.col("total"), sql.float(100.0), of: sql.value)),
    )

  let q =
    sql.from(sql.table("active_users"))
    |> sql.select([sql.col("id"), sql.col("total")])
    |> sql.join(sql.inner_join(
      table: sql.table("recent_orders"),
      on: sql.eq(
        sql.col("id") |> sql.col_for("active_users"),
        sql.col("user_id") |> sql.col_for("recent_orders"),
        of: sql.column,
      ),
    ))
    |> sql.with(ctes: [active_users, recent_orders])
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
        |> sql.select([sql.col("user_id"), sql.col("amount")]),
    )
    |> sql.cte_columns(columns: ["uid", "total"])

  let s =
    sql.from(sql.table("totals"))
    |> sql.select([sql.col("uid"), sql.col("total")])
    |> sql.with(ctes: [totals])
    |> sql.to_string(a())

  assert s
    == "WITH totals(uid, total) AS (SELECT user_id, amount FROM orders) SELECT uid, total FROM totals;"
}

pub fn cte_recursive_test() {
  let base =
    sql.from(sql.table("categories"))
    |> sql.select([sql.col("id"), sql.col("parent_id"), sql.col("name")])
    |> sql.where(sql.is_null(sql.col("parent_id")))

  let recursive_part =
    sql.from(sql.table_as(sql.table("categories"), "c"))
    |> sql.select([
      sql.col("id") |> sql.col_for("c"),
      sql.col("parent_id") |> sql.col_for("c"),
      sql.col("name") |> sql.col_for("c"),
    ])
    |> sql.join(sql.inner_join(
      table: sql.table_as(sql.table("category_tree"), "ct"),
      on: sql.eq(
        sql.col("parent_id") |> sql.col_for("c"),
        sql.col("id") |> sql.col_for("ct"),
        of: sql.column,
      ),
    ))

  let category_tree =
    sql.cte(name: "category_tree", query: base |> sql.union(recursive_part))

  let s =
    sql.from(sql.table("category_tree"))
    |> sql.select([sql.col("id"), sql.col("name")])
    |> sql.with(ctes: [category_tree])
    |> sql.recursive()
    |> sql.to_string(a())

  assert s
    == "WITH RECURSIVE category_tree AS (SELECT id, parent_id, name FROM categories WHERE parent_id IS NULL UNION SELECT c.id, c.parent_id, c.name FROM categories AS c INNER JOIN category_tree AS ct ON c.parent_id = ct.id) SELECT id, name FROM category_tree;"
}

pub fn cte_with_insert_test() {
  let new_users =
    sql.cte(
      name: "new_users",
      query: sql.from(sql.table("users"))
        |> sql.select([sql.col("id")])
        |> sql.where(sql.eq(sql.col("status"), sql.text("new"), of: sql.value)),
    )

  let q =
    sql.insert(into: sql.table("notifications"))
    |> sql.values([
      {
        use <- sql.field(column: "user_id", value: sql.int(1))
        sql.final(column: "message", value: sql.text("Welcome!"))
      },
    ])
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
        |> sql.select([sql.col("id")])
        |> sql.where(sql.lt(sql.col("score"), sql.int(10), of: sql.value)),
    )

  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("status", sql.text("inactive"), of: sql.value)
    |> sql.where(sql.in(sql.col("id"), [sql.int(1), sql.int(2)], of: sql.value))
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
        |> sql.select([sql.col("id")])
        |> sql.where(sql.lt(sql.col("year"), sql.int(2020), of: sql.value)),
    )

  let q =
    sql.from(sql.table("order_items"))
    |> sql.delete()
    |> sql.where(sql.in(
      sql.col("order_id"),
      [
        sql.int(100),
        sql.int(200),
      ],
      of: sql.value,
    ))
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
        |> sql.select([sql.col("id")])
        |> sql.where(sql.eq(sql.col("a"), sql.int(1), of: sql.value)),
    )

  let cte2 =
    sql.cte(
      name: "cte2",
      query: sql.from(sql.table("t2"))
        |> sql.select([sql.col("id")])
        |> sql.where(sql.eq(sql.col("b"), sql.int(2), of: sql.value)),
    )

  let q =
    sql.from(sql.table("cte1"))
    |> sql.select([sql.col("id")])
    |> sql.where(sql.eq(sql.col("c"), sql.int(3), of: sql.value))
    |> sql.with(ctes: [cte1, cte2])
    |> sql.to_query(a())

  // CTE1 body uses $1, CTE2 body uses $2, main query uses $3
  assert q.sql
    == "WITH cte1 AS (SELECT id FROM t1 WHERE a = $1), cte2 AS (SELECT id FROM t2 WHERE b = $2) SELECT id FROM cte1 WHERE c = $3;"
  assert q.values == [sql.Int(1), sql.Int(2), sql.Int(3)]
}

// ---- FOR UPDATE Tests ----

pub fn for_update_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where(sql.eq(sql.col("id"), sql.int(1), of: sql.value))
    |> sql.for_update
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE id = $1 FOR UPDATE"
  assert q.values == [sql.Int(1)]
}

pub fn for_update_with_order_by_and_limit_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id"), sql.col("name")])
    |> sql.where(sql.gt(sql.col("age"), sql.int(18), of: sql.value))
    |> sql.order_by(sql.col("name"), sql.asc)
    |> sql.limit(10)
    |> sql.for_update
    |> sql.to_query(a())

  assert q.sql
    == "SELECT id, name FROM users WHERE age > $1 ORDER BY name ASC LIMIT 10 FOR UPDATE"
  assert q.values == [sql.Int(18)]
}

pub fn for_update_with_join_test() {
  let q =
    sql.from(sql.table("users") |> sql.table_as("u"))
    |> sql.select([
      sql.col("id") |> sql.col_for("u"),
      sql.col("total") |> sql.col_for("o"),
    ])
    |> sql.join(sql.inner_join(
      table: sql.table("orders") |> sql.table_as("o"),
      on: sql.eq(
        sql.col("id") |> sql.col_for("u"),
        sql.col("user_id") |> sql.col_for("o"),
        of: sql.column,
      ),
    ))
    |> sql.where(sql.eq(
      sql.col("id") |> sql.col_for("u"),
      sql.int(1),
      of: sql.value,
    ))
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
    |> sql.where(sql.eq(sql.col("id"), sql.int(42), of: sql.value))
    |> sql.for_update
    |> sql.to_string(a())

  assert q == "SELECT * FROM users WHERE id = 42 FOR UPDATE"
}

// ============================================================
// Backtick Formatter Tests
// ============================================================

pub fn backtick_select_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id"), sql.col("name")])
    |> sql.where(sql.eq(sql.col("active"), sql.true, of: sql.value))
    |> sql.to_query(backtick_a())

  assert q.sql == "SELECT `id`, `name` FROM `users` WHERE `active` = ?"
  assert q.values == [sql.Bool(True)]
}

pub fn backtick_select_multiple_wheres_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id"), sql.col("name")])
    |> sql.where(sql.eq(sql.col("active"), sql.true, of: sql.value))
    |> sql.where(sql.gt(sql.col("age"), sql.int(18), of: sql.value))
    |> sql.to_query(backtick_a())

  assert q.sql
    == "SELECT `id`, `name` FROM `users` WHERE (`active` = ? AND `age` > ?)"
  assert q.values == [sql.Bool(True), sql.Int(18)]
}

pub fn backtick_insert_test() {
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values([
      {
        use <- sql.field(column: "name", value: sql.text("Alice"))
        sql.final(column: "email", value: sql.text("alice@example.com"))
      },
    ])
    |> sql.to_query(backtick_a())

  assert q.sql == "INSERT INTO `users` (`name`, `email`) VALUES (?, ?)"
  assert q.values == [sql.Text("Alice"), sql.Text("alice@example.com")]
}

pub fn backtick_update_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("name", sql.text("Bob"), of: sql.value)
    |> sql.where(sql.eq(sql.col("id"), sql.int(1), of: sql.value))
    |> sql.to_query(backtick_a())

  assert q.sql == "UPDATE `users` SET `name` = ? WHERE `id` = ?"
  assert q.values == [sql.Text("Bob"), sql.Int(1)]
}

pub fn backtick_delete_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.delete()
    |> sql.where(sql.eq(sql.col("id"), sql.int(1), of: sql.value))
    |> sql.to_query(backtick_a())

  assert q.sql == "DELETE FROM `users` WHERE `id` = ?"
  assert q.values == [sql.Int(1)]
}

pub fn backtick_union_test() {
  let q1 =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id")])
    |> sql.where(sql.eq(sql.col("active"), sql.true, of: sql.value))

  let q2 =
    sql.from(sql.table("admins"))
    |> sql.select([sql.col("id")])
    |> sql.where(sql.eq(sql.col("active"), sql.true, of: sql.value))

  let q =
    q1
    |> sql.union(q2)
    |> sql.to_query(backtick_a())

  assert q.sql
    == "SELECT `id` FROM `users` WHERE `active` = ? UNION SELECT `id` FROM `admins` WHERE `active` = ?"
  assert q.values == [sql.Bool(True), sql.Bool(True)]
}

pub fn backtick_to_string_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id"), sql.col("name")])
    |> sql.where(sql.eq(sql.col("active"), sql.true, of: sql.value))
    |> sql.to_string(backtick_a())

  assert q == "SELECT `id`, `name` FROM `users` WHERE `active` = TRUE"
}

pub fn backtick_aliased_identifiers_test() {
  let q =
    sql.from(sql.table("users") |> sql.table_as("u"))
    |> sql.select([
      sql.col("name") |> sql.col_for("u"),
      sql.col("total") |> sql.col_for("o") |> sql.col_as("order_total"),
    ])
    |> sql.join(sql.inner_join(
      table: sql.table("orders") |> sql.table_as("o"),
      on: sql.eq(
        sql.col("id") |> sql.col_for("u"),
        sql.col("user_id") |> sql.col_for("o"),
        of: sql.column,
      ),
    ))
    |> sql.where(sql.eq(
      sql.col("active") |> sql.col_for("u"),
      sql.true,
      of: sql.value,
    ))
    |> sql.to_query(backtick_a())

  assert q.sql
    == "SELECT `u`.`name`, `o`.`total` AS `order_total` FROM `users` AS `u` INNER JOIN `orders` AS `o` ON `u`.`id` = `o`.`user_id` WHERE `u`.`active` = ?"
  assert q.values == [sql.Bool(True)]
}

// ============================================================
// SELECT Tests
// ============================================================

pub fn select_not_like_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id"), sql.col("name")])
    |> sql.where(sql.not_like(
      sql.col("name"),
      sql.text("%admin%"),
      of: sql.value,
    ))
    |> sql.to_query(a())

  assert q.sql == "SELECT id, name FROM users WHERE name NOT LIKE $1"
  assert q.values == [sql.Text("%admin%")]
}

pub fn select_not_like_to_string_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id"), sql.col("name")])
    |> sql.where(sql.not_like(
      sql.col("name"),
      sql.text("%admin%"),
      of: sql.value,
    ))
    |> sql.to_string(a())

  assert q == "SELECT id, name FROM users WHERE name NOT LIKE '%admin%'"
}

pub fn select_not_between_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id"), sql.col("age")])
    |> sql.where(
      sql.not(sql.between(
        sql.col("age"),
        sql.int(18),
        sql.int(65),
        of: sql.value,
      )),
    )
    |> sql.to_query(a())

  assert q.sql == "SELECT id, age FROM users WHERE NOT (age BETWEEN $1 AND $2)"
  assert q.values == [sql.Int(18), sql.Int(65)]
}

pub fn select_not_between_to_string_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id"), sql.col("age")])
    |> sql.where(
      sql.not(sql.between(
        sql.col("age"),
        sql.int(18),
        sql.int(65),
        of: sql.value,
      )),
    )
    |> sql.to_string(a())

  assert q == "SELECT id, age FROM users WHERE NOT (age BETWEEN 18 AND 65)"
}

pub fn select_right_join_test() {
  let q =
    sql.from(sql.table("users") |> sql.table_as("u"))
    |> sql.select([
      sql.col("id") |> sql.col_for("u"),
      sql.col("order_id") |> sql.col_for("o"),
    ])
    |> sql.join(sql.right_join(
      table: sql.table("orders") |> sql.table_as("o"),
      on: sql.eq(
        sql.col("id") |> sql.col_for("u"),
        sql.col("user_id") |> sql.col_for("o"),
        of: sql.column,
      ),
    ))
    |> sql.to_query(a())

  assert q.sql
    == "SELECT u.id, o.order_id FROM users AS u RIGHT JOIN orders AS o ON u.id = o.user_id"
  assert q.values == []
}

pub fn select_full_join_test() {
  let q =
    sql.from(sql.table("users") |> sql.table_as("u"))
    |> sql.select([
      sql.col("id") |> sql.col_for("u"),
      sql.col("order_id") |> sql.col_for("o"),
    ])
    |> sql.join(sql.full_join(
      table: sql.table("orders") |> sql.table_as("o"),
      on: sql.eq(
        sql.col("id") |> sql.col_for("u"),
        sql.col("user_id") |> sql.col_for("o"),
        of: sql.column,
      ),
    ))
    |> sql.to_query(a())

  assert q.sql
    == "SELECT u.id, o.order_id FROM users AS u FULL JOIN orders AS o ON u.id = o.user_id"
  assert q.values == []
}

pub fn select_multiple_joins_test() {
  let q =
    sql.from(sql.table("users") |> sql.table_as("u"))
    |> sql.select([
      sql.col("id") |> sql.col_for("u"),
      sql.col("order_id") |> sql.col_for("o"),
      sql.col("product_name") |> sql.col_for("p"),
    ])
    |> sql.join(sql.inner_join(
      table: sql.table("orders") |> sql.table_as("o"),
      on: sql.eq(
        sql.col("id") |> sql.col_for("u"),
        sql.col("user_id") |> sql.col_for("o"),
        of: sql.column,
      ),
    ))
    |> sql.join(sql.left_join(
      table: sql.table("products") |> sql.table_as("p"),
      on: sql.eq(
        sql.col("product_id") |> sql.col_for("o"),
        sql.col("id") |> sql.col_for("p"),
        of: sql.column,
      ),
    ))
    |> sql.to_query(a())

  assert q.sql
    == "SELECT u.id, o.order_id, p.product_name FROM users AS u INNER JOIN orders AS o ON u.id = o.user_id LEFT JOIN products AS p ON o.product_id = p.id"
  assert q.values == []
}

pub fn select_join_with_and_conditions_test() {
  let q =
    sql.from(sql.table("users") |> sql.table_as("u"))
    |> sql.select([sql.col("id") |> sql.col_for("u")])
    |> sql.join(sql.inner_join(
      table: sql.table("orders") |> sql.table_as("o"),
      on: sql.and(
        sql.eq(
          sql.col("id") |> sql.col_for("u"),
          sql.col("user_id") |> sql.col_for("o"),
          of: sql.column,
        ),
        sql.eq(
          sql.col("status") |> sql.col_for("o"),
          sql.text("active"),
          of: sql.value,
        ),
      ),
    ))
    |> sql.to_query(a())

  assert q.sql
    == "SELECT u.id FROM users AS u INNER JOIN orders AS o ON (u.id = o.user_id AND o.status = $1)"
  assert q.values == [sql.Text("active")]
}

pub fn select_offset_without_limit_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id"), sql.col("name")])
    |> sql.offset(10)
    |> sql.to_query(a())

  assert q.sql == "SELECT id, name FROM users OFFSET 10"
  assert q.values == []
}

pub fn select_where_gt_lt_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id"), sql.col("name")])
    |> sql.where(sql.gt(sql.col("age"), sql.int(18), of: sql.value))
    |> sql.where(sql.lt(sql.col("age"), sql.int(65), of: sql.value))
    |> sql.to_query(a())

  assert q.sql == "SELECT id, name FROM users WHERE (age > $1 AND age < $2)"
  assert q.values == [sql.Int(18), sql.Int(65)]
}

pub fn select_where_gt_eq_lt_eq_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id")])
    |> sql.where(sql.gt_eq(sql.col("age"), sql.int(18), of: sql.value))
    |> sql.where(sql.lt_eq(sql.col("age"), sql.int(65), of: sql.value))
    |> sql.to_query(a())

  assert q.sql == "SELECT id FROM users WHERE (age >= $1 AND age <= $2)"
  assert q.values == [sql.Int(18), sql.Int(65)]
}

pub fn select_where_not_eq_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id"), sql.col("name")])
    |> sql.where(sql.not_eq(
      sql.col("status"),
      sql.text("banned"),
      of: sql.value,
    ))
    |> sql.to_query(a())

  assert q.sql == "SELECT id, name FROM users WHERE status != $1"
  assert q.values == [sql.Text("banned")]
}

pub fn select_complex_between_with_conditions_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id"), sql.col("name"), sql.col("age")])
    |> sql.where(sql.between(
      sql.col("age"),
      sql.int(18),
      sql.int(65),
      of: sql.value,
    ))
    |> sql.where(sql.eq(sql.col("active"), sql.true, of: sql.value))
    |> sql.to_query(a())

  assert q.sql
    == "SELECT id, name, age FROM users WHERE (age BETWEEN $1 AND $2 AND active = $3)"
  assert q.values == [sql.Int(18), sql.Int(65), sql.Bool(True)]
}

pub fn select_chained_three_wheres_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id")])
    |> sql.where(sql.eq(sql.col("a"), sql.int(1), of: sql.value))
    |> sql.where(sql.eq(sql.col("b"), sql.int(2), of: sql.value))
    |> sql.where(sql.eq(sql.col("c"), sql.int(3), of: sql.value))
    |> sql.to_query(a())

  assert q.sql == "SELECT id FROM users WHERE ((a = $1 AND b = $2) AND c = $3)"
  assert q.values == [sql.Int(1), sql.Int(2), sql.Int(3)]
}

pub fn select_where_not_eq_to_string_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id"), sql.col("name")])
    |> sql.where(sql.not_eq(
      sql.col("status"),
      sql.text("banned"),
      of: sql.value,
    ))
    |> sql.to_string(a())

  assert q == "SELECT id, name FROM users WHERE status != 'banned'"
}

pub fn select_date_to_string_test() {
  let d = calendar.Date(2024, calendar.January, 15)
  let q =
    sql.from(sql.table("events"))
    |> sql.select([sql.col("id")])
    |> sql.where(sql.eq(sql.col("event_date"), sql.date(d), of: sql.value))
    |> sql.to_string(sql.default_adapter())

  assert q == "SELECT id FROM events WHERE event_date = '2024-01-15'"
}

pub fn select_time_to_string_test() {
  let t = calendar.TimeOfDay(14, 30, 0, 0)
  let q =
    sql.from(sql.table("events"))
    |> sql.select([sql.col("id")])
    |> sql.where(sql.eq(sql.col("event_time"), sql.time(t), of: sql.value))
    |> sql.to_string(sql.default_adapter())

  assert q == "SELECT id FROM events WHERE event_time = '14:30:00'"
}

pub fn select_datetime_to_string_test() {
  let d = calendar.Date(2024, calendar.January, 15)
  let t = calendar.TimeOfDay(14, 30, 0, 0)
  let q =
    sql.from(sql.table("events"))
    |> sql.select([sql.col("id")])
    |> sql.where(sql.eq(sql.col("event_at"), sql.datetime(d, t), of: sql.value))
    |> sql.to_string(sql.default_adapter())

  assert q == "SELECT id FROM events WHERE event_at = '2024-01-15 14:30:00'"
}

pub fn select_like_to_string_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id"), sql.col("name")])
    |> sql.where(sql.like(sql.col("name"), sql.text("%alice%"), of: sql.value))
    |> sql.to_string(a())

  assert q == "SELECT id, name FROM users WHERE name LIKE '%alice%'"
}

pub fn select_in_to_string_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id"), sql.col("name")])
    |> sql.where(sql.in(
      sql.col("id"),
      [
        sql.int(1),
        sql.int(2),
        sql.int(3),
      ],
      of: sql.value,
    ))
    |> sql.to_string(a())

  assert q == "SELECT id, name FROM users WHERE id IN (1, 2, 3)"
}

pub fn select_is_null_to_string_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id"), sql.col("name")])
    |> sql.where(sql.is_null(sql.col("deleted_at")))
    |> sql.to_string(a())

  assert q == "SELECT id, name FROM users WHERE deleted_at IS NULL"
}

pub fn select_is_not_null_to_string_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id"), sql.col("name")])
    |> sql.where(sql.is_not_null(sql.col("email")))
    |> sql.to_string(a())

  assert q == "SELECT id, name FROM users WHERE email IS NOT NULL"
}

pub fn select_between_to_string_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id"), sql.col("age")])
    |> sql.where(sql.between(
      sql.col("age"),
      sql.int(18),
      sql.int(65),
      of: sql.value,
    ))
    |> sql.to_string(a())

  assert q == "SELECT id, age FROM users WHERE age BETWEEN 18 AND 65"
}

pub fn select_or_where_to_string_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id")])
    |> sql.where(sql.eq(sql.col("active"), sql.true, of: sql.value))
    |> sql.or_where(sql.eq(sql.col("role"), sql.text("admin"), of: sql.value))
    |> sql.to_string(a())

  assert q == "SELECT id FROM users WHERE (active = TRUE OR role = 'admin')"
}

pub fn select_join_to_string_test() {
  let q =
    sql.from(sql.table("users") |> sql.table_as("u"))
    |> sql.select([
      sql.col("id") |> sql.col_for("u"),
      sql.col("total") |> sql.col_for("o"),
    ])
    |> sql.join(sql.inner_join(
      table: sql.table("orders") |> sql.table_as("o"),
      on: sql.eq(
        sql.col("id") |> sql.col_for("u"),
        sql.col("user_id") |> sql.col_for("o"),
        of: sql.column,
      ),
    ))
    |> sql.to_string(a())

  assert q
    == "SELECT u.id, o.total FROM users AS u INNER JOIN orders AS o ON u.id = o.user_id"
}

pub fn select_group_by_having_to_string_test() {
  let q =
    sql.from(sql.table("employees"))
    |> sql.select([sql.col("department"), sql.count("*") |> sql.col_as("cnt")])
    |> sql.group_by([sql.col("department")])
    |> sql.having(sql.gt(sql.count("*"), sql.int(5), of: sql.value))
    |> sql.to_string(a())

  assert q
    == "SELECT department, COUNT(*) AS cnt FROM employees GROUP BY department HAVING COUNT(*) > 5"
}

pub fn select_order_by_to_string_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id"), sql.col("name")])
    |> sql.order_by(sql.col("name"), sql.asc)
    |> sql.order_by(sql.col("id"), sql.desc)
    |> sql.to_string(a())

  assert q == "SELECT id, name FROM users ORDER BY name ASC, id DESC"
}

pub fn select_limit_offset_to_string_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id")])
    |> sql.limit(10)
    |> sql.offset(20)
    |> sql.to_string(a())

  assert q == "SELECT id FROM users LIMIT 10 OFFSET 20"
}

pub fn select_distinct_to_query_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("name")])
    |> sql.distinct
    |> sql.to_query(a())

  assert q.sql == "SELECT DISTINCT name FROM users"
  assert q.values == []
}

pub fn select_where_and_or_combined_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id")])
    |> sql.where(sql.or(
      sql.eq(sql.col("role"), sql.text("admin"), of: sql.value),
      sql.and(
        sql.eq(sql.col("active"), sql.true, of: sql.value),
        sql.gt(sql.col("age"), sql.int(18), of: sql.value),
      ),
    ))
    |> sql.to_query(a())

  assert q.sql
    == "SELECT id FROM users WHERE (role = $1 OR (active = $2 AND age > $3))"
  assert q.values == [sql.Text("admin"), sql.Bool(True), sql.Int(18)]
}

pub fn select_where_not_is_null_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id")])
    |> sql.where(sql.not(sql.is_null(sql.col("email"))))
    |> sql.to_query(a())

  assert q.sql == "SELECT id FROM users WHERE NOT (email IS NULL)"
  assert q.values == []
}

pub fn select_multiple_joins_right_full_test() {
  let q =
    sql.from(sql.table("users") |> sql.table_as("u"))
    |> sql.select([
      sql.col("id") |> sql.col_for("u"),
      sql.col("oid") |> sql.col_for("o"),
      sql.col("pid") |> sql.col_for("p"),
    ])
    |> sql.join(sql.right_join(
      table: sql.table("orders") |> sql.table_as("o"),
      on: sql.eq(
        sql.col("id") |> sql.col_for("u"),
        sql.col("user_id") |> sql.col_for("o"),
        of: sql.column,
      ),
    ))
    |> sql.join(sql.full_join(
      table: sql.table("products") |> sql.table_as("p"),
      on: sql.eq(
        sql.col("product_id") |> sql.col_for("o"),
        sql.col("id") |> sql.col_for("p"),
        of: sql.column,
      ),
    ))
    |> sql.to_query(a())

  assert q.sql
    == "SELECT u.id, o.oid, p.pid FROM users AS u RIGHT JOIN orders AS o ON u.id = o.user_id FULL JOIN products AS p ON o.product_id = p.id"
  assert q.values == []
}

// ============================================================
// INSERT Tests
// ============================================================

pub fn insert_with_null_test() {
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values([
      {
        use <- sql.field(column: "name", value: sql.text("Alice"))
        sql.final(column: "email", value: sql.null)
      },
    ])
    |> sql.to_query(a())

  assert q.sql == "INSERT INTO users (name, email) VALUES ($1, $2)"
  assert q.values == [sql.Text("Alice"), sql.Null]
}

pub fn insert_with_null_to_string_test() {
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values([
      {
        use <- sql.field(column: "name", value: sql.text("Alice"))
        sql.final(column: "email", value: sql.null)
      },
    ])
    |> sql.to_string(a())

  assert q == "INSERT INTO users (name, email) VALUES ('Alice', NULL)"
}

pub fn insert_mixed_types_test() {
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values([
      {
        use <- sql.field(column: "name", value: sql.text("Alice"))
        use <- sql.field(column: "age", value: sql.int(30))
        use <- sql.field(column: "score", value: sql.float(9.5))
        sql.final(column: "active", value: sql.true)
      },
    ])
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
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values([
      {
        use <- sql.field(column: "name", value: sql.text("Alice"))
        use <- sql.field(column: "age", value: sql.int(30))
        use <- sql.field(column: "score", value: sql.float(9.5))
        sql.final(column: "active", value: sql.true)
      },
    ])
    |> sql.to_string(a())

  assert q
    == "INSERT INTO users (name, age, score, active) VALUES ('Alice', 30, 9.5, TRUE)"
}

pub fn insert_multiple_rows_to_string_test() {
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values([
      {
        use <- sql.field(column: "name", value: sql.text("Alice"))
        sql.final(column: "age", value: sql.int(30))
      },
      {
        use <- sql.field(column: "name", value: sql.text("Bob"))
        sql.final(column: "age", value: sql.int(25))
      },
    ])
    |> sql.to_string(a())

  assert q == "INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25)"
}

pub fn insert_on_conflict_do_nothing_backtick_test() {
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values([
      {
        use <- sql.field(column: "name", value: sql.text("Alice"))
        sql.final(column: "email", value: sql.text("alice@example.com"))
      },
    ])
    |> sql.on_conflict(target: "email", action: sql.DoNothing, where: [])
    |> sql.to_query(backtick_a())

  assert q.sql
    == "INSERT INTO `users` (`name`, `email`) VALUES (?, ?) ON CONFLICT (`email`) DO NOTHING"
  assert q.values == [sql.Text("Alice"), sql.Text("alice@example.com")]
}

pub fn insert_returning_backtick_test() {
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values([
      {
        use <- sql.field(column: "name", value: sql.text("Alice"))
        sql.final(column: "email", value: sql.text("alice@example.com"))
      },
    ])
    |> sql.returning([sql.col("id")])
    |> sql.to_query(backtick_a())

  assert q.sql
    == "INSERT INTO `users` (`name`, `email`) VALUES (?, ?) RETURNING `id`"
  assert q.values == [sql.Text("Alice"), sql.Text("alice@example.com")]
}

// ============================================================
// UPDATE Tests
// ============================================================

pub fn update_where_not_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("active", sql.false, of: sql.value)
    |> sql.where(
      sql.not(sql.eq(sql.col("role"), sql.text("admin"), of: sql.value)),
    )
    |> sql.to_query(a())

  assert q.sql == "UPDATE users SET active = $1 WHERE NOT (role = $2)"
  assert q.values == [sql.Bool(False), sql.Text("admin")]
}

pub fn update_where_like_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("category", sql.text("vip"), of: sql.value)
    |> sql.where(sql.like(
      sql.col("email"),
      sql.text("%@company.com"),
      of: sql.value,
    ))
    |> sql.to_query(a())

  assert q.sql == "UPDATE users SET category = $1 WHERE email LIKE $2"
  assert q.values == [sql.Text("vip"), sql.Text("%@company.com")]
}

pub fn update_where_not_like_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("category", sql.text("standard"), of: sql.value)
    |> sql.where(sql.not_like(
      sql.col("email"),
      sql.text("%@company.com"),
      of: sql.value,
    ))
    |> sql.to_query(a())

  assert q.sql == "UPDATE users SET category = $1 WHERE email NOT LIKE $2"
  assert q.values == [sql.Text("standard"), sql.Text("%@company.com")]
}

pub fn update_multiple_sets_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("name", sql.text("Bob"), of: sql.value)
    |> sql.set("age", sql.int(30), of: sql.value)
    |> sql.set("active", sql.true, of: sql.value)
    |> sql.where(sql.eq(sql.col("id"), sql.int(1), of: sql.value))
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
    |> sql.set("name", sql.text("Bob"), of: sql.value)
    |> sql.set("age", sql.int(30), of: sql.value)
    |> sql.where(sql.eq(sql.col("id"), sql.int(1), of: sql.value))
    |> sql.to_string(a())

  assert q == "UPDATE users SET name = 'Bob', age = 30 WHERE id = 1"
}

pub fn update_where_not_to_string_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("active", sql.false, of: sql.value)
    |> sql.where(
      sql.not(sql.eq(sql.col("role"), sql.text("admin"), of: sql.value)),
    )
    |> sql.to_string(a())

  assert q == "UPDATE users SET active = FALSE WHERE NOT (role = 'admin')"
}

pub fn update_returning_to_string_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("active", sql.true, of: sql.value)
    |> sql.where(sql.eq(sql.col("id"), sql.int(1), of: sql.value))
    |> sql.returning([sql.col("id"), sql.col("active")])
    |> sql.to_string(a())

  assert q == "UPDATE users SET active = TRUE WHERE id = 1 RETURNING id, active"
}

pub fn update_backtick_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("name", sql.text("Bob"), of: sql.value)
    |> sql.where(sql.eq(sql.col("id"), sql.int(1), of: sql.value))
    |> sql.to_query(backtick_a())

  assert q.sql == "UPDATE `users` SET `name` = ? WHERE `id` = ?"
  assert q.values == [sql.Text("Bob"), sql.Int(1)]
}

// ============================================================
// DELETE Tests
// ============================================================

pub fn delete_where_not_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.delete()
    |> sql.where(
      sql.not(sql.eq(sql.col("role"), sql.text("admin"), of: sql.value)),
    )
    |> sql.to_query(a())

  assert q.sql == "DELETE FROM users WHERE NOT (role = $1)"
  assert q.values == [sql.Text("admin")]
}

pub fn delete_where_like_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.delete()
    |> sql.where(sql.like(
      sql.col("email"),
      sql.text("%@spam.com"),
      of: sql.value,
    ))
    |> sql.to_query(a())

  assert q.sql == "DELETE FROM users WHERE email LIKE $1"
  assert q.values == [sql.Text("%@spam.com")]
}

pub fn delete_where_not_like_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.delete()
    |> sql.where(sql.not_like(
      sql.col("email"),
      sql.text("%@company.com"),
      of: sql.value,
    ))
    |> sql.to_query(a())

  assert q.sql == "DELETE FROM users WHERE email NOT LIKE $1"
  assert q.values == [sql.Text("%@company.com")]
}

pub fn delete_chained_wheres_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.delete()
    |> sql.where(sql.eq(sql.col("active"), sql.false, of: sql.value))
    |> sql.where(sql.lt(sql.col("age"), sql.int(18), of: sql.value))
    |> sql.to_query(a())

  assert q.sql == "DELETE FROM users WHERE (active = $1 AND age < $2)"
  assert q.values == [sql.Bool(False), sql.Int(18)]
}

pub fn delete_to_string_with_date_test() {
  let d = calendar.Date(2024, calendar.January, 1)
  let q =
    sql.from(sql.table("events"))
    |> sql.delete()
    |> sql.where(sql.lt(sql.col("event_date"), sql.date(d), of: sql.value))
    |> sql.to_string(sql.default_adapter())

  assert q == "DELETE FROM events WHERE event_date < '2024-01-01'"
}

pub fn delete_where_not_to_string_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.delete()
    |> sql.where(
      sql.not(sql.eq(sql.col("role"), sql.text("admin"), of: sql.value)),
    )
    |> sql.to_string(a())

  assert q == "DELETE FROM users WHERE NOT (role = 'admin')"
}

pub fn delete_returning_to_string_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.delete()
    |> sql.where(sql.eq(sql.col("id"), sql.int(1), of: sql.value))
    |> sql.returning([sql.col("id")])
    |> sql.to_string(a())

  assert q == "DELETE FROM users WHERE id = 1 RETURNING id"
}

pub fn delete_backtick_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.delete()
    |> sql.where(sql.eq(sql.col("id"), sql.int(1), of: sql.value))
    |> sql.to_query(backtick_a())

  assert q.sql == "DELETE FROM `users` WHERE `id` = ?"
  assert q.values == [sql.Int(1)]
}

// ============================================================
// Union additional tests
// ============================================================

pub fn union_all_backtick_test() {
  let q1 =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id")])

  let q2 =
    sql.from(sql.table("admins"))
    |> sql.select([sql.col("id")])

  let q =
    q1
    |> sql.union_all(q2)
    |> sql.to_query(backtick_a())

  assert q.sql == "SELECT `id` FROM `users` UNION ALL SELECT `id` FROM `admins`"
  assert q.values == []
}

pub fn union_with_limit_offset_test() {
  // Note: union currently doesn't support limit/offset on the combined result,
  // but individual selects can have them
  let q1 =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id")])
    |> sql.limit(5)

  let q2 =
    sql.from(sql.table("admins"))
    |> sql.select([sql.col("id")])
    |> sql.limit(3)

  let q =
    q1
    |> sql.union(q2)
    |> sql.to_query(a())

  assert q.sql
    == "SELECT id FROM users LIMIT 5 UNION SELECT id FROM admins LIMIT 3"
  assert q.values == []
}

pub fn union_to_string_with_values_test() {
  let q1 =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id"), sql.col("name")])
    |> sql.where(sql.eq(sql.col("active"), sql.true, of: sql.value))

  let q2 =
    sql.from(sql.table("admins"))
    |> sql.select([sql.col("id"), sql.col("name")])
    |> sql.where(sql.eq(sql.col("active"), sql.true, of: sql.value))

  let q =
    q1
    |> sql.union(q2)
    |> sql.to_string(a())

  assert q
    == "SELECT id, name FROM users WHERE active = TRUE UNION SELECT id, name FROM admins WHERE active = TRUE"
}

// ============================================================
// CTE additional tests
// ============================================================

pub fn cte_with_join_test() {
  let cte_query =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id"), sql.col("name")])
    |> sql.where(sql.eq(sql.col("active"), sql.true, of: sql.value))

  let q =
    sql.from(sql.table("active_users") |> sql.table_as("au"))
    |> sql.select([
      sql.col("id") |> sql.col_for("au"),
      sql.col("total") |> sql.col_for("o"),
    ])
    |> sql.join(sql.inner_join(
      table: sql.table("orders") |> sql.table_as("o"),
      on: sql.eq(
        sql.col("id") |> sql.col_for("au"),
        sql.col("user_id") |> sql.col_for("o"),
        of: sql.column,
      ),
    ))
    |> sql.with([sql.cte(name: "active_users", query: cte_query)])
    |> sql.to_query(a())

  assert q.sql
    == "WITH active_users AS (SELECT id, name FROM users WHERE active = $1) SELECT au.id, o.total FROM active_users AS au INNER JOIN orders AS o ON au.id = o.user_id;"
  assert q.values == [sql.Bool(True)]
}

pub fn cte_with_union_body_test() {
  let cte_query1 =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id"), sql.col("name")])
    |> sql.where(sql.eq(sql.col("active"), sql.true, of: sql.value))

  // NOTE: CTE bodies must be QueryBuilder(Select, v), so union as CTE body
  // would require CombinedSelectBuilder support in Cte — this may need
  // to be tested differently if the type doesn't allow it.
  // For now, test a simple CTE with the main query being a union.

  let main1 =
    sql.from(sql.table("active_users"))
    |> sql.select([sql.col("id"), sql.col("name")])

  let main2 =
    sql.from(sql.table("admins"))
    |> sql.select([sql.col("id"), sql.col("name")])

  let q =
    main1
    |> sql.union(main2)
    |> sql.with([sql.cte(name: "active_users", query: cte_query1)])
    |> sql.to_query(a())

  assert q.sql
    == "WITH active_users AS (SELECT id, name FROM users WHERE active = $1) SELECT id, name FROM active_users UNION SELECT id, name FROM admins;"
  assert q.values == [sql.Bool(True)]
}

pub fn cte_basic_to_string_with_values_test() {
  let cte_query =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id"), sql.col("name")])
    |> sql.where(sql.eq(sql.col("active"), sql.true, of: sql.value))

  let q =
    sql.from(sql.table("active_users"))
    |> sql.select([sql.col("id"), sql.col("name")])
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
    |> sql.select([sql.col("id"), sql.col("parent_id"), sql.col("name")])
    |> sql.where(sql.is_null(sql.col("parent_id")))

  let q =
    sql.from(sql.table("category_tree"))
    |> sql.select([sql.col("id"), sql.col("parent_id"), sql.col("name")])
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
    |> sql.select([sql.col("id"), sql.col("name")])
    |> sql.where(sql.eq(sql.col("active"), sql.true, of: sql.value))

  let cte2 =
    sql.from(sql.table("orders"))
    |> sql.select([sql.col("user_id"), sql.sum("amount") |> sql.col_as("total")])
    |> sql.group_by([sql.col("user_id")])

  let q =
    sql.from(sql.table("active_users") |> sql.table_as("au"))
    |> sql.select([
      sql.col("name") |> sql.col_for("au"),
      sql.col("total") |> sql.col_for("uo"),
    ])
    |> sql.join(sql.inner_join(
      table: sql.table("user_orders") |> sql.table_as("uo"),
      on: sql.eq(
        sql.col("id") |> sql.col_for("au"),
        sql.col("user_id") |> sql.col_for("uo"),
        of: sql.column,
      ),
    ))
    |> sql.with([
      sql.cte(name: "active_users", query: cte1),
      sql.cte(name: "user_orders", query: cte2),
    ])
    |> sql.to_string(a())

  assert q
    == "WITH active_users AS (SELECT id, name FROM users WHERE active = TRUE), user_orders AS (SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id) SELECT au.name, uo.total FROM active_users AS au INNER JOIN user_orders AS uo ON au.id = uo.user_id;"
}

// ============================================================
// Additional edge cases
// ============================================================

pub fn select_all_with_where_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where(sql.eq(sql.col("id"), sql.int(1), of: sql.value))
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE id = $1"
  assert q.values == [sql.Int(1)]
}

pub fn select_count_star_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.count("*")])
    |> sql.to_query(a())

  assert q.sql == "SELECT COUNT(*) FROM users"
  assert q.values == []
}

pub fn select_count_star_with_alias_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.count("*") |> sql.col_as("total")])
    |> sql.to_query(a())

  assert q.sql == "SELECT COUNT(*) AS total FROM users"
  assert q.values == []
}

pub fn insert_single_row_to_string_test() {
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values([
      {
        use <- sql.field(column: "name", value: sql.text("Alice"))
        sql.final(column: "email", value: sql.text("alice@example.com"))
      },
    ])
    |> sql.to_string(a())

  assert q
    == "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')"
}

pub fn insert_with_boolean_to_string_test() {
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values([
      {
        use <- sql.field(column: "name", value: sql.text("Alice"))
        sql.final(column: "active", value: sql.true)
      },
    ])
    |> sql.to_string(a())

  assert q == "INSERT INTO users (name, active) VALUES ('Alice', TRUE)"
}

pub fn select_from_aliased_table_test() {
  let q =
    sql.from(sql.table("users") |> sql.table_as("u"))
    |> sql.select([sql.col("id") |> sql.col_for("u")])
    |> sql.to_query(a())

  assert q.sql == "SELECT u.id FROM users AS u"
  assert q.values == []
}

pub fn select_where_in_multiple_values_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id"), sql.col("name")])
    |> sql.where(sql.in(
      sql.col("status"),
      [
        sql.text("active"),
        sql.text("pending"),
        sql.text("verified"),
      ],
      of: sql.value,
    ))
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
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values([
      {
        use <- sql.field(column: "name", value: sql.text("Alice"))
        sql.final(column: "email", value: sql.text("alice@example.com"))
      },
    ])
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
  let q =
    sql.insert(into: sql.table("users"))
    |> sql.values([
      {
        use <- sql.field(column: "name", value: sql.text("Alice"))
        sql.final(column: "email", value: sql.text("alice@example.com"))
      },
    ])
    |> sql.on_conflict(target: "email", action: sql.DoNothing, where: [])
    |> sql.to_string(a())

  assert q
    == "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com') ON CONFLICT (email) DO NOTHING"
}

// ============================================================
// TODO: Tests for features that don't exist yet
// These are commented out because they reference APIs or produce
// SQL that our current implementation doesn't support.
// Uncomment and fix as features are added.
// ============================================================

// --- Subquery in WHERE ---
// TODO: requires subquery support (e.g. sql.sub_query or sql.SubQuery operand)
pub fn select_where_subquery_test() {
  let sub =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id")])
    |> sql.where(sql.eq(sql.col("name"), sql.text("Alice"), of: sql.value))

  let q =
    sql.from(sql.table("orders"))
    |> sql.select([sql.star])
    |> sql.where(sql.eq(sql.col("user_id"), sub, of: sql.subquery))
    |> sql.to_query(a())

  assert q.sql
    == "SELECT * FROM orders WHERE user_id = (SELECT id FROM users WHERE name = $1)"
  assert q.values == [sql.Text("Alice")]
}

// --- FROM subquery ---
pub fn select_from_subqueryquery_test() {
  let sub =
    sql.from(sql.table("orders"))
    |> sql.select([sql.col("user_id"), sql.sum("amount") |> sql.col_as("total")])
    |> sql.group_by([sql.col("user_id")])

  let q =
    sql.from_subquery(sub, "order_totals")
    |> sql.select([sql.col("user_id"), sql.col("total")])
    |> sql.where(sql.gt(sql.col("total"), sql.int(100), of: sql.value))
    |> sql.to_query(a())

  assert q.sql
    == "SELECT user_id, total FROM (SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id) AS order_totals WHERE total > $1"
  assert q.values == [sql.Int(100)]
}

// --- EXISTS subquery condition ---
pub fn select_where_exists_test() {
  let sub =
    sql.from(sql.table("orders"))
    |> sql.select([sql.col("id")])
    |> sql.where(sql.eq(
      sql.col("user_id") |> sql.col_for("orders"),
      sql.col("id") |> sql.col_for("users"),
      of: sql.column,
    ))

  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where(sql.exists(sub))
    |> sql.to_query(a())

  assert q.sql
    == "SELECT * FROM users WHERE EXISTS (SELECT id FROM orders WHERE orders.user_id = users.id)"
}

// --- ANY subquery condition ---
pub fn select_where_any_test() {
  let sub =
    sql.from(sql.table("orders"))
    |> sql.select([sql.col("user_id")])

  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where(sql.eq(sql.col("id"), sub, of: sql.any))
    |> sql.to_query(a())

  assert q.sql
    == "SELECT * FROM users WHERE id = ANY (SELECT user_id FROM orders)"
}

// --- ALL subquery condition ---
pub fn select_where_all_test() {
  let sub =
    sql.from(sql.table("requirements"))
    |> sql.select([sql.col("min_age")])

  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where(sql.gt(sql.col("age"), sub, of: sql.all))
    |> sql.to_query(a())

  assert q.sql
    == "SELECT * FROM users WHERE age > ALL (SELECT min_age FROM requirements)"
}

// --- IS TRUE / IS FALSE conditions ---

pub fn select_where_is_true_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id")])
    |> sql.where(sql.is_true(sql.col("active")))
    |> sql.to_query(a())

  assert q.sql == "SELECT id FROM users WHERE active IS TRUE"
}

pub fn select_where_is_false_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id")])
    |> sql.where(sql.is_false(sql.col("active")))
    |> sql.to_query(a())

  assert q.sql == "SELECT id FROM users WHERE active IS FALSE"
}

// --- Raw SQL condition ---

pub fn select_where_raw_sql_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where(sql.raw("age > 18 AND active = true"))
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE age > 18 AND active = true"
  assert q.values == []
}

pub fn select_where_raw_sql_with_values_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where(
      sql.raw_with_values("age > ? AND active = ?", [sql.int(18), sql.true]),
    )
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE age > $1 AND active = $2"
  assert q.values == [sql.Int(18), sql.Bool(True)]
}

pub fn select_where_raw_sql_to_string_test() {
  let s =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where(sql.raw("age > 18 AND active = true"))
    |> sql.to_string(a())

  assert s == "SELECT * FROM users WHERE age > 18 AND active = true"
}

pub fn select_where_raw_sql_with_values_to_string_test() {
  let s =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where(
      sql.raw_with_values("age > ? AND active = ?", [sql.int(18), sql.true]),
    )
    |> sql.to_string(a())

  assert s == "SELECT * FROM users WHERE age > 18 AND active = TRUE"
}

pub fn select_where_raw_combined_with_regular_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where(sql.eq(sql.col("name"), sql.text("Alice"), of: sql.value))
    |> sql.where(sql.raw_with_values("age > ?", [sql.int(18)]))
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE (name = $1 AND age > $2)"
  assert q.values == [sql.Text("Alice"), sql.Int(18)]
}

pub fn delete_where_raw_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.delete()
    |> sql.where(
      sql.raw_with_values("active = ? AND age > ?", [sql.false, sql.int(21)]),
    )
    |> sql.to_query(a())

  assert q.sql == "DELETE FROM users WHERE active = $1 AND age > $2"
  assert q.values == [sql.Bool(False), sql.Int(21)]
}

pub fn update_where_raw_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("status", sql.text("inactive"), of: sql.value)
    |> sql.where(
      sql.raw_with_values("age > ? AND active = ?", [sql.int(65), sql.true]),
    )
    |> sql.to_query(a())

  assert q.sql == "UPDATE users SET status = $1 WHERE age > $2 AND active = $3"
  assert q.values == [sql.Text("inactive"), sql.Int(65), sql.Bool(True)]
}

// --- where_not convenience function ---

pub fn select_where_not_convenience_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where_not(sql.eq(sql.col("active"), sql.false, of: sql.value))
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE NOT (active = $1)"
  assert q.values == [sql.Bool(False)]
}

pub fn select_where_not_to_string_convenience_test() {
  let s =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where_not(sql.eq(sql.col("active"), sql.false, of: sql.value))
    |> sql.to_string(a())

  assert s == "SELECT * FROM users WHERE NOT (active = FALSE)"
}

// --- where_exists convenience function ---

pub fn select_where_exists_convenience_test() {
  let subquery =
    sql.from(sql.table("orders"))
    |> sql.select([sql.col("id")])
    |> sql.where(sql.eq(
      sql.col("user_id") |> sql.col_for("orders"),
      sql.col("id") |> sql.col_for("users"),
      of: sql.column,
    ))

  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where_exists(subquery)
    |> sql.to_query(a())

  assert q.sql
    == "SELECT * FROM users WHERE EXISTS (SELECT id FROM orders WHERE orders.user_id = users.id)"
  assert q.values == []
}

// --- nullable Kind wrapper ---

pub fn select_where_nullable_some_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where(sql.eq(
      sql.col("age"),
      Some(sql.int(25)),
      of: sql.nullable(of: sql.value),
    ))
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE age = $1"
  assert q.values == [sql.Int(25)]
}

pub fn select_where_nullable_none_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where(sql.eq(sql.col("age"), None, of: sql.nullable(of: sql.value)))
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE age = $1"
  assert q.values == [sql.Null]
}

pub fn select_where_nullable_none_to_string_test() {
  let s =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where(sql.eq(sql.col("age"), None, of: sql.nullable(of: sql.value)))
    |> sql.to_string(a())

  assert s == "SELECT * FROM users WHERE age = NULL"
}

// --- list Kind constructor ---

pub fn select_where_in_list_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where(sql.in(
      sql.col("id"),
      [1, 2, 3],
      of: sql.list(of: fn(i) { sql.int(i) }),
    ))
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE id IN ($1, $2, $3)"
  assert q.values == [sql.Int(1), sql.Int(2), sql.Int(3)]
}

pub fn select_where_in_list_strings_test() {
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where(sql.in(
      sql.col("name"),
      ["Alice", "Bob"],
      of: sql.list(of: fn(s) { sql.text(s) }),
    ))
    |> sql.to_query(a())

  assert q.sql == "SELECT * FROM users WHERE name IN ($1, $2)"
  assert q.values == [sql.Text("Alice"), sql.Text("Bob")]
}

pub fn select_where_in_list_to_string_test() {
  let s =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where(sql.in(
      sql.col("id"),
      [1, 2, 3],
      of: sql.list(of: fn(i) { sql.int(i) }),
    ))
    |> sql.to_string(a())

  assert s == "SELECT * FROM users WHERE id IN (1, 2, 3)"
}

// --- default_adapter test ---

pub fn default_adapter_test() {
  let r = sql.default_adapter()
  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("name")])
    |> sql.where(sql.eq(sql.col("active"), sql.true, of: sql.value))
    |> sql.to_query(r)

  assert q.sql == "SELECT name FROM users WHERE active = $1"
  assert q.values == [sql.true]
}

// --- Mapper tests ---

pub fn mapper_handle_null_test() {
  let r =
    sql.adapter()
    |> sql.on_null(with: fn() { sql.null })
    |> sql.on_int(with: fn(i) { sql.int(i) })
    |> sql.on_text(with: fn(s) { sql.text(s) })
    |> sql.on_value(with: sql_value_to_string)

  let q =
    sql.from(sql.table("users"))
    |> sql.select([sql.star])
    |> sql.where(sql.eq(sql.col("name"), None, of: sql.nullable(of: sql.value)))
    |> sql.to_query(r)

  assert q.sql == "SELECT * FROM users WHERE name = $1"
  assert q.values == [sql.Null]
}

// --- UPDATE with ORDER BY ---

pub fn update_with_order_by_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("active", sql.false, of: sql.value)
    |> sql.order_by(sql.col("created_at"), sql.asc)
    |> sql.to_query(a())

  assert q.sql == "UPDATE users SET active = $1 ORDER BY created_at ASC"
  assert q.values == [sql.Bool(False)]
}

// --- UPDATE with LIMIT ---

pub fn update_with_limit_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("active", sql.false, of: sql.value)
    |> sql.limit(10)
    |> sql.to_query(a())

  assert q.sql == "UPDATE users SET active = $1 LIMIT 10"
  assert q.values == [sql.Bool(False)]
}

// --- UPDATE with ORDER BY + LIMIT + RETURNING ---

pub fn update_with_order_by_limit_returning_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("active", sql.false, of: sql.value)
    |> sql.order_by(sql.col("created_at"), sql.asc)
    |> sql.limit(10)
    |> sql.returning([sql.col("id")])
    |> sql.to_query(a())

  assert q.sql
    == "UPDATE users SET active = $1 ORDER BY created_at ASC LIMIT 10 RETURNING id"
  assert q.values == [sql.Bool(False)]
}

// --- UPDATE with LIMIT + OFFSET ---

pub fn update_with_limit_offset_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("active", sql.false, of: sql.value)
    |> sql.limit(10)
    |> sql.offset(20)
    |> sql.to_query(a())

  assert q.sql == "UPDATE users SET active = $1 LIMIT 10 OFFSET 20"
  assert q.values == [sql.Bool(False)]
}

// --- UPDATE OFFSET without LIMIT ---

pub fn update_offset_without_limit_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("active", sql.false, of: sql.value)
    |> sql.offset(5)
    |> sql.to_query(a())

  assert q.sql == "UPDATE users SET active = $1 OFFSET 5"
  assert q.values == [sql.Bool(False)]
}

// --- UPDATE SET from subquery ---
// TODO: requires subquery support in SET values
pub fn update_set_from_subqueryquery_test() {
  let sub =
    sql.from(sql.table("new_emails"))
    |> sql.select([sql.col("email")])
    |> sql.where(sql.eq(
      sql.col("user_id") |> sql.col_for("new_emails"),
      sql.col("id") |> sql.col_for("users"),
      of: sql.column,
    ))

  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("email", sub, of: sql.subquery)
    |> sql.to_query(a())

  assert q.sql
    == "UPDATE users SET email = (SELECT email FROM new_emails WHERE new_emails.user_id = users.id)"
  assert q.values == []
}

pub fn update_set_from_subqueryquery_literal_test() {
  let sub =
    sql.from(sql.table("new_emails"))
    |> sql.select([sql.col("email")])
    |> sql.where(sql.eq(
      sql.col("user_id") |> sql.col_for("new_emails"),
      sql.col("id") |> sql.col_for("users"),
      of: sql.column,
    ))

  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("email", sub, of: sql.subquery)
    |> sql.to_string(a())

  assert q
    == "UPDATE users SET email = (SELECT email FROM new_emails WHERE new_emails.user_id = users.id)"
}

pub fn update_set_from_subqueryquery_with_scalar_test() {
  let sub =
    sql.from(sql.table("new_emails"))
    |> sql.select([sql.col("email")])
    |> sql.where(sql.eq(
      sql.col("user_id") |> sql.col_for("new_emails"),
      sql.col("id") |> sql.col_for("users"),
      of: sql.column,
    ))

  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("email", sub, of: sql.subquery)
    |> sql.set("name", sql.text("Alice"), of: sql.value)
    |> sql.to_query(a())

  assert q.sql
    == "UPDATE users SET email = (SELECT email FROM new_emails WHERE new_emails.user_id = users.id), name = $1"
  assert q.values == [sql.text("Alice")]
}

// --- CTE with trailing semicolons ---

pub fn cte_basic_with_semicolon_test() {
  let cte_query =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id"), sql.col("name")])
    |> sql.where(sql.eq(sql.col("active"), sql.true, of: sql.value))

  let q =
    sql.from(sql.table("active_users"))
    |> sql.select([sql.col("id"), sql.col("name")])
    |> sql.with([sql.cte(name: "active_users", query: cte_query)])
    |> sql.to_query(a())

  assert q.sql
    == "WITH active_users AS (SELECT id, name FROM users WHERE active = $1) SELECT id, name FROM active_users;"
}

pub fn cte_multiple_with_semicolon_test() {
  let cte1 =
    sql.from(sql.table("users"))
    |> sql.select([sql.col("id"), sql.col("name")])
    |> sql.where(sql.eq(sql.col("active"), sql.true, of: sql.value))

  let cte2 =
    sql.from(sql.table("orders"))
    |> sql.select([sql.col("user_id"), sql.sum("amount") |> sql.col_as("total")])
    |> sql.group_by([sql.col("user_id")])

  let q =
    sql.from(sql.table("active_users") |> sql.table_as("au"))
    |> sql.select([
      sql.col("name") |> sql.col_for("au"),
      sql.col("total") |> sql.col_for("uo"),
    ])
    |> sql.join(sql.inner_join(
      table: sql.table("user_orders") |> sql.table_as("uo"),
      on: sql.eq(
        sql.col("id") |> sql.col_for("au"),
        sql.col("user_id") |> sql.col_for("uo"),
        of: sql.column,
      ),
    ))
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
    |> sql.select([sql.col("id"), sql.col("name")])

  let q =
    sql.from(sql.table("u"))
    |> sql.select([sql.col("user_id"), sql.col("user_name")])
    |> sql.with([
      sql.cte(name: "u", query: cte_query)
      |> sql.cte_columns(columns: ["user_id", "user_name"]),
    ])
    |> sql.to_query(a())

  assert q.sql
    == "WITH u(user_id, user_name) AS (SELECT id, name FROM users) SELECT user_id, user_name FROM u;"
}

pub fn cte_recursive_union_all_semicolon_test() {
  let base_query =
    sql.from(sql.table("categories"))
    |> sql.select([sql.col("id"), sql.col("parent_id"), sql.col("name")])
    |> sql.where(sql.is_null(sql.col("parent_id")))

  let q =
    sql.from(sql.table("category_tree"))
    |> sql.select([sql.col("id"), sql.col("parent_id"), sql.col("name")])
    |> sql.with([sql.cte(name: "category_tree", query: base_query)])
    |> sql.recursive
    |> sql.to_query(a())

  assert q.sql
    == "WITH RECURSIVE category_tree AS (SELECT id, parent_id, name FROM categories WHERE parent_id IS NULL) SELECT id, parent_id, name FROM category_tree;"
}

// --- UPDATE with IS FALSE condition ---

pub fn update_where_is_false_test() {
  let q =
    sql.update(table: sql.table("users"))
    |> sql.set("status", sql.text("inactive"), of: sql.value)
    |> sql.where(sql.is_false(sql.col("active")))
    |> sql.to_query(a())

  assert q.sql == "UPDATE users SET status = $1 WHERE active IS FALSE"
  assert q.values == [sql.Text("inactive")]
}
