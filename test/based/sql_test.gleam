import based/sql
import based/value
import gleam/int
import gleam/string_tree
import gleeunit/should

// Column

pub fn column_test() {
  let col =
    sql.name("id")
    |> sql.column

  let expected = "id"
  sql.node_to_string(col, value.format())
  |> should.equal(expected)
}

pub fn alias_test() {
  let col = sql.name("user_id") |> sql.alias("id")

  let expected = "user_id AS id"
  sql.identifier_to_string(col, value.format())
  |> should.equal(expected)
}

pub fn table_and_alias_test() {
  let users = sql.name("users") |> sql.table

  let col =
    users
    |> sql.attribute("user_id")
    |> sql.alias("id")

  let expected = "users.user_id AS id"

  sql.identifier_to_string(col, value.format())
  |> should.equal(expected)
}

// Expr

pub fn eq_test() {
  let val = sql.value(1, of: value.int)

  let sql = sql.eq(sql.name("id") |> sql.column, val)

  let expected = "id = :param"
  sql.expr_to_string_tree(sql, sql.format())
  |> string_tree.to_string
  |> should.equal(expected)

  sql.expr_to_values(sql) |> should.equal([value.int(1)])
}

pub fn greater_than_test() {
  let val = sql.value(18, of: value.int)

  let sql = sql.gt(sql.name("age") |> sql.column, val)

  let expected = "age > :param"
  sql.expr_to_string_tree(sql, sql.format())
  |> string_tree.to_string
  |> should.equal(expected)

  sql.expr_to_values(sql) |> should.equal([value.int(18)])
}

pub fn less_than_test() {
  let col = sql.name("age") |> sql.column
  let val = sql.value(65, of: value.int)

  let sql = sql.lt(col, val)

  let expected = "age < :param"
  sql.expr_to_string_tree(sql, sql.format())
  |> string_tree.to_string
  |> should.equal(expected)

  sql.expr_to_values(sql) |> should.equal([value.int(65)])
}

pub fn greater_than_equal_test() {
  let col = sql.name("age") |> sql.column
  let val = sql.value(18, of: value.int)

  let sql = sql.gt_eq(col, val)

  let expected = "age >= :param"
  sql.expr_to_string_tree(sql, sql.format())
  |> string_tree.to_string
  |> should.equal(expected)

  sql.expr_to_values(sql) |> should.equal([value.int(18)])
}

pub fn less_than_equal_test() {
  let col = sql.name("age") |> sql.column
  let val = sql.value(65, of: value.int)

  let sql = sql.lt_eq(col, val)

  let expected = "age <= :param"
  sql.expr_to_string_tree(sql, sql.format())
  |> string_tree.to_string
  |> should.equal(expected)

  sql.expr_to_values(sql) |> should.equal([value.int(65)])
}

pub fn not_equal_test() {
  let col = sql.name("status") |> sql.column
  let val = sql.value("inactive", of: value.text)

  let sql = sql.not_eq(col, val)

  let expected = "status <> :param"
  sql.expr_to_string_tree(sql, sql.format())
  |> string_tree.to_string
  |> should.equal(expected)

  sql.expr_to_values(sql) |> should.equal([value.text("inactive")])
}

pub fn between_test() {
  let col = sql.name("price") |> sql.column
  let start = sql.value(10.0, of: value.float)
  let end = sql.value(50.0, of: value.float)

  let sql = sql.between(col, start, end)

  let expected = "price BETWEEN :param AND :param"
  sql.expr_to_string_tree(sql, sql.format())
  |> string_tree.to_string
  |> should.equal(expected)

  sql.expr_to_values(sql)
  |> should.equal([value.float(10.0), value.float(50.0)])
}

pub fn like_test() {
  let col = sql.name("name") |> sql.column

  let sql = sql.like(col, "%John%", of: sql.value(_, value.text))

  let expected = "name LIKE :param"
  sql.expr_to_string_tree(sql, sql.format())
  |> string_tree.to_string
  |> should.equal(expected)

  sql.expr_to_values(sql) |> should.equal([value.text("%John%")])
}

pub fn in_test() {
  let col = sql.name("id") |> sql.column
  let values = sql.values([value.int(1), value.int(2), value.int(3)])

  let sql = sql.in(col, values)

  let expected = "id IN (:param, :param, :param)"
  sql.expr_to_string_tree(sql, sql.format())
  |> string_tree.to_string
  |> should.equal(expected)

  sql.expr_to_values(sql)
  |> should.equal([value.int(1), value.int(2), value.int(3)])
}

pub fn is_test() {
  let col = sql.name("active") |> sql.column

  let sql = sql.is(col, sql.value(True, of: value.bool))

  let expected = "active IS :param"
  sql.expr_to_string_tree(sql, sql.format())
  |> string_tree.to_string
  |> should.equal(expected)

  sql.expr_to_values(sql) |> should.equal([value.true])
}

pub fn not_like_test() {
  let col = sql.name("name") |> sql.column

  let sql = sql.not_like(col, "%admin%", of: sql.value(_, value.text))

  let expected = "name NOT LIKE :param"
  sql.expr_to_string_tree(sql, sql.format())
  |> string_tree.to_string
  |> should.equal(expected)

  sql.expr_to_values(sql) |> should.equal([value.text("%admin%")])
}

pub fn and_test() {
  let sql1 =
    sql.eq(sql.name("active") |> sql.column, sql.value(True, of: value.bool))
  let sql2 = sql.gt(sql.name("age") |> sql.column, sql.value(18, of: value.int))

  let and_sql = sql.and(sql1, sql2)

  let expected = "active = :param AND age > :param"
  sql.expr_to_string_tree(and_sql, sql.format())
  |> string_tree.to_string
  |> should.equal(expected)

  sql.expr_to_values(and_sql) |> should.equal([value.true, value.int(18)])
}

pub fn or_test() {
  let sql1 =
    sql.eq(sql.name("name") |> sql.column, sql.value("John", of: value.text))
  let sql2 =
    sql.eq(
      sql.name("email") |> sql.column,
      sql.value("john@example.com", of: value.text),
    )

  let or_sql = sql.or(sql1, sql2)

  let expected = "name = :param OR email = :param"
  sql.expr_to_string_tree(or_sql, sql.format())
  |> string_tree.to_string
  |> should.equal(expected)

  sql.expr_to_values(or_sql)
  |> should.equal([value.text("John"), value.text("john@example.com")])
}

pub fn not_test() {
  let sql1 =
    sql.eq(sql.name("active") |> sql.column, sql.value(True, of: value.bool))

  let not_sql = sql.not(sql1)

  let expected = "NOT active = :param"
  sql.expr_to_string_tree(not_sql, sql.format())
  |> string_tree.to_string
  |> should.equal(expected)

  sql.expr_to_values(not_sql) |> should.equal([value.true])
}

pub fn complex_sqlession_test() {
  let sql1 =
    sql.eq(sql.name("active") |> sql.column, sql.value(True, of: value.bool))
  let sql2 = sql.gt(sql.name("age") |> sql.column, sql.value(18, of: value.int))
  let sql3 =
    sql.eq(sql.name("role") |> sql.column, sql.value("admin", of: value.text))

  let and_sql = sql.and(sql1, sql2)
  let or_sql = sql.or(and_sql, sql3)

  let expected = "active = :param AND age > :param OR role = :param"
  sql.expr_to_string_tree(or_sql, sql.format())
  |> string_tree.to_string
  |> should.equal(expected)

  sql.expr_to_values(or_sql)
  |> should.equal([value.true, value.int(18), value.text("admin")])
}

pub fn column_with_table_test() {
  let users = sql.name("users") |> sql.table
  let col = users |> sql.attribute("id")

  let expected = "users.id"
  sql.identifier_to_string(col, value.format())
  |> should.equal(expected)
}

pub fn columns_test() {
  let cols_node = sql.columns(["id", "name", "email"])

  let expected = "(id, name, email)"
  sql.node_to_string(cols_node, value.format())
  |> should.equal(expected)
}

pub fn value_test() {
  let val = sql.value(42, of: value.int)

  let expected = ":param"
  sql.node_to_string(val, value.format())
  |> should.equal(expected)

  sql.unwrap(val) |> should.equal([value.int(42)])
}

pub fn values_test() {
  let vals = sql.values([value.int(1), value.int(2), value.int(3)])

  let expected = "(:param, :param, :param)"
  sql.node_to_string(vals, value.format())
  |> should.equal(expected)

  sql.unwrap(vals)
  |> should.equal([value.int(1), value.int(2), value.int(3)])
}

pub fn tuples_test() {
  let tuples =
    sql.tuples([
      [sql.value(1, of: value.int), sql.value("John", of: value.text)],
      [sql.value(2, of: value.int), sql.value("Jane", of: value.text)],
    ])

  let expected = "((:param, :param), (:param, :param))"
  sql.node_to_string(tuples, value.format())
  |> should.equal(expected)

  sql.unwrap(tuples)
  |> should.equal([
    value.int(1),
    value.text("John"),
    value.int(2),
    value.text("Jane"),
  ])
}

pub fn special_values_test() {
  let true_node = sql.value(True, of: value.bool)
  let false_node = sql.value(False, of: value.bool)
  let null_node = sql.value(Nil, value.null)

  sql.node_to_string(true_node, value.format())
  |> should.equal(":param")
  sql.node_to_string(false_node, value.format())
  |> should.equal(":param")
  sql.node_to_string(null_node, value.format())
  |> should.equal(":param")

  sql.unwrap(true_node) |> should.equal([value.true])
  sql.unwrap(false_node) |> should.equal([value.false])
  sql.unwrap(null_node) |> should.equal([value.null(Nil)])
}

// Format

pub fn format_test() {
  let int_fmt =
    sql.format()
    |> sql.on_identifier(fn(s) { "\"" <> s <> "\"" })
    |> sql.on_placeholder(fn(i) { "$" <> int.to_string(i) })
    |> sql.on_value(int.to_string)

  sql.to_identifier(int_fmt, "column") |> should.equal("\"column\"")
  sql.to_placeholder(int_fmt, 1) |> should.equal("$1")
  sql.to_string(int_fmt, 42) |> should.equal("42")
}

pub fn on_placeholder_test() {
  let int_fmt =
    sql.format()
    |> sql.on_identifier(fn(s) { s })
    |> sql.on_placeholder(fn(i) { "$" <> int.to_string(i) })
    |> sql.on_value(int.to_string)

  let int_fmt = sql.on_placeholder(int_fmt, fn(i) { "?" <> int.to_string(i) })

  sql.to_placeholder(int_fmt, 1) |> should.equal("?1")
  sql.to_identifier(int_fmt, "column") |> should.equal("column")
  sql.to_string(int_fmt, 42) |> should.equal("42")
}

pub fn on_identifier_test() {
  let int_fmt =
    sql.format()
    |> sql.on_identifier(fn(s) { s })
    |> sql.on_placeholder(fn(i) { "$" <> int.to_string(i) })
    |> sql.on_value(int.to_string)

  let int_fmt = sql.on_identifier(int_fmt, fn(s) { "[" <> s <> "]" })

  sql.to_identifier(int_fmt, "column") |> should.equal("[column]")
  sql.to_placeholder(int_fmt, 1) |> should.equal("$1")
  sql.to_string(int_fmt, 42) |> should.equal("42")
}

pub fn on_value_test() {
  let int_fmt =
    sql.format()
    |> sql.on_identifier(fn(s) { s })
    |> sql.on_placeholder(fn(i) { "$" <> int.to_string(i) })
    |> sql.on_value(int.to_string)

  let int_fmt = sql.on_value(int_fmt, fn(i) { "num:" <> int.to_string(i) })

  sql.to_string(int_fmt, 42) |> should.equal("num:42")
  sql.to_identifier(int_fmt, "column") |> should.equal("column")
  sql.to_placeholder(int_fmt, 1) |> should.equal("$1")
}
