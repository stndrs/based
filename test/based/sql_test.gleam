import based/sql
import based/value
import gleam/int
import gleeunit/should

// Column

// pub fn column_test() {
//   let col =
//     sql.identifier("id")
//     |> sql.column
// 
//   let expected = "id"
//   sql.node_to_string(col, value.format())
//   |> should.equal(expected)
// }
// 
// pub fn alias_test() {
//   let col = sql.identifier("user_id") |> sql.alias("id") |> sql.column
// 
//   let expected = "user_id AS id"
//   sql.node_to_string(col, value.format())
//   |> should.equal(expected)
// }
// 
// pub fn table_and_alias_test() {
//   let col =
//     sql.identifier("users")
//     |> sql.attr("user_id")
//     |> sql.alias("id")
//     |> sql.column
// 
//   let expected = "users.user_id AS id"
// 
//   sql.node_to_string(col, value.format())
//   |> should.equal(expected)
// }

// Expr

pub fn eq_test() {
  let val = sql.value(value.int(1))

  let sql = sql.eq(sql.identifier("id") |> sql.column, val)

  let expected = "id = :param"
  sql.expr_to_string(sql, sql.new())
  |> should.equal(expected)

  sql.expr_to_values(sql) |> should.equal([value.int(1)])
}

pub fn greater_than_test() {
  let val = sql.value(value.int(18))

  let sql = sql.gt(sql.identifier("age") |> sql.column, val)

  let expected = "age > :param"
  sql.expr_to_string(sql, sql.new())
  |> should.equal(expected)

  sql.expr_to_values(sql) |> should.equal([value.int(18)])
}

pub fn less_than_test() {
  let col = sql.identifier("age") |> sql.column
  let val = sql.value(value.int(65))

  let sql = sql.lt(col, val)

  let expected = "age < :param"
  sql.expr_to_string(sql, sql.new())
  |> should.equal(expected)

  sql.expr_to_values(sql) |> should.equal([value.int(65)])
}

pub fn greater_than_equal_test() {
  let col = sql.identifier("age") |> sql.column
  let val = sql.value(value.int(18))

  let sql = sql.gt_eq(col, val)

  let expected = "age >= :param"
  sql.expr_to_string(sql, sql.new())
  |> should.equal(expected)

  sql.expr_to_values(sql) |> should.equal([value.int(18)])
}

pub fn less_than_equal_test() {
  let col = sql.identifier("age") |> sql.column
  let val = sql.value(value.int(65))

  let sql = sql.lt_eq(col, val)

  let expected = "age <= :param"
  sql.expr_to_string(sql, sql.new())
  |> should.equal(expected)

  sql.expr_to_values(sql) |> should.equal([value.int(65)])
}

pub fn not_equal_test() {
  let col = sql.identifier("status") |> sql.column
  let val = sql.value(value.text("inactive"))

  let sql = sql.not_eq(col, val)

  let expected = "status <> :param"
  sql.expr_to_string(sql, sql.new())
  |> should.equal(expected)

  sql.expr_to_values(sql) |> should.equal([value.text("inactive")])
}

pub fn between_test() {
  let col = sql.identifier("price") |> sql.column
  let start = sql.value(value.float(10.0))
  let end = sql.value(value.float(50.0))

  let sql = sql.between(col, start, end)

  let expected = "price BETWEEN :param AND :param"
  sql.expr_to_string(sql, sql.new())
  |> should.equal(expected)

  sql.expr_to_values(sql)
  |> should.equal([value.float(10.0), value.float(50.0)])
}

pub fn like_test() {
  let col = sql.identifier("name") |> sql.column

  let sql = sql.like(col, "%John%", value.text)

  let expected = "name LIKE :param"
  sql.expr_to_string(sql, sql.new())
  |> should.equal(expected)

  sql.expr_to_values(sql) |> should.equal([value.text("%John%")])
}

pub fn in_test() {
  let col = sql.identifier("id") |> sql.column
  let values = sql.values([value.int(1), value.int(2), value.int(3)])

  let sql = sql.in(col, values)

  let expected = "id IN (:param, :param, :param)"
  sql.expr_to_string(sql, sql.new())
  |> should.equal(expected)

  sql.expr_to_values(sql)
  |> should.equal([value.int(1), value.int(2), value.int(3)])
}

pub fn is_test() {
  let col = sql.identifier("active") |> sql.column

  let sql = sql.is(col, True)

  let expected = "active IS TRUE"
  sql.expr_to_string(sql, sql.new())
  |> should.equal(expected)

  sql.expr_to_values(sql) |> should.equal([])
}

pub fn not_like_test() {
  let col = sql.identifier("name") |> sql.column

  let sql = sql.not_like(col, "%admin%", of: value.text)

  let expected = "name NOT LIKE :param"
  sql.expr_to_string(sql, sql.new())
  |> should.equal(expected)

  sql.expr_to_values(sql) |> should.equal([value.text("%admin%")])
}

pub fn and_test() {
  let sql1 =
    sql.eq(sql.identifier("active") |> sql.column, sql.value(value.true))
  let sql2 =
    sql.gt(sql.identifier("age") |> sql.column, sql.value(value.int(18)))

  let and_sql = sql.and(sql1, sql2)

  let expected = "active = :param AND age > :param"
  sql.expr_to_string(and_sql, sql.new())
  |> should.equal(expected)

  sql.expr_to_values(and_sql) |> should.equal([value.true, value.int(18)])
}

pub fn or_test() {
  let sql1 =
    sql.eq(sql.identifier("name") |> sql.column, sql.value(value.text("John")))
  let sql2 =
    sql.eq(
      sql.identifier("email") |> sql.column,
      sql.value(value.text("john@example.com")),
    )

  let or_sql = sql.or(sql1, sql2)

  let expected = "name = :param OR email = :param"
  sql.expr_to_string(or_sql, sql.new())
  |> should.equal(expected)

  sql.expr_to_values(or_sql)
  |> should.equal([value.text("John"), value.text("john@example.com")])
}

pub fn not_test() {
  let sql1 =
    sql.eq(sql.identifier("active") |> sql.column, sql.value(value.true))

  let not_sql = sql.not(sql1)

  let expected = "NOT active = :param"
  sql.expr_to_string(not_sql, sql.new())
  |> should.equal(expected)

  sql.expr_to_values(not_sql) |> should.equal([value.true])
}

pub fn complex_sqlession_test() {
  let sql1 =
    sql.eq(sql.identifier("active") |> sql.column, sql.value(value.true))
  let sql2 =
    sql.gt(sql.identifier("age") |> sql.column, sql.value(value.int(18)))
  let sql3 =
    sql.eq(sql.identifier("role") |> sql.column, sql.value(value.text("admin")))

  let and_sql = sql.and(sql1, sql2)
  let or_sql = sql.or(and_sql, sql3)

  let expected = "active = :param AND age > :param OR role = :param"
  sql.expr_to_string(or_sql, sql.new())
  |> should.equal(expected)

  sql.expr_to_values(or_sql)
  |> should.equal([value.true, value.int(18), value.text("admin")])
}

// pub fn column_with_table_test() {
//   let users = sql.identifier("users")
//   let col = users |> sql.attr("id") |> sql.column
// 
//   let expected = "users.id"
//   sql.node_to_string(col, value.format())
//   |> should.equal(expected)
// }
// 
// pub fn columns_test() {
//   let cols_node = sql.columns(["id", "name", "email"])
// 
//   let expected = "(id, name, email)"
//   sql.node_to_string(cols_node, value.format())
//   |> should.equal(expected)
// }
// 
// pub fn value_test() {
//   let val = sql.value(value.int(42))
// 
//   let expected = ":param"
//   sql.node_to_string(val, value.format())
//   |> should.equal(expected)
// 
//   sql.unwrap(val) |> should.equal([value.int(42)])
// }
// 
// pub fn values_test() {
//   let vals = sql.values([value.int(1), value.int(2), value.int(3)])
// 
//   let expected = "(:param, :param, :param)"
//   sql.node_to_string(vals, value.format())
//   |> should.equal(expected)
// 
//   sql.unwrap(vals)
//   |> should.equal([value.int(1), value.int(2), value.int(3)])
// }
// 
// pub fn tuples_test() {
//   let tuples =
//     sql.tuples([
//       [sql.value(value.int(1)), sql.value(value.text("John"))],
//       [sql.value(value.int(2)), sql.value(value.text("Jane"))],
//     ])
// 
//   let expected = "((:param, :param), (:param, :param))"
//   sql.node_to_string(tuples, value.format())
//   |> should.equal(expected)
// 
//   sql.unwrap(tuples)
//   |> should.equal([
//     value.int(1),
//     value.text("John"),
//     value.int(2),
//     value.text("Jane"),
//   ])
// }
// 
// pub fn special_values_test() {
//   let true_node = sql.value(value.true)
//   let false_node = sql.value(value.false)
//   let null_node = sql.value(value.null)
// 
//   sql.node_to_string(true_node, value.format())
//   |> should.equal(":param")
//   sql.node_to_string(false_node, value.format())
//   |> should.equal(":param")
//   sql.node_to_string(null_node, value.format())
//   |> should.equal(":param")
// 
//   sql.unwrap(true_node) |> should.equal([value.true])
//   sql.unwrap(false_node) |> should.equal([value.false])
//   sql.unwrap(null_node) |> should.equal([value.null])
// }

// Format

pub fn format_test() {
  let int_fmt =
    sql.new()
    |> sql.on_identifier(fn(s) { "\"" <> s <> "\"" })
    |> sql.on_placeholder(fn(i) { "$" <> int.to_string(i) })
    |> sql.on_value(int.to_string)

  sql.to_identifier(int_fmt, "column") |> should.equal("\"column\"")
  sql.to_placeholder(int_fmt, 1) |> should.equal("$1")
  sql.to_string(int_fmt, 42) |> should.equal("42")
}

pub fn on_placeholder_test() {
  let int_fmt =
    sql.new()
    |> sql.on_identifier(fn(s) { s })
    |> sql.on_placeholder(fn(i) { "$" <> int.to_string(i) })
    |> sql.on_value(int.to_string)

  let int_fmt = sql.on_placeholder(int_fmt, fn(i) { "?" <> int.to_string(i) })

  sql.to_placeholder(int_fmt, 1) |> should.equal("?1")
  sql.to_identifier(int_fmt, "column") |> should.equal("column")
  sql.to_string(int_fmt, 42) |> should.equal("42")
}

pub fn on_node_test() {
  let int_fmt =
    sql.new()
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
    sql.new()
    |> sql.on_identifier(fn(s) { s })
    |> sql.on_placeholder(fn(i) { "$" <> int.to_string(i) })
    |> sql.on_value(int.to_string)

  let int_fmt = sql.on_value(int_fmt, fn(i) { "num:" <> int.to_string(i) })

  sql.to_string(int_fmt, 42) |> should.equal("num:42")
  sql.to_identifier(int_fmt, "column") |> should.equal("column")
  sql.to_placeholder(int_fmt, 1) |> should.equal("$1")
}
