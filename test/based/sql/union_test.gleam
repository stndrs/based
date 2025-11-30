import based/sql
import based/sql/select
import based/sql/union
import based/value
import gleeunit/should

pub fn union_test() {
  let expected =
    "SELECT id, name FROM users WHERE active IS ? UNION SELECT id, name FROM employees WHERE department = ?"

  let users_query =
    sql.select(["id", "name"])
    |> select.from(sql.table("users"))
    |> select.where([
      sql.column("active") |> sql.is(sql.value(True, value.bool)),
    ])

  let employees_query =
    sql.select(["id", "name"])
    |> select.from(sql.table("employees"))
    |> select.where([
      sql.column("department")
      |> sql.eq(sql.value("Engineering", of: value.text)),
    ])

  let query =
    sql.union([users_query, employees_query])
    |> union.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.true, value.text("Engineering")])
}

pub fn union_all_test() {
  let expected =
    "SELECT id, name FROM users WHERE active IS ? UNION ALL SELECT id, name FROM employees WHERE department = ?"

  let users_query =
    sql.select(["id", "name"])
    |> select.from(sql.table("users"))
    |> select.where([
      sql.column("active") |> sql.is(sql.value(True, value.bool)),
    ])

  let employees_query =
    sql.select(["id", "name"])
    |> select.from(sql.table("employees"))
    |> select.where([
      sql.column("department")
      |> sql.eq(sql.value("Engineering", of: value.text)),
    ])

  let query =
    sql.union_all([users_query, employees_query])
    |> union.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.true, value.text("Engineering")])
}

pub fn union_to_string_test() {
  let expected =
    "SELECT id, name FROM users WHERE active IS TRUE UNION SELECT id, name FROM employees WHERE department = 'Engineering'"

  let users_query =
    sql.select(["id", "name"])
    |> select.from(sql.table("users"))
    |> select.where([
      sql.column("active") |> sql.is(sql.value(True, value.bool)),
    ])

  let employees_query =
    sql.select(["id", "name"])
    |> select.from(sql.table("employees"))
    |> select.where([
      sql.column("department")
      |> sql.eq(sql.value("Engineering", of: value.text)),
    ])

  let result =
    sql.union([users_query, employees_query])
    |> union.to_string(value.format())

  result |> should.equal(expected)
}

pub fn union_all_to_string_test() {
  let expected =
    "SELECT id, name FROM users WHERE active IS TRUE UNION ALL SELECT id, name FROM employees WHERE department = 'Engineering'"

  let users_query =
    sql.select(["id", "name"])
    |> select.from(sql.table("users"))
    |> select.where([
      sql.column("active") |> sql.is(sql.value(True, value.bool)),
    ])

  let employees_query =
    sql.select(["id", "name"])
    |> select.from(sql.table("employees"))
    |> select.where([
      sql.column("department")
      |> sql.eq(sql.value("Engineering", of: value.text)),
    ])

  let result =
    sql.union_all([users_query, employees_query])
    |> union.to_string(value.format())

  result |> should.equal(expected)
}

pub fn multi_union_to_string_test() {
  let expected =
    "SELECT id, name FROM users WHERE active IS TRUE UNION SELECT id, name FROM employees WHERE department = 'Engineering' UNION SELECT id, name FROM contractors WHERE status = 'available'"

  let users_query =
    sql.select(["id", "name"])
    |> select.from(sql.table("users"))
    |> select.where([
      sql.column("active") |> sql.is(sql.value(True, value.bool)),
    ])

  let employees_query =
    sql.select(["id", "name"])
    |> select.from(sql.table("employees"))
    |> select.where([
      sql.column("department")
      |> sql.eq(sql.value("Engineering", of: value.text)),
    ])

  let contractors_query =
    sql.select(["id", "name"])
    |> select.from(sql.table("contractors"))
    |> select.where([
      sql.column("status") |> sql.eq(sql.value("available", of: value.text)),
    ])

  let result =
    sql.union([users_query, employees_query, contractors_query])
    |> union.to_string(value.format())

  result |> should.equal(expected)
}
