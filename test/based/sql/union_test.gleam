import based/sql
import based/sql/select
import based/sql/union
import based/value
import gleeunit/should

pub fn union_test() {
  let expected =
    "SELECT id, name FROM users WHERE active IS ? UNION SELECT id, name FROM employees WHERE department = ?"
  let users = sql.name("users") |> sql.table
  let employees = sql.name("employees") |> sql.table

  let users_query =
    select.from(users)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.name("active")
      |> sql.column
      |> sql.is(sql.value(True, value.bool)),
    ])

  let employees_query =
    select.from(employees)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.name("department")
      |> sql.column
      |> sql.eq(sql.value("Engineering", of: value.text)),
    ])

  let query =
    union.new([users_query, employees_query])
    |> union.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.true, value.text("Engineering")])
}

pub fn union_all_test() {
  let expected =
    "SELECT id, name FROM users WHERE active IS ? UNION ALL SELECT id, name FROM employees WHERE department = ?"
  let users = sql.name("users") |> sql.table
  let employees = sql.name("employees") |> sql.table

  let users_query =
    select.from(users)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.name("active")
      |> sql.column
      |> sql.is(sql.value(True, value.bool)),
    ])

  let employees_query =
    select.from(employees)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.name("department")
      |> sql.column
      |> sql.eq(sql.value("Engineering", of: value.text)),
    ])

  let query =
    union.all([users_query, employees_query])
    |> union.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.true, value.text("Engineering")])
}

pub fn union_to_string_test() {
  let expected =
    "SELECT id, name FROM users WHERE active IS TRUE UNION SELECT id, name FROM employees WHERE department = 'Engineering'"
  let users = sql.name("users") |> sql.table
  let employees = sql.name("employees") |> sql.table

  let users_query =
    select.from(users)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.name("active")
      |> sql.column
      |> sql.is(sql.value(True, value.bool)),
    ])

  let employees_query =
    select.from(employees)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.name("department")
      |> sql.column
      |> sql.eq(sql.value("Engineering", of: value.text)),
    ])

  let result =
    union.new([users_query, employees_query])
    |> union.to_string(value.format())

  result |> should.equal(expected)
}

pub fn union_all_to_string_test() {
  let expected =
    "SELECT id, name FROM users WHERE active IS TRUE UNION ALL SELECT id, name FROM employees WHERE department = 'Engineering'"
  let users = sql.name("users") |> sql.table
  let employees = sql.name("employees") |> sql.table

  let users_query =
    select.from(users)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.name("active")
      |> sql.column
      |> sql.is(sql.value(True, value.bool)),
    ])

  let employees_query =
    select.from(employees)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.name("department")
      |> sql.column
      |> sql.eq(sql.value("Engineering", of: value.text)),
    ])

  let result =
    union.all([users_query, employees_query])
    |> union.to_string(value.format())

  result |> should.equal(expected)
}

pub fn multi_union_to_string_test() {
  let expected =
    "SELECT id, name FROM users WHERE active IS TRUE UNION SELECT id, name FROM employees WHERE department = 'Engineering' UNION SELECT id, name FROM contractors WHERE status = 'available'"
  let users = sql.name("users") |> sql.table
  let employees = sql.name("employees") |> sql.table
  let contractors = sql.name("contractors") |> sql.table

  let users_query =
    select.from(users)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.name("active")
      |> sql.column
      |> sql.is(sql.value(True, value.bool)),
    ])

  let employees_query =
    select.from(employees)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.name("department")
      |> sql.column
      |> sql.eq(sql.value("Engineering", of: value.text)),
    ])

  let contractors_query =
    select.from(contractors)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.name("status")
      |> sql.column
      |> sql.eq(sql.value("available", of: value.text)),
    ])

  let result =
    union.new([users_query, employees_query, contractors_query])
    |> union.to_string(value.format())

  result |> should.equal(expected)
}
