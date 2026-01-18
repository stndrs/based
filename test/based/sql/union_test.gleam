import based/sql
import based/sql/select
import based/sql/union
import based/value
import gleeunit/should

pub fn union_test() {
  let expected =
    "SELECT id, name FROM users WHERE active IS TRUE UNION SELECT id, name FROM employees WHERE department = ?"
  let users = sql.identifier("users")
  let employees = sql.identifier("employees")
  let users_query =
    value.repo()
    |> select.from(users)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.identifier("active")
      |> sql.column
      |> sql.is(True),
    ])

  let employees_query =
    value.repo()
    |> select.from(employees)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.identifier("department")
      |> sql.column
      |> sql.eq(sql.value(value.text("Engineering"))),
    ])

  let query =
    union.new([users_query, employees_query])
    |> union.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("Engineering")])
}

pub fn union_all_test() {
  let expected =
    "SELECT id, name FROM users WHERE active IS TRUE UNION ALL SELECT id, name FROM employees WHERE department = ?"
  let users = sql.identifier("users")
  let employees = sql.identifier("employees")
  let users_query =
    value.repo()
    |> select.from(users)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.identifier("active")
      |> sql.column
      |> sql.is(True),
    ])

  let employees_query =
    value.repo()
    |> select.from(employees)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.identifier("department")
      |> sql.column
      |> sql.eq(sql.value(value.text("Engineering"))),
    ])

  let query =
    union.all([users_query, employees_query])
    |> union.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("Engineering")])
}

pub fn union_to_string_test() {
  let expected =
    "SELECT id, name FROM users WHERE active IS TRUE UNION SELECT id, name FROM employees WHERE department = 'Engineering'"
  let users = sql.identifier("users")
  let employees = sql.identifier("employees")
  let users_query =
    value.repo()
    |> select.from(users)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.identifier("active")
      |> sql.column
      |> sql.is(True),
    ])

  let employees_query =
    value.repo()
    |> select.from(employees)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.identifier("department")
      |> sql.column
      |> sql.eq(sql.value(value.text("Engineering"))),
    ])

  let result =
    union.new([users_query, employees_query])
    |> union.to_string

  result |> should.equal(expected)
}

pub fn union_all_to_string_test() {
  let expected =
    "SELECT id, name FROM users WHERE active IS TRUE UNION ALL SELECT id, name FROM employees WHERE department = 'Engineering'"
  let users = sql.identifier("users")
  let employees = sql.identifier("employees")
  let users_query =
    value.repo()
    |> select.from(users)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.identifier("active")
      |> sql.column
      |> sql.is(True),
    ])

  let employees_query =
    value.repo()
    |> select.from(employees)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.identifier("department")
      |> sql.column
      |> sql.eq(sql.value(value.text("Engineering"))),
    ])

  let result =
    union.all([users_query, employees_query])
    |> union.to_string

  result |> should.equal(expected)
}

pub fn multi_union_to_string_test() {
  let expected =
    "SELECT id, name FROM users WHERE active IS TRUE UNION SELECT id, name FROM employees WHERE department = 'Engineering' UNION SELECT id, name FROM contractors WHERE status = 'available'"
  let users = sql.identifier("users")
  let employees = sql.identifier("employees")
  let contractors = sql.identifier("contractors")
  let users_query =
    value.repo()
    |> select.from(users)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.identifier("active")
      |> sql.column
      |> sql.is(True),
    ])

  let employees_query =
    value.repo()
    |> select.from(employees)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.identifier("department")
      |> sql.column
      |> sql.eq(sql.value(value.text("Engineering"))),
    ])

  let contractors_query =
    value.repo()
    |> select.from(contractors)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.identifier("status")
      |> sql.column
      |> sql.eq(sql.value(value.text("available"))),
    ])

  let result =
    union.new([users_query, employees_query, contractors_query])
    |> union.to_string

  result |> should.equal(expected)
}
