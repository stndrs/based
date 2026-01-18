import based/sql
import based/sql/column
import based/sql/select
import based/sql/union
import based/value
import gleeunit/should

pub fn union_test() {
  let expected =
    "SELECT id, name FROM users WHERE active IS TRUE UNION SELECT id, name FROM employees WHERE department = ?"
  let users = sql.table("users")
  let employees = sql.table("employees")
  let users_query =
    value.repo()
    |> select.from(users)
    |> select.columns(["id", "name"])
    |> select.where([
      column.new("active")
      |> column.is(True),
    ])

  let employees_query =
    value.repo()
    |> select.from(employees)
    |> select.columns(["id", "name"])
    |> select.where([
      column.new("department")
      |> column.eq(value.text("Engineering"), of: sql.value),
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
  let users = sql.table("users")
  let employees = sql.table("employees")
  let users_query =
    value.repo()
    |> select.from(users)
    |> select.columns(["id", "name"])
    |> select.where([
      column.new("active")
      |> column.is(True),
    ])

  let employees_query =
    value.repo()
    |> select.from(employees)
    |> select.columns(["id", "name"])
    |> select.where([
      column.new("department")
      |> column.eq(value.text("Engineering"), of: sql.value),
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
  let users = sql.table("users")
  let employees = sql.table("employees")
  let users_query =
    value.repo()
    |> select.from(users)
    |> select.columns(["id", "name"])
    |> select.where([
      column.new("active")
      |> column.is(True),
    ])

  let employees_query =
    value.repo()
    |> select.from(employees)
    |> select.columns(["id", "name"])
    |> select.where([
      column.new("department")
      |> column.eq(value.text("Engineering"), of: sql.value),
    ])

  let result =
    union.new([users_query, employees_query])
    |> union.to_string

  result |> should.equal(expected)
}

pub fn union_all_to_string_test() {
  let expected =
    "SELECT id, name FROM users WHERE active IS TRUE UNION ALL SELECT id, name FROM employees WHERE department = 'Engineering'"
  let users = sql.table("users")
  let employees = sql.table("employees")
  let users_query =
    value.repo()
    |> select.from(users)
    |> select.columns(["id", "name"])
    |> select.where([
      column.new("active")
      |> column.is(True),
    ])

  let employees_query =
    value.repo()
    |> select.from(employees)
    |> select.columns(["id", "name"])
    |> select.where([
      column.new("department")
      |> column.eq(value.text("Engineering"), of: sql.value),
    ])

  let result =
    union.all([users_query, employees_query])
    |> union.to_string

  result |> should.equal(expected)
}

pub fn multi_union_to_string_test() {
  let expected =
    "SELECT id, name FROM users WHERE active IS TRUE UNION SELECT id, name FROM employees WHERE department = 'Engineering' UNION SELECT id, name FROM contractors WHERE status = 'available'"
  let users = sql.table("users")
  let employees = sql.table("employees")
  let contractors = sql.table("contractors")
  let users_query =
    value.repo()
    |> select.from(users)
    |> select.columns(["id", "name"])
    |> select.where([
      column.new("active")
      |> column.is(True),
    ])

  let employees_query =
    value.repo()
    |> select.from(employees)
    |> select.columns(["id", "name"])
    |> select.where([
      column.new("department")
      |> column.eq(value.text("Engineering"), of: sql.value),
    ])

  let contractors_query =
    value.repo()
    |> select.from(contractors)
    |> select.columns(["id", "name"])
    |> select.where([
      column.new("status")
      |> column.eq(value.text("available"), of: sql.value),
    ])

  let result =
    union.new([users_query, employees_query, contractors_query])
    |> union.to_string

  result |> should.equal(expected)
}
