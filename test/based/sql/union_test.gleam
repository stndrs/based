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
    select.from(users, of: sql.table)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.identifier("active")
      |> sql.column
      |> sql.is(True),
    ])

  let employees_query =
    select.from(employees, of: sql.table)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.identifier("department")
      |> sql.column
      |> sql.eq(sql.value(value.text("Engineering"))),
    ])

  let query =
    union.new([users_query, employees_query])
    |> union.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("Engineering")])
}

pub fn union_all_test() {
  let expected =
    "SELECT id, name FROM users WHERE active IS TRUE UNION ALL SELECT id, name FROM employees WHERE department = ?"
  let users = sql.identifier("users")
  let employees = sql.identifier("employees")
  let users_query =
    select.from(users, of: sql.table)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.identifier("active")
      |> sql.column
      |> sql.is(True),
    ])

  let employees_query =
    select.from(employees, of: sql.table)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.identifier("department")
      |> sql.column
      |> sql.eq(sql.value(value.text("Engineering"))),
    ])

  let query =
    union.all([users_query, employees_query])
    |> union.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("Engineering")])
}

pub fn union_to_string_test() {
  let expected =
    "SELECT id, name FROM users WHERE active IS TRUE UNION SELECT id, name FROM employees WHERE department = 'Engineering'"
  let users = sql.identifier("users")
  let employees = sql.identifier("employees")
  let users_query =
    select.from(users, of: sql.table)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.identifier("active")
      |> sql.column
      |> sql.is(True),
    ])

  let employees_query =
    select.from(employees, of: sql.table)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.identifier("department")
      |> sql.column
      |> sql.eq(sql.value(value.text("Engineering"))),
    ])

  let result =
    union.new([users_query, employees_query])
    |> union.to_string(value.format())

  result |> should.equal(expected)
}

pub fn union_all_to_string_test() {
  let expected =
    "SELECT id, name FROM users WHERE active IS TRUE UNION ALL SELECT id, name FROM employees WHERE department = 'Engineering'"
  let users = sql.identifier("users")
  let employees = sql.identifier("employees")
  let users_query =
    select.from(users, of: sql.table)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.identifier("active")
      |> sql.column
      |> sql.is(True),
    ])

  let employees_query =
    select.from(employees, of: sql.table)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.identifier("department")
      |> sql.column
      |> sql.eq(sql.value(value.text("Engineering"))),
    ])

  let result =
    union.all([users_query, employees_query])
    |> union.to_string(value.format())

  result |> should.equal(expected)
}

pub fn multi_union_to_string_test() {
  let expected =
    "SELECT id, name FROM users WHERE active IS TRUE UNION SELECT id, name FROM employees WHERE department = 'Engineering' UNION SELECT id, name FROM contractors WHERE status = 'available'"
  let users = sql.identifier("users")
  let employees = sql.identifier("employees")
  let contractors = sql.identifier("contractors")
  let users_query =
    select.from(users, of: sql.table)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.identifier("active")
      |> sql.column
      |> sql.is(True),
    ])

  let employees_query =
    select.from(employees, of: sql.table)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.identifier("department")
      |> sql.column
      |> sql.eq(sql.value(value.text("Engineering"))),
    ])

  let contractors_query =
    select.from(contractors, of: sql.table)
    |> select.columns(["id", "name"])
    |> select.where([
      sql.identifier("status")
      |> sql.column
      |> sql.eq(sql.value(value.text("available"))),
    ])

  let result =
    union.new([users_query, employees_query, contractors_query])
    |> union.to_string(value.format())

  result |> should.equal(expected)
}
