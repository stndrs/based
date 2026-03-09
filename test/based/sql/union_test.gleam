import based/db
import based/repo
import based/sql
import based/sql/column
import based/sql/select
import based/sql/union
import gleeunit/should

pub fn union_test() {
  let repo = repo.default()

  let expected =
    "SELECT id, name FROM users WHERE active IS TRUE UNION SELECT id, name FROM employees WHERE department = ?"
  let users = sql.table("users")
  let employees = sql.table("employees")
  let users_query =
    repo
    |> select.from(users)
    |> select.columns([sql.column("id"), sql.column("name")])
    |> select.where([
      sql.column("active")
      |> column.is(True),
    ])

  let employees_query =
    repo
    |> select.from(employees)
    |> select.columns([sql.column("id"), sql.column("name")])
    |> select.where([
      sql.column("department")
      |> sql.eq(db.text("Engineering"), of: sql.val),
    ])

  let query =
    union.new(repo, [users_query, employees_query])
    |> union.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([db.text("Engineering")])
}

pub fn union_all_test() {
  let repo = repo.default()

  let expected =
    "SELECT id, name FROM users WHERE active IS TRUE UNION ALL SELECT id, name FROM employees WHERE department = ?"
  let users = sql.table("users")
  let employees = sql.table("employees")
  let users_query =
    repo
    |> select.from(users)
    |> select.columns([sql.column("id"), sql.column("name")])
    |> select.where([
      sql.column("active")
      |> column.is(True),
    ])

  let employees_query =
    repo
    |> select.from(employees)
    |> select.columns([sql.column("id"), sql.column("name")])
    |> select.where([
      sql.column("department")
      |> sql.eq(db.text("Engineering"), of: sql.val),
    ])

  let query =
    union.all(repo, [users_query, employees_query])
    |> union.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([db.text("Engineering")])
}

pub fn union_to_string_test() {
  let repo = repo.default()

  let expected =
    "SELECT id, name FROM users WHERE active IS TRUE UNION SELECT id, name FROM employees WHERE department = 'Engineering'"
  let users = sql.table("users")
  let employees = sql.table("employees")
  let users_query =
    repo
    |> select.from(users)
    |> select.columns([sql.column("id"), sql.column("name")])
    |> select.where([
      sql.column("active")
      |> column.is(True),
    ])

  let employees_query =
    repo
    |> select.from(employees)
    |> select.columns([sql.column("id"), sql.column("name")])
    |> select.where([
      sql.column("department")
      |> sql.eq(db.text("Engineering"), of: sql.val),
    ])

  let result =
    union.new(repo, [users_query, employees_query])
    |> union.to_string

  result |> should.equal(expected)
}

pub fn union_all_to_string_test() {
  let repo = repo.default()

  let expected =
    "SELECT id, name FROM users WHERE active IS TRUE UNION ALL SELECT id, name FROM employees WHERE department = 'Engineering'"
  let users = sql.table("users")
  let employees = sql.table("employees")
  let users_query =
    repo
    |> select.from(users)
    |> select.columns([sql.column("id"), sql.column("name")])
    |> select.where([
      sql.column("active")
      |> column.is(True),
    ])

  let employees_query =
    repo
    |> select.from(employees)
    |> select.columns([sql.column("id"), sql.column("name")])
    |> select.where([
      sql.column("department")
      |> sql.eq(db.text("Engineering"), of: sql.val),
    ])

  let result =
    union.all(repo, [users_query, employees_query])
    |> union.to_string

  result |> should.equal(expected)
}

pub fn multi_union_to_string_test() {
  let repo = repo.default()

  let expected =
    "SELECT id, name FROM users WHERE active IS TRUE UNION SELECT id, name FROM employees WHERE department = 'Engineering' UNION SELECT id, name FROM contractors WHERE status = 'available'"
  let users = sql.table("users")
  let employees = sql.table("employees")
  let contractors = sql.table("contractors")
  let users_query =
    repo
    |> select.from(users)
    |> select.columns([sql.column("id"), sql.column("name")])
    |> select.where([
      sql.column("active")
      |> column.is(True),
    ])

  let employees_query =
    repo
    |> select.from(employees)
    |> select.columns([sql.column("id"), sql.column("name")])
    |> select.where([
      sql.column("department")
      |> sql.eq(db.text("Engineering"), of: sql.val),
    ])

  let contractors_query =
    repo
    |> select.from(contractors)
    |> select.columns([sql.column("id"), sql.column("name")])
    |> select.where([
      sql.column("status")
      |> sql.eq(db.text("available"), of: sql.val),
    ])

  let result =
    union.new(repo, [users_query, employees_query, contractors_query])
    |> union.to_string

  result |> should.equal(expected)
}
