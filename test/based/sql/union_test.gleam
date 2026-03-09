import based/db
import based/repo
import based/sql
import based/sql/column
import based/sql/select
import based/sql/union
import gleam/int
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

// Positional placeholder tests ($1, $2, ...) — verifies that placeholder
// indices are numbered sequentially across all sub-SELECTs in a UNION.

pub fn union_positional_placeholder_single_param_each_test() {
  let pg =
    repo.default()
    |> repo.on_placeholder(fn(idx) { "$" <> int.to_string(idx) })

  let employees = sql.table("employees")
  let contractors = sql.table("contractors")

  let employees_query =
    pg
    |> select.from(employees)
    |> select.columns([sql.column("id"), sql.column("name")])
    |> select.where([
      sql.column("department")
      |> sql.eq(db.text("Engineering"), of: sql.val),
    ])

  let contractors_query =
    pg
    |> select.from(contractors)
    |> select.columns([sql.column("id"), sql.column("name")])
    |> select.where([
      sql.column("status")
      |> sql.eq(db.text("available"), of: sql.val),
    ])

  let query =
    union.new(pg, [employees_query, contractors_query])
    |> union.to_query

  let expected =
    "SELECT id, name FROM employees WHERE department = $1 UNION SELECT id, name FROM contractors WHERE status = $2"

  query.sql |> should.equal(expected)
  query.values
  |> should.equal([db.text("Engineering"), db.text("available")])
}

pub fn union_positional_placeholder_multi_param_test() {
  let pg =
    repo.default()
    |> repo.on_placeholder(fn(idx) { "$" <> int.to_string(idx) })

  let employees = sql.table("employees")
  let contractors = sql.table("contractors")

  let employees_query =
    pg
    |> select.from(employees)
    |> select.columns([sql.column("id"), sql.column("name")])
    |> select.where([
      sql.column("department")
        |> sql.eq(db.text("Engineering"), of: sql.val),
      sql.column("active")
        |> sql.eq(db.bool(True), of: sql.val),
    ])

  let contractors_query =
    pg
    |> select.from(contractors)
    |> select.columns([sql.column("id"), sql.column("name")])
    |> select.where([
      sql.column("status")
      |> sql.eq(db.text("available"), of: sql.val),
    ])

  let query =
    union.new(pg, [employees_query, contractors_query])
    |> union.to_query

  // First SELECT uses $1, $2; second SELECT should use $3
  let expected =
    "SELECT id, name FROM employees WHERE department = $1 AND active = $2 UNION SELECT id, name FROM contractors WHERE status = $3"

  query.sql |> should.equal(expected)
  query.values
  |> should.equal([db.text("Engineering"), db.bool(True), db.text("available")])
}

pub fn union_all_positional_placeholder_test() {
  let pg =
    repo.default()
    |> repo.on_placeholder(fn(idx) { "$" <> int.to_string(idx) })

  let users = sql.table("users")
  let employees = sql.table("employees")

  let users_query =
    pg
    |> select.from(users)
    |> select.columns([sql.column("id")])
    |> select.where([
      sql.column("role")
      |> sql.eq(db.text("admin"), of: sql.val),
    ])

  let employees_query =
    pg
    |> select.from(employees)
    |> select.columns([sql.column("id")])
    |> select.where([
      sql.column("level")
      |> sql.eq(db.int(5), of: sql.val),
    ])

  let query =
    union.all(pg, [users_query, employees_query])
    |> union.to_query

  let expected =
    "SELECT id FROM users WHERE role = $1 UNION ALL SELECT id FROM employees WHERE level = $2"

  query.sql |> should.equal(expected)
  query.values |> should.equal([db.text("admin"), db.int(5)])
}

pub fn multi_union_positional_placeholder_test() {
  let pg =
    repo.default()
    |> repo.on_placeholder(fn(idx) { "$" <> int.to_string(idx) })

  let t1 = sql.table("t1")
  let t2 = sql.table("t2")
  let t3 = sql.table("t3")

  let q1 =
    pg
    |> select.from(t1)
    |> select.columns([sql.column("id")])
    |> select.where([
      sql.column("a") |> sql.eq(db.text("x"), of: sql.val),
    ])

  let q2 =
    pg
    |> select.from(t2)
    |> select.columns([sql.column("id")])
    |> select.where([
      sql.column("b") |> sql.eq(db.text("y"), of: sql.val),
      sql.column("c") |> sql.eq(db.text("z"), of: sql.val),
    ])

  let q3 =
    pg
    |> select.from(t3)
    |> select.columns([sql.column("id")])
    |> select.where([
      sql.column("d") |> sql.eq(db.int(42), of: sql.val),
    ])

  let query =
    union.new(pg, [q1, q2, q3])
    |> union.to_query

  // q1: $1, q2: $2 $3, q3: $4
  let expected =
    "SELECT id FROM t1 WHERE a = $1 UNION SELECT id FROM t2 WHERE b = $2 AND c = $3 UNION SELECT id FROM t3 WHERE d = $4"

  query.sql |> should.equal(expected)
  query.values
  |> should.equal([db.text("x"), db.text("y"), db.text("z"), db.int(42)])
}
