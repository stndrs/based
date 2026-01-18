import based/sql
import based/sql/select
import based/sql/union
import based/sql/with
import based/value
import gleeunit/should

pub fn with_test() {
  let expected =
    "WITH departments AS (SELECT e.name, d.name FROM employees AS e INNER JOIN departments AS d ON e.dept_id = d.id) SELECT * FROM departments WHERE name = ?;"

  let departments = sql.identifier("departments")
  let employees = sql.identifier("employees") |> sql.alias("e")

  let employees_select =
    value.repo()
    |> select.from(employees)
    |> select.columns(["e.name", "d.name"])
    |> select.join(sql.alias(departments, "d"), on: [
      sql.identifier("e.dept_id")
      |> sql.column
      |> sql.eq(sql.identifier("d.id") |> sql.column),
    ])
    |> select.to_query

  let query =
    value.repo()
    |> with.new([with.cte("departments", employees_select)])
    |> with.query(fn(sql) {
      select.from(sql, departments)
      |> select.columns(["*"])
      |> select.where([
        sql.identifier("name")
        |> sql.column
        |> sql.eq(sql.value(value.text("Engineering"))),
      ])
      |> select.to_query
    })
    |> with.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("Engineering")])
}

pub fn with_multiple_ctes_test() {
  let expected =
    "WITH departments AS (SELECT id, name FROM departments), employees AS (SELECT id, name, dept_id FROM employees) SELECT e.name, d.name FROM employees AS e INNER JOIN departments AS d ON e.dept_id = d.id;"

  let departments = sql.identifier("departments")
  let employees = sql.identifier("employees")

  let deps_select =
    value.repo()
    |> select.from(departments)
    |> select.columns(["id", "name"])
    |> select.to_query

  let employees_select =
    value.repo()
    |> select.from(employees)
    |> select.columns(["id", "name", "dept_id"])
    |> select.to_query

  let query =
    value.repo()
    |> with.new([
      with.cte("departments", deps_select),
      with.cte("employees", employees_select),
    ])
    |> with.query(fn(sql) {
      select.from(sql, sql.alias(employees, "e"))
      |> select.columns(["e.name", "d.name"])
      |> select.join(sql.alias(departments, "d"), on: [
        sql.identifier("e.dept_id")
        |> sql.column
        |> sql.eq(sql.identifier("d.id") |> sql.column),
      ])
      |> select.to_query
    })
    |> with.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn with_column_names_test() {
  let expected =
    "WITH departments (dept_id, dept_name) AS (SELECT id, name FROM departments) SELECT dept_name FROM departments WHERE dept_id = ?;"
  let departments = sql.identifier("departments")

  let deps_select =
    value.repo()
    |> select.from(departments)
    |> select.columns(["id", "name"])
    |> select.to_query

  let query =
    value.repo()
    |> with.new([
      with.cte("departments", deps_select)
      |> with.columns(["dept_id", "dept_name"]),
    ])
    |> with.query(fn(sql) {
      select.from(sql, departments)
      |> select.columns(["dept_name"])
      |> select.where([
        sql.identifier("dept_id")
        |> sql.column
        |> sql.eq(sql.value(value.int(42))),
      ])
      |> select.to_query
    })
    |> with.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(42)])
}

pub fn recursive_with_test() {
  let expected =
    "WITH RECURSIVE numbers (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM numbers WHERE n < ?) SELECT n FROM numbers;"

  let base_query = select.new(value.repo()) |> select.columns(["1"])

  let numbers = sql.identifier("numbers")

  let recursive_query =
    value.repo()
    |> select.from(numbers)
    |> select.columns(["n + 1"])
    |> select.where([
      sql.identifier("n")
      |> sql.column
      |> sql.lt(sql.value(value.int(5))),
    ])

  let union_all =
    union.all([base_query, recursive_query])
    |> union.to_query

  let query =
    value.repo()
    |> with.new([with.cte("numbers", union_all) |> with.columns(["n"])])
    |> with.recursive
    |> with.query(fn(sql) {
      select.from(sql, numbers)
      |> select.columns(["n"])
      |> select.to_query
    })
    |> with.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(5)])
}

pub fn with_union_test() {
  let expected =
    "WITH combined_data AS (SELECT id, name FROM users UNION SELECT id, username FROM accounts) SELECT * FROM combined_data ORDER BY id;"
  let users = sql.identifier("users")
  let accounts = sql.identifier("accounts")
  let combined_data = sql.identifier("combined_data")

  let users_select =
    value.repo()
    |> select.from(users)
    |> select.columns(["id", "name"])

  let accounts_select =
    value.repo()
    |> select.from(accounts)
    |> select.columns(["id", "username"])

  let union_query =
    union.new([users_select, accounts_select])
    |> union.to_query

  let query =
    value.repo()
    |> with.new([with.cte("combined_data", union_query)])
    |> with.query(fn(sql) {
      select.from(sql, combined_data)
      |> select.columns(["*"])
      |> select.order_by(["id"])
      |> select.to_query
    })
    |> with.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}
