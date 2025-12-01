import based/sql
import based/sql/select
import based/sql/union
import based/sql/with
import based/value
import gleeunit/should

pub fn with_test() {
  let expected =
    "WITH departments AS (SELECT e.name, d.name FROM employees AS e INNER JOIN departments AS d ON e.dept_id = d.id) SELECT * FROM departments WHERE name = ?;"

  let departments = sql.name("departments")
  let employees = sql.name("employees") |> sql.alias("e") |> sql.table

  let employees_select =
    select.from(employees)
    |> select.columns(["e.name", "d.name"])
    |> select.join(sql.alias(departments, "d") |> sql.table, [
      sql.name("e.dept_id")
      |> sql.column
      |> sql.eq(sql.name("d.id") |> sql.column),
    ])
    |> select.to_query(value.format())

  let query =
    with.new([with.cte("departments", employees_select)])
    |> with.query(fn() {
      select.from(sql.table(departments))
      |> select.columns(["*"])
      |> select.where([
        sql.name("name")
        |> sql.column
        |> sql.eq(sql.value("Engineering", of: value.text)),
      ])
      |> select.to_query(value.format())
    })
    |> with.to_query(sql.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("Engineering")])
}

pub fn with_multiple_ctes_test() {
  let expected =
    "WITH departments AS (SELECT id, name FROM departments), employees AS (SELECT id, name, dept_id FROM employees) SELECT e.name, d.name FROM employees AS e INNER JOIN departments AS d ON e.dept_id = d.id;"

  let departments = sql.name("departments")
  let employees = sql.name("employees")

  let deps_select =
    select.from(sql.table(departments))
    |> select.columns(["id", "name"])
    |> select.to_query(value.format())

  let employees_select =
    select.from(sql.table(employees))
    |> select.columns(["id", "name", "dept_id"])
    |> select.to_query(value.format())

  let query =
    with.new([
      with.cte("departments", deps_select),
      with.cte("employees", employees_select),
    ])
    |> with.query(fn() {
      select.from(sql.table(sql.alias(employees, "e")))
      |> select.columns(["e.name", "d.name"])
      |> select.join(sql.table(sql.alias(departments, "d")), [
        sql.name("e.dept_id")
        |> sql.column
        |> sql.eq(sql.name("d.id") |> sql.column),
      ])
      |> select.to_query(value.format())
    })
    |> with.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn with_column_names_test() {
  let expected =
    "WITH departments (dept_id, dept_name) AS (SELECT id, name FROM departments) SELECT dept_name FROM departments WHERE dept_id = ?;"
  let departments = sql.name("departments") |> sql.table

  let deps_select =
    select.from(departments)
    |> select.columns(["id", "name"])
    |> select.to_query(value.format())

  let query =
    with.new([
      with.cte("departments", deps_select)
      |> with.columns(["dept_id", "dept_name"]),
    ])
    |> with.query(fn() {
      select.from(departments)
      |> select.columns(["dept_name"])
      |> select.where([
        sql.name("dept_id")
        |> sql.column
        |> sql.eq(sql.value(42, of: value.int)),
      ])
      |> select.to_query(value.format())
    })
    |> with.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(42)])
}

pub fn recursive_with_test() {
  let expected =
    "WITH RECURSIVE numbers (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM numbers WHERE n < ?) SELECT n FROM numbers;"

  let base_query = select.new() |> select.columns(["1"])

  let numbers = sql.name("numbers") |> sql.table

  let recursive_query =
    select.from(numbers)
    |> select.columns(["n + 1"])
    |> select.where([
      sql.name("n")
      |> sql.column
      |> sql.lt(sql.value(5, of: value.int)),
    ])

  let union_all =
    union.all([base_query, recursive_query])
    |> union.to_query(value.format())

  let query =
    with.new([with.cte("numbers", union_all) |> with.columns(["n"])])
    |> with.recursive
    |> with.query(fn() {
      select.from(numbers)
      |> select.columns(["n"])
      |> select.to_query(value.format())
    })
    |> with.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(5)])
}

pub fn with_union_test() {
  let expected =
    "WITH combined_data AS (SELECT id, name FROM users UNION SELECT id, username FROM accounts) SELECT * FROM combined_data ORDER BY id;"
  let users = sql.name("users") |> sql.table
  let accounts = sql.name("accounts") |> sql.table
  let combined_data = sql.name("combined_data") |> sql.table

  let users_select =
    select.from(users)
    |> select.columns(["id", "name"])

  let accounts_select =
    select.from(accounts)
    |> select.columns(["id", "username"])

  let union_query =
    union.new([users_select, accounts_select])
    |> union.to_query(value.format())

  let query =
    with.new([with.cte("combined_data", union_query)])
    |> with.query(fn() {
      select.from(combined_data)
      |> select.columns(["*"])
      |> select.order_by(["id"])
      |> select.to_query(value.format())
    })
    |> with.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}
