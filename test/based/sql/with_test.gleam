import based/sql
import based/sql/select
import based/sql/table
import based/sql/union
import based/sql/with
import based/value
import gleeunit/should

pub fn with_test() {
  let expected =
    "WITH departments AS (SELECT e.name, d.name FROM employees e INNER JOIN departments AS d ON e.dept_id = d.id) SELECT * FROM departments WHERE name = ?;"

  let deps = sql.table("departments")
  let employees = sql.table("employees e")

  let departments = sql.table("departments") |> table.alias("d")

  let employees_select =
    sql.select(["e.name", "d.name"])
    |> select.from(employees)
    |> select.join(departments, [
      sql.column("e.dept_id") |> sql.eq(sql.column("d.id")),
    ])
    |> select.to_query(value.format())

  let query =
    sql.with([with.cte("departments", employees_select)])
    |> with.query(fn() {
      sql.select(["*"])
      |> select.from(deps)
      |> select.where([sql.column("name") |> sql.eq(sql.text("Engineering"))])
      |> select.to_query(value.format())
    })
    |> with.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("Engineering")])
}

pub fn with_multiple_ctes_test() {
  let expected =
    "WITH departments AS (SELECT id, name FROM departments), employees AS (SELECT id, name, dept_id FROM employees) SELECT e.name, d.name FROM employees e INNER JOIN departments AS d ON e.dept_id = d.id;"

  let deps = sql.table("departments")
  let employees = sql.table("employees")

  let departments = sql.table("departments") |> table.alias("d")

  let deps_select =
    sql.select(["id", "name"])
    |> select.from(deps)
    |> select.to_query(value.format())

  let employees_select =
    sql.select(["id", "name", "dept_id"])
    |> select.from(employees)
    |> select.to_query(value.format())

  let query =
    sql.with([
      with.cte("departments", deps_select),
      with.cte("employees", employees_select),
    ])
    |> with.query(fn() {
      sql.select(["e.name", "d.name"])
      |> select.from(sql.table("employees e"))
      |> select.join(departments, [
        sql.column("e.dept_id") |> sql.eq(sql.column("d.id")),
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

  let deps = sql.table("departments")

  let deps_select =
    sql.select(["id", "name"])
    |> select.from(deps)
    |> select.to_query(value.format())

  let query =
    sql.with([
      with.cte("departments", deps_select)
      |> with.columns(["dept_id", "dept_name"]),
    ])
    |> with.query(fn() {
      sql.select(["dept_name"])
      |> select.from(deps)
      |> select.where([sql.column("dept_id") |> sql.eq(sql.int(42))])
      |> select.to_query(value.format())
    })
    |> with.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(42)])
}

pub fn recursive_with_test() {
  let expected =
    "WITH RECURSIVE numbers (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM numbers WHERE n < ?) SELECT n FROM numbers;"

  let base_query = sql.select(["1"])

  let recursive_query =
    sql.select(["n + 1"])
    |> select.from(sql.table("numbers"))
    |> select.where([sql.column("n") |> sql.lt(sql.int(5))])

  let union_all =
    sql.union_all([base_query, recursive_query])
    |> union.to_query(value.format())

  let query =
    sql.with([with.cte("numbers", union_all) |> with.columns(["n"])])
    |> with.recursive
    |> with.query(fn() {
      sql.select(["n"])
      |> select.from(sql.table("numbers"))
      |> select.to_query(value.format())
    })
    |> with.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(5)])
}

pub fn with_union_test() {
  let expected =
    "WITH combined_data AS (SELECT id, name FROM users UNION SELECT id, username FROM accounts) SELECT * FROM combined_data ORDER BY id;"

  let users_select =
    sql.select(["id", "name"])
    |> select.from(sql.table("users"))

  let accounts_select =
    sql.select(["id", "username"])
    |> select.from(sql.table("accounts"))

  let union_query =
    sql.union([users_select, accounts_select])
    |> union.to_query(value.format())

  let query =
    sql.with([with.cte("combined_data", union_query)])
    |> with.query(fn() {
      sql.select(["*"])
      |> select.from(sql.table("combined_data"))
      |> select.order_by(["id"])
      |> select.to_query(value.format())
    })
    |> with.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}
