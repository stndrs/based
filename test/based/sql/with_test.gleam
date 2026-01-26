import based/sql
import based/sql/column
import based/sql/select
import based/sql/table
import based/sql/union
import based/sql/with
import based/value

pub fn with_test() {
  let expected =
    "WITH departments AS (SELECT e.name, d.name FROM employees AS e INNER JOIN departments AS d ON e.dept_id = d.id) SELECT * FROM departments AS d WHERE d.name = ?;"

  let departments = sql.table("departments") |> table.alias("d")
  let employees = sql.table("employees") |> table.alias("e")

  let employees_select =
    value.repo()
    |> select.from(employees)
    |> select.columns([
      sql.column("name") |> column.for(employees),
      sql.column("name") |> column.for(departments),
    ])
    |> select.join(departments, on: [
      sql.column("dept_id")
      |> column.for(employees)
      |> sql.eq(sql.column("id") |> column.for(departments), of: sql.col),
    ])
    |> select.to_query

  let query =
    value.repo()
    |> with.new([with.cte("departments", employees_select)])
    |> with.query(fn(repo) {
      select.from(repo, departments)
      |> select.columns([column.all])
      |> select.where([
        sql.column("name")
        |> column.for(departments)
        |> sql.eq(value.text("Engineering"), of: sql.val),
      ])
      |> select.to_query
    })
    |> with.to_query

  assert expected == query.sql
  assert [value.text("Engineering")] == query.values
}

pub fn with_multiple_ctes_test() {
  let expected =
    "WITH departments AS (SELECT id, name FROM departments), employees AS (SELECT id, name, dept_id FROM employees) SELECT e.name, d.name FROM employees AS e INNER JOIN departments AS d ON e.dept_id = d.id;"

  let departments = sql.table("departments")
  let employees = sql.table("employees")

  let deps_select =
    value.repo()
    |> select.from(departments)
    |> select.columns([sql.column("id"), sql.column("name")])
    |> select.to_query

  let employees_select =
    value.repo()
    |> select.from(employees)
    |> select.columns([
      sql.column("id"),
      sql.column("name"),
      sql.column("dept_id"),
    ])
    |> select.to_query

  let employees = table.alias(employees, "e")
  let departments = table.alias(departments, "d")

  let query =
    value.repo()
    |> with.new([
      with.cte("departments", deps_select),
      with.cte("employees", employees_select),
    ])
    |> with.query(fn(repo) {
      select.from(repo, employees)
      |> select.columns([
        sql.column("name") |> column.for(employees),
        sql.column("name") |> column.for(departments),
      ])
      |> select.join(departments, on: [
        sql.column("e.dept_id")
        |> sql.eq(sql.column("d.id"), of: sql.col),
      ])
      |> select.to_query
    })
    |> with.to_query

  assert expected == query.sql
  assert [] == query.values
}

pub fn with_column_names_test() {
  let expected =
    "WITH departments (dept_id, dept_name) AS (SELECT id, name FROM departments) SELECT dept_name FROM departments WHERE dept_id = ?;"
  let departments = sql.table("departments")

  let deps_select =
    value.repo()
    |> select.from(departments)
    |> select.columns([sql.column("id"), sql.column("name")])
    |> select.to_query

  let query =
    value.repo()
    |> with.new([
      with.cte("departments", deps_select)
      |> with.columns(["dept_id", "dept_name"]),
    ])
    |> with.query(fn(repo) {
      select.from(repo, departments)
      |> select.columns([sql.column("dept_name")])
      |> select.where([
        sql.column("dept_id")
        |> sql.eq(value.int(42), of: sql.val),
      ])
      |> select.to_query
    })
    |> with.to_query

  assert expected == query.sql
  assert [value.int(42)] == query.values
}

pub fn recursive_with_test() {
  let expected =
    "WITH RECURSIVE numbers (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM numbers WHERE n < ?) SELECT n FROM numbers;"

  let base_query = select.new(value.repo()) |> select.columns([sql.column("1")])

  let numbers = sql.table("numbers")

  let recursive_query =
    value.repo()
    |> select.from(numbers)
    |> select.columns([sql.column("n + 1")])
    |> select.where([
      sql.column("n")
      |> sql.lt(value.int(5), of: sql.val),
    ])

  let union_all =
    union.all([base_query, recursive_query])
    |> union.to_query

  let query =
    value.repo()
    |> with.new([with.cte("numbers", union_all) |> with.columns(["n"])])
    |> with.recursive
    |> with.query(fn(repo) {
      select.from(repo, numbers)
      |> select.columns([sql.column("n")])
      |> select.to_query
    })
    |> with.to_query

  assert expected == query.sql
  assert [value.int(5)] == query.values
}

pub fn with_union_test() {
  let expected =
    "WITH combined_data AS (SELECT id, name FROM users UNION SELECT id, username FROM accounts) SELECT * FROM combined_data ORDER BY id;"
  let users = sql.table("users")
  let accounts = sql.table("accounts")
  let combined_data = sql.table("combined_data")

  let users_select =
    value.repo()
    |> select.from(users)
    |> select.columns([sql.column("id"), sql.column("name")])

  let accounts_select =
    value.repo()
    |> select.from(accounts)
    |> select.columns([sql.column("id"), sql.column("username")])

  let union_query =
    union.new([users_select, accounts_select])
    |> union.to_query

  let query =
    value.repo()
    |> with.new([with.cte("combined_data", union_query)])
    |> with.query(fn(repo) {
      select.from(repo, combined_data)
      |> select.columns([column.all])
      |> select.order_by(["id"])
      |> select.to_query
    })
    |> with.to_query

  assert expected == query.sql
  assert [] == query.values
}
