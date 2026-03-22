import based/internal/fmt

pub fn placeholder_test() {
  assert fmt.placeholder == ":param:"
}

pub fn select_test() {
  assert fmt.select("id, name") == "SELECT id, name"
}

pub fn select_star_test() {
  assert fmt.select("*") == "SELECT *"
}

pub fn select_distinct_test() {
  assert fmt.select_distinct("name, age") == "SELECT DISTINCT name, age"
}

pub fn insert_test() {
  assert fmt.insert(into: "users", columns: ["name", "age"], values: ["(?, ?)"])
    == "INSERT INTO users (name, age) VALUES (?, ?)"
}

pub fn insert_multiple_rows_test() {
  assert fmt.insert(into: "users", columns: ["name", "age"], values: [
      "(?, ?)",
      "(?, ?)",
    ])
    == "INSERT INTO users (name, age) VALUES (?, ?), (?, ?)"
}

pub fn update_test() {
  assert fmt.update("users") == "UPDATE users"
}

pub fn delete_test() {
  assert fmt.delete(from: "users") == "DELETE FROM users"
}

pub fn from_test() {
  assert fmt.from("SELECT *", "users") == "SELECT * FROM users"
}

pub fn where_test() {
  assert fmt.where("SELECT * FROM users", "id = 1")
    == "SELECT * FROM users WHERE id = 1"
}

pub fn set_test() {
  assert fmt.set("UPDATE users", "name = ?, age = ?")
    == "UPDATE users SET name = ?, age = ?"
}

pub fn on_test() {
  assert fmt.on("INNER JOIN posts", "posts.user_id = users.id")
    == "INNER JOIN posts ON posts.user_id = users.id"
}

pub fn returning_test() {
  assert fmt.returning("INSERT INTO users (name) VALUES (?)", "id")
    == "INSERT INTO users (name) VALUES (?) RETURNING id"
}

pub fn returning_star_test() {
  assert fmt.returning("INSERT INTO users (name) VALUES (?)", "*")
    == "INSERT INTO users (name) VALUES (?) RETURNING *"
}

pub fn group_by_test() {
  assert fmt.group_by(
      "SELECT department, COUNT(*) FROM employees",
      "department",
    )
    == "SELECT department, COUNT(*) FROM employees GROUP BY department"
}

pub fn having_test() {
  assert fmt.having("GROUP BY department", "COUNT(*) > 5")
    == "GROUP BY department HAVING COUNT(*) > 5"
}

pub fn order_by_test() {
  assert fmt.order_by("SELECT * FROM users", "name ASC, id DESC")
    == "SELECT * FROM users ORDER BY name ASC, id DESC"
}

pub fn limit_test() {
  assert fmt.limit("SELECT * FROM users", 25) == "SELECT * FROM users LIMIT 25"
}

pub fn offset_test() {
  assert fmt.offset("SELECT * FROM users LIMIT 25", 50)
    == "SELECT * FROM users LIMIT 25 OFFSET 50"
}

pub fn for_update_test() {
  assert fmt.for_update("SELECT * FROM users WHERE id = 1")
    == "SELECT * FROM users WHERE id = 1 FOR UPDATE"
}

pub fn on_conflict_test() {
  assert fmt.on_conflict("INSERT INTO users (name) VALUES (?)", "name")
    == "INSERT INTO users (name) VALUES (?) ON CONFLICT (name)"
}

pub fn do_nothing_test() {
  assert fmt.do_nothing("ON CONFLICT (name)") == "ON CONFLICT (name) DO NOTHING"
}

pub fn do_update_test() {
  assert fmt.do_update("ON CONFLICT (name)", ["name = EXCLUDED.name"])
    == "ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name"
}

pub fn inner_join_test() {
  assert fmt.inner_join("SELECT * FROM users", "posts")
    == "SELECT * FROM users INNER JOIN posts"
}

pub fn left_join_test() {
  assert fmt.left_join("SELECT * FROM users", "posts")
    == "SELECT * FROM users LEFT JOIN posts"
}

pub fn right_join_test() {
  assert fmt.right_join("SELECT * FROM users", "posts")
    == "SELECT * FROM users RIGHT JOIN posts"
}

pub fn full_join_test() {
  assert fmt.full_join("SELECT * FROM users", "posts")
    == "SELECT * FROM users FULL JOIN posts"
}

pub fn eq_test() {
  assert fmt.eq("name", ":param:") == "name = :param:"
}

pub fn not_eq_test() {
  assert fmt.not_eq("status", "'inactive'") == "status != 'inactive'"
}

pub fn gt_test() {
  assert fmt.gt("age", "18") == "age > 18"
}

pub fn lt_test() {
  assert fmt.lt("price", "100") == "price < 100"
}

pub fn gt_eq_test() {
  assert fmt.gt_eq("age", "21") == "age >= 21"
}

pub fn lt_eq_test() {
  assert fmt.lt_eq("price", "50") == "price <= 50"
}

pub fn like_test() {
  assert fmt.like("name", "'%bob%'") == "name LIKE '%bob%'"
}

pub fn not_like_test() {
  assert fmt.not_like("name", "'%test%'") == "name NOT LIKE '%test%'"
}

pub fn in_test() {
  assert fmt.in_("id", "1, 2, 3") == "id IN (1, 2, 3)"
}

pub fn in_placeholder_test() {
  assert fmt.in_("status", ":param:, :param:") == "status IN (:param:, :param:)"
}

pub fn is_null_test() {
  assert fmt.is_null("email") == "email IS NULL"
}

pub fn is_not_null_test() {
  assert fmt.is_not_null("email") == "email IS NOT NULL"
}

pub fn is_true_test() {
  assert fmt.is_true("active") == "active IS TRUE"
}

pub fn is_false_test() {
  assert fmt.is_false("deleted") == "deleted IS FALSE"
}

pub fn between_test() {
  assert fmt.between("age", "18", "65") == "age BETWEEN 18 AND 65"
}

pub fn between_placeholder_test() {
  assert fmt.between("created_at", ":param:", ":param:")
    == "created_at BETWEEN :param: AND :param:"
}

pub fn not_test() {
  assert fmt.not("active = TRUE") == "NOT (active = TRUE)"
}

pub fn exists_test() {
  assert fmt.exists("SELECT 1 FROM users WHERE id = 1")
    == "EXISTS (SELECT 1 FROM users WHERE id = 1)"
}

pub fn and_op_test() {
  assert fmt.and_op("a = 1", "b = 2") == "(a = 1 AND b = 2)"
}

pub fn or_op_test() {
  assert fmt.or_op("a = 1", "b = 2") == "(a = 1 OR b = 2)"
}

pub fn nested_logical_ops_test() {
  let result = fmt.and_op(fmt.or_op("a = 1", "a = 2"), "b = 3")
  assert result == "((a = 1 OR a = 2) AND b = 3)"
}

pub fn any_test() {
  assert fmt.any("SELECT id FROM users") == "ANY (SELECT id FROM users)"
}

pub fn all_test() {
  assert fmt.all("SELECT id FROM users") == "ALL (SELECT id FROM users)"
}

pub fn subquery_test() {
  assert fmt.subquery("SELECT id FROM users") == "(SELECT id FROM users)"
}

pub fn count_test() {
  assert fmt.count("*") == "COUNT(*)"
}

pub fn count_column_test() {
  assert fmt.count("id") == "COUNT(id)"
}

pub fn sum_test() {
  assert fmt.sum("amount") == "SUM(amount)"
}

pub fn avg_test() {
  assert fmt.avg("price") == "AVG(price)"
}

pub fn max_test() {
  assert fmt.max("score") == "MAX(score)"
}

pub fn min_test() {
  assert fmt.min("score") == "MIN(score)"
}

pub fn with_cte_test() {
  assert fmt.with_cte("active_users AS (SELECT * FROM users WHERE active)")
    == "WITH active_users AS (SELECT * FROM users WHERE active)"
}

pub fn with_recursive_test() {
  assert fmt.with_recursive("tree AS (SELECT * FROM nodes)")
    == "WITH RECURSIVE tree AS (SELECT * FROM nodes)"
}

pub fn cte_test() {
  assert fmt.cte("active_users", "SELECT * FROM users WHERE active")
    == "active_users AS (SELECT * FROM users WHERE active)"
}

pub fn union_test() {
  assert fmt.union("SELECT * FROM a", "SELECT * FROM b")
    == "SELECT * FROM a UNION SELECT * FROM b"
}

pub fn union_all_test() {
  assert fmt.union_all("SELECT * FROM a", "SELECT * FROM b")
    == "SELECT * FROM a UNION ALL SELECT * FROM b"
}

pub fn enclose_test() {
  assert fmt.enclose("a + b") == "(a + b)"
}

pub fn alias_as_test() {
  assert fmt.alias_as("users", "u") == "users AS u"
}

pub fn alias_as_expression_test() {
  assert fmt.alias_as("COUNT(*)", "total") == "COUNT(*) AS total"
}

pub fn asc_test() {
  assert fmt.asc("name") == "name ASC"
}

pub fn desc_test() {
  assert fmt.desc("created_at") == "created_at DESC"
}

pub fn terminate_test() {
  assert fmt.terminate("SELECT * FROM users") == "SELECT * FROM users;"
}

pub fn comma_join_test() {
  assert fmt.comma_join(["id", "name", "email"]) == "id, name, email"
}

pub fn comma_join_single_test() {
  assert fmt.comma_join(["id"]) == "id"
}

pub fn comma_join_empty_test() {
  assert fmt.comma_join([]) == ""
}

pub fn value_row_test() {
  assert fmt.value_row(":param:, :param:") == "(:param:, :param:)"
}

pub fn full_select_query_test() {
  let result =
    fmt.select("id, name, email")
    |> fmt.from("users")
    |> fmt.where(fmt.and_op("active = TRUE", "age > 18"))
    |> fmt.order_by("name ASC")
    |> fmt.limit(10)
    |> fmt.offset(20)

  assert result
    == "SELECT id, name, email FROM users WHERE (active = TRUE AND age > 18) ORDER BY name ASC LIMIT 10 OFFSET 20"
}

pub fn full_select_distinct_query_test() {
  let result =
    fmt.select_distinct("department")
    |> fmt.from("employees")
    |> fmt.where("active = TRUE")
    |> fmt.order_by("department ASC")

  assert result
    == "SELECT DISTINCT department FROM employees WHERE active = TRUE ORDER BY department ASC"
}

pub fn full_update_query_test() {
  let result =
    fmt.update("users")
    |> fmt.set("name = :param:, age = :param:")
    |> fmt.where("id = :param:")
    |> fmt.returning("*")

  assert result
    == "UPDATE users SET name = :param:, age = :param: WHERE id = :param: RETURNING *"
}

pub fn full_delete_query_test() {
  let result =
    fmt.delete(from: "users")
    |> fmt.where("id = :param:")
    |> fmt.returning("id")

  assert result == "DELETE FROM users WHERE id = :param: RETURNING id"
}

pub fn select_with_join_test() {
  let result =
    fmt.select("u.id, u.name, p.title")
    |> fmt.from("users u")
    |> fmt.inner_join("posts p")
    |> fmt.on("p.user_id = u.id")
    |> fmt.where("u.active = TRUE")

  assert result
    == "SELECT u.id, u.name, p.title FROM users u INNER JOIN posts p ON p.user_id = u.id WHERE u.active = TRUE"
}

pub fn select_with_left_join_test() {
  let result =
    fmt.select("u.name, COUNT(p.id)")
    |> fmt.from("users u")
    |> fmt.left_join("posts p")
    |> fmt.on("p.user_id = u.id")
    |> fmt.group_by("u.name")
    |> fmt.having("COUNT(p.id) > 0")

  assert result
    == "SELECT u.name, COUNT(p.id) FROM users u LEFT JOIN posts p ON p.user_id = u.id GROUP BY u.name HAVING COUNT(p.id) > 0"
}

pub fn insert_with_conflict_do_nothing_test() {
  let result =
    fmt.insert(into: "users", columns: ["name", "email"], values: [
      "(:param:, :param:)",
    ])
    |> fmt.on_conflict("email")
    |> fmt.do_nothing()

  assert result
    == "INSERT INTO users (name, email) VALUES (:param:, :param:) ON CONFLICT (email) DO NOTHING"
}

pub fn insert_with_conflict_do_update_test() {
  let result =
    fmt.insert(into: "users", columns: ["name", "email"], values: [
      "(:param:, :param:)",
    ])
    |> fmt.on_conflict("email")
    |> fmt.do_update(["name = EXCLUDED.name"])

  assert result
    == "INSERT INTO users (name, email) VALUES (:param:, :param:) ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name"
}

pub fn select_for_update_test() {
  let result =
    fmt.select("*")
    |> fmt.from("accounts")
    |> fmt.where("id = :param:")
    |> fmt.for_update()

  assert result == "SELECT * FROM accounts WHERE id = :param: FOR UPDATE"
}

pub fn cte_with_select_test() {
  let cte_body = fmt.cte("active_users", "SELECT * FROM users WHERE active")
  let result = fmt.with_cte(cte_body) <> " SELECT * FROM active_users"

  assert result
    == "WITH active_users AS (SELECT * FROM users WHERE active) SELECT * FROM active_users"
}

pub fn union_composition_test() {
  let left =
    fmt.select("name")
    |> fmt.from("employees")
  let right =
    fmt.select("name")
    |> fmt.from("contractors")

  assert fmt.union(left, right)
    == "SELECT name FROM employees UNION SELECT name FROM contractors"
}

pub fn union_all_composition_test() {
  let left =
    fmt.select("name")
    |> fmt.from("employees")
  let right =
    fmt.select("name")
    |> fmt.from("contractors")

  assert fmt.union_all(left, right)
    == "SELECT name FROM employees UNION ALL SELECT name FROM contractors"
}

pub fn complex_where_composition_test() {
  let condition =
    fmt.and_op(
      fmt.or_op(fmt.eq("status", "'active'"), fmt.eq("status", "'pending'")),
      fmt.gt("age", "18"),
    )

  assert condition == "((status = 'active' OR status = 'pending') AND age > 18)"
}

pub fn aggregate_with_alias_test() {
  let col = fmt.alias_as(fmt.count("*"), "total")
  assert col == "COUNT(*) AS total"
}

pub fn comma_join_with_aggregates_test() {
  let columns =
    fmt.comma_join([
      "department",
      fmt.alias_as(fmt.count("*"), "total"),
      fmt.alias_as(fmt.avg("salary"), "avg_salary"),
    ])

  assert columns == "department, COUNT(*) AS total, AVG(salary) AS avg_salary"
}

pub fn order_by_with_directions_test() {
  let ordering = fmt.comma_join([fmt.asc("name"), fmt.desc("created_at")])
  let result =
    fmt.select("*")
    |> fmt.from("users")
    |> fmt.order_by(ordering)

  assert result == "SELECT * FROM users ORDER BY name ASC, created_at DESC"
}

pub fn terminated_query_test() {
  let result =
    fmt.select("*")
    |> fmt.from("users")
    |> fmt.terminate()

  assert result == "SELECT * FROM users;"
}

pub fn subquery_in_where_test() {
  let sub = fmt.subquery(fmt.select("id") |> fmt.from("admins"))
  let result =
    fmt.select("*")
    |> fmt.from("users")
    |> fmt.where(fmt.in_("id", sub))

  assert result == "SELECT * FROM users WHERE id IN ((SELECT id FROM admins))"
}

pub fn exists_subquery_test() {
  let sub =
    fmt.select("1")
    |> fmt.from("orders")
    |> fmt.where("orders.user_id = users.id")

  let result =
    fmt.select("*")
    |> fmt.from("users")
    |> fmt.where(fmt.exists(sub))

  assert result
    == "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)"
}

pub fn value_row_composition_test() {
  let rows =
    fmt.comma_join([
      fmt.value_row(":param:, :param:"),
      fmt.value_row(":param:, :param:"),
    ])

  assert rows == "(:param:, :param:), (:param:, :param:)"
}
