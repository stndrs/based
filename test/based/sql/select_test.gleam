import based/db
import based/interval
import based/repo
import based/sql
import based/sql/column
import based/sql/select
import gleam/int
import gleam/time/calendar
import gleam/time/timestamp

pub fn select_test() {
  let expected = "SELECT id, name FROM users"

  let users = sql.table("users")

  let query =
    repo.default()
    |> select.from(users)
    |> select.columns([sql.column("id"), sql.column("name")])
    |> select.to_query

  assert expected == query.sql
  assert [] == query.values
}

pub fn select_alias_test() {
  let expected = "SELECT user_id AS id FROM user_posts"
  let user_posts = sql.table("user_posts")

  let query =
    repo.default()
    |> select.from(user_posts)
    |> select.columns([
      sql.column("user_id")
      |> column.alias("id"),
    ])
    |> select.to_query

  assert expected == query.sql
}

pub fn select_distincts_test() {
  let expected = "SELECT DISTINCT id, name FROM users"
  let users = sql.table("users")

  let query =
    repo.default()
    |> select.from(users)
    |> select.columns([sql.column("id"), sql.column("name")])
    |> select.distinct
    |> select.to_query

  assert expected == query.sql
  assert [] == query.values
}

pub fn select_where_test() {
  let expected = "SELECT title FROM posts WHERE created_at > ? AND user_id = ?"

  let users = sql.table("posts")

  let query =
    repo.default()
    |> select.from(users)
    |> select.columns([sql.column("title")])
    |> select.where([
      sql.column("created_at")
        |> sql.gt(db.text("2024-01-01"), of: sql.val),

      sql.column("user_id")
        |> sql.eq(db.int(10), of: sql.val),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [db.text("2024-01-01"), db.int(10)] == query.values
}

pub fn select_subquery_test() {
  let expected =
    "SELECT title FROM posts WHERE created_at > ? AND user_id = (SELECT id FROM users WHERE name = ?)"
  let users = sql.table("users")
  let posts = sql.table("posts")

  let subquery =
    repo.default()
    |> select.from(users)
    |> select.columns([sql.column("id")])
    |> select.where([
      sql.column("name")
      |> sql.eq(db.text("Human Person"), of: sql.val),
    ])

  let query =
    repo.default()
    |> select.from(posts)
    |> select.columns([sql.column("title")])
    |> select.where([
      sql.column("created_at")
        |> sql.gt(db.text("2024-01-01"), of: sql.val),
      sql.column("user_id")
        |> sql.eq(subquery, of: select.subquery),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [db.text("2024-01-01"), db.text("Human Person")] == query.values
}

pub fn select_or_test() {
  let expected = "SELECT * FROM users WHERE name = ? OR email = ?"
  let users = sql.table("users")

  let query =
    repo.default()
    |> select.from(users)
    |> select.where([
      sql.column("name")
      |> sql.eq(db.text("Human Person"), of: sql.val)
      |> sql.or(
        sql.column("email")
        |> sql.eq(db.text("human.person@example.com"), of: sql.val),
      ),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [
      db.text("Human Person"),
      db.text("human.person@example.com"),
    ]
    == query.values
}

pub fn select_where_not_test() {
  let expected = "SELECT * FROM users WHERE NOT email = ?"
  let users = sql.table("users")

  let query =
    repo.default()
    |> select.from(users)
    |> select.where_not([
      sql.column("email")
      |> sql.eq(db.text("Human Person"), of: sql.val),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [db.text("Human Person")] == query.values
}

pub fn select_where_not_like_test() {
  let expected = "SELECT * FROM users WHERE email NOT LIKE ?"
  let users = sql.table("users")

  let query =
    repo.default()
    |> select.from(users)
    |> select.where([
      sql.column("email")
      |> sql.not_like("Human Person"),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [db.text("Human Person")] == query.values
}

pub fn select_distinct_test() {
  let expected = "SELECT DISTINCT value FROM users"
  let users = sql.table("users")

  let query =
    repo.default()
    |> select.from(users)
    |> select.columns([sql.column("value")])
    |> select.distinct
    |> select.to_query

  assert expected == query.sql
  assert [] == query.values
}

pub fn select_with_join_test() {
  let expected =
    "SELECT * FROM users INNER JOIN posts ON users.id = posts.user_id AND posts.title LIKE ?"

  let users = sql.table("users")
  let posts = sql.table("posts")

  let query =
    repo.default()
    |> select.from(users)
    |> select.join(posts, on: [
      sql.column("users.id")
        |> sql.eq(sql.column("posts.user_id"), of: sql.col),
      sql.column("posts.title")
        |> sql.like("%gleam%"),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [db.text("%gleam%")] == query.values
}

pub fn select_with_multiple_joins_test() {
  let expected =
    "SELECT * FROM users INNER JOIN posts ON users.id = posts.user_id AND posts.title LIKE ? LEFT JOIN comments ON posts.comment_id = comments.id RIGHT JOIN tags ON tags.id = posts.tag_id FULL OUTER JOIN followers ON followers.user_id = users.id"

  let users = sql.table("users")
  let posts = sql.table("posts")
  let comments = sql.table("comments")
  let tags = sql.table("tags")
  let followers = sql.table("followers")

  let query =
    repo.default()
    |> select.from(users)
    |> select.join(posts, on: [
      sql.column("users.id")
        |> sql.eq(sql.column("posts.user_id"), of: sql.col),
      sql.column("posts.title")
        |> sql.like("%gleam%"),
    ])
    |> select.left_join(comments, on: [
      sql.column("posts.comment_id")
      |> sql.eq(sql.column("comments.id"), of: sql.col),
    ])
    |> select.right_join(tags, on: [
      sql.column("tags.id")
      |> sql.eq(sql.column("posts.tag_id"), of: sql.col),
    ])
    |> select.full_join(followers, on: [
      sql.column("followers.user_id")
      |> sql.eq(sql.column("users.id"), of: sql.col),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [db.text("%gleam%")] == query.values
}

pub fn select_with_in_test() {
  let expected = "SELECT * FROM users WHERE id IN (?, ?, ?)"
  let users = sql.table("users")

  let query =
    repo.default()
    |> select.from(users)
    |> select.where([
      sql.column("id")
      |> sql.in([1, 2, 3], of: sql.list(of: db.int)),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [db.int(1), db.int(2), db.int(3)] == query.values
}

pub fn select_with_is_null_test() {
  let expected = "SELECT * FROM users WHERE deleted_at IS NULL"
  let users = sql.table("users")

  let query =
    repo.default()
    |> select.from(users)
    |> select.where([sql.column("deleted_at") |> sql.is_null])
    |> select.to_query

  assert expected == query.sql
  assert [] == query.values
}

pub fn select_with_is_true_test() {
  let expected = "SELECT * FROM users WHERE deleted_at IS TRUE"
  let users = sql.table("users")

  let query =
    repo.default()
    |> select.from(users)
    |> select.where([sql.column("deleted_at") |> column.is(True)])
    |> select.to_query

  assert expected == query.sql
  assert [] == query.values
}

pub fn select_with_is_not_null_test() {
  let expected = "SELECT * FROM users WHERE active IS NOT NULL"
  let users = sql.table("users")

  let query =
    repo.default()
    |> select.from(users)
    |> select.where([sql.column("active") |> column.is_not_null])
    |> select.to_query

  assert expected == query.sql
  assert [] == query.values
}

pub fn select_with_sql_is_true_test() {
  let expected = "SELECT * FROM users WHERE active IS TRUE"
  let users = sql.table("users")

  let query =
    repo.default()
    |> select.from(users)
    |> select.where([sql.is(sql.column("active"), True)])
    |> select.to_query

  assert expected == query.sql
  assert [] == query.values
}

pub fn select_with_sql_is_false_test() {
  let expected = "SELECT * FROM users WHERE active IS FALSE"
  let users = sql.table("users")

  let query =
    repo.default()
    |> select.from(users)
    |> select.where([sql.is(sql.column("active"), False)])
    |> select.to_query

  assert expected == query.sql
  assert [] == query.values
}

pub fn select_with_sql_is_not_null_test() {
  let expected = "SELECT * FROM users WHERE active IS NOT NULL"
  let users = sql.table("users")

  let query =
    repo.default()
    |> select.from(users)
    |> select.where([sql.is_not_null(sql.column("active"))])
    |> select.to_query

  assert expected == query.sql
  assert [] == query.values
}

pub fn select_wildcard_test() {
  let expected = "SELECT * FROM users"
  let users = sql.table("users")

  let query =
    repo.default()
    |> select.from(users)
    |> select.to_query

  assert expected == query.sql
  assert [] == query.values
}

pub fn where_lt_test() {
  let expected = "SELECT * FROM users WHERE age < ?"
  let users = sql.table("users")

  let query =
    repo.default()
    |> select.from(users)
    |> select.where([
      sql.column("age")
      |> sql.lt(db.int(65), of: sql.val),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [db.int(65)] == query.values
}

pub fn where_lt_eq_test() {
  let expected = "SELECT * FROM users WHERE age <= ?"
  let users = sql.table("users")

  let query =
    repo.default()
    |> select.from(users)
    |> select.where([
      sql.column("age")
      |> sql.lt_eq(db.int(65), of: sql.val),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [db.int(65)] == query.values
}

pub fn where_not_eq_test() {
  let expected = "SELECT * FROM users WHERE status <> ?"
  let users = sql.table("users")

  let query =
    repo.default()
    |> select.from(users)
    |> select.where([
      sql.column("status")
      |> sql.not_eq(db.text("inactive"), of: sql.val),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [db.text("inactive")] == query.values
}

pub fn multiple_where_test() {
  let expected = "SELECT * FROM users WHERE age > ? AND status = ?"
  let users = sql.table("users")

  let query =
    repo.default()
    |> select.from(users)
    |> select.where([
      sql.column("age")
        |> sql.gt(db.int(18), of: sql.val),
      sql.column("status")
        |> sql.eq(db.text("active"), of: sql.val),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [db.int(18), db.text("active")] == query.values
}

pub fn join_with_multiple_conditions_to_string_test() {
  let expected =
    "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id AND orders.status = 'completed' WHERE users.age > 20 AND users.active = TRUE"

  let users = sql.table("users")
  let orders = sql.table("orders")

  let query =
    repo.default()
    |> select.from(users)
    |> select.join(orders, on: [
      sql.column("users.id")
        |> sql.eq(sql.column("orders.user_id"), of: sql.col),
      sql.column("orders.status")
        |> sql.eq(db.text("completed"), of: sql.val),
    ])
    |> select.where([
      sql.column("users.age")
        |> sql.gt(db.int(20), of: sql.val),
      sql.column("users.active")
        |> sql.eq(db.bool(True), of: sql.val),
    ])
    |> select.to_string

  assert expected == query
}

pub fn select_with_limit_test() {
  let expected = "SELECT * FROM users LIMIT ?"
  let users = sql.table("users")

  let query =
    repo.default()
    |> select.from(users)
    |> select.limit(10)
    |> select.to_query

  assert expected == query.sql
  assert [db.int(10)] == query.values
}

pub fn select_with_limit_and_offset_test() {
  let expected = "SELECT * FROM users LIMIT ? OFFSET ?"
  let users = sql.table("users")

  let query =
    repo.default()
    |> select.from(users)
    |> select.limit(20)
    |> select.offset(10)
    |> select.to_query

  assert expected == query.sql
  assert [db.int(20), db.int(10)] == query.values
}

pub fn select_with_offset_test() {
  let expected = "SELECT * FROM users LIMIT ? OFFSET ?"
  let users = sql.table("users")

  let query =
    repo.default()
    |> select.from(users)
    |> select.limit(100)
    |> select.offset(50)
    |> select.to_query

  assert expected == query.sql
  assert [db.int(100), db.int(50)] == query.values
}

pub fn group_by_test() {
  let expected =
    "SELECT department, COUNT(*) FROM employees GROUP BY department"
  let employees = sql.table("employees")

  let query =
    repo.default()
    |> select.from(employees)
    |> select.columns([sql.column("department"), sql.count("*")])
    |> select.group_by(["department"])
    |> select.to_query

  assert expected == query.sql
  assert [] == query.values
}

pub fn multiple_group_by_test() {
  let expected =
    "SELECT department, location, COUNT(*) FROM employees GROUP BY department, location"
  let employees = sql.table("employees")

  let query =
    repo.default()
    |> select.from(employees)
    |> select.columns([
      sql.column("department"),
      sql.column("location"),
      sql.count("*"),
    ])
    |> select.group_by(["department", "location"])
    |> select.to_query

  assert expected == query.sql
  assert [] == query.values
}

pub fn having_test() {
  let expected =
    "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > ?"
  let employees = sql.table("employees")

  let query =
    repo.default()
    |> select.from(employees)
    |> select.columns([sql.column("department"), sql.count("*")])
    |> select.group_by(["department"])
    |> select.having([
      sql.count("*")
      |> sql.gt(db.int(5), of: sql.val),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [db.int(5)] == query.values
}

pub fn multiple_having_test() {
  let expected =
    "SELECT department, AVG(salary) FROM employees GROUP BY department HAVING COUNT(*) > ? AND AVG(salary) > ?"
  let employees = sql.table("employees")

  let query =
    repo.default()
    |> select.from(employees)
    |> select.columns([sql.column("department"), sql.avg("salary")])
    |> select.group_by(["department"])
    |> select.having([
      sql.count("*")
        |> sql.gt(db.int(5), of: sql.val),
      sql.avg("salary")
        |> sql.gt(db.float(50_000.0), of: sql.val),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [db.int(5), db.float(50_000.0)] == query.values
}

pub fn order_by_with_asc_test() {
  let expected = "SELECT * FROM users ORDER BY name ASC"
  let users = sql.table("users")

  let query =
    repo.default()
    |> select.from(users)
    |> select.order_by(["name"])
    |> select.asc
    |> select.to_query

  assert expected == query.sql
  assert [] == query.values
}

pub fn order_by_with_desc_test() {
  let expected = "SELECT * FROM users ORDER BY created_at DESC"
  let users = sql.table("users")

  let query =
    repo.default()
    |> select.from(users)
    |> select.order_by(["created_at"])
    |> select.desc
    |> select.to_query

  assert expected == query.sql
  assert [] == query.values
}

pub fn multiple_order_by_columns_test() {
  let expected = "SELECT * FROM users ORDER BY department, name ASC"
  let users = sql.table("users")

  let query =
    repo.default()
    |> select.from(users)
    |> select.order_by(["department", "name"])
    |> select.asc
    |> select.to_query

  assert expected == query.sql
  assert [] == query.values
}

pub fn complex_query_with_order_by_test() {
  let expected =
    "SELECT department, COUNT(*) FROM employees WHERE active = ? GROUP BY department HAVING COUNT(*) > ? ORDER BY COUNT(*) DESC LIMIT ? OFFSET ?"
  let employees = sql.table("employees")

  let query =
    repo.default()
    |> select.from(employees)
    |> select.columns([
      sql.column("department"),
      sql.count("*"),
    ])
    |> select.where([
      sql.column("active")
      |> sql.eq(db.bool(True), of: sql.val),
    ])
    |> select.group_by(["department"])
    |> select.having([
      sql.count("*")
      |> sql.gt(db.int(10), of: sql.val),
    ])
    |> select.order_by(["COUNT(*)"])
    |> select.desc
    |> select.limit(5)
    |> select.offset(0)
    |> select.to_query

  assert expected == query.sql
  assert [db.true, db.int(10), db.int(5), db.int(0)] == query.values
}

pub fn from_subquery_test() {
  let expected =
    "SELECT name, department FROM (SELECT id, name, department FROM employees WHERE active = ?) WHERE name LIKE ?"
  let employees = sql.table("employees")

  let employees_query =
    repo.default()
    |> select.from(employees)
    |> select.columns([
      sql.column("id"),
      sql.column("name"),
      sql.column("department"),
    ])
    |> select.where([
      sql.column("active")
      |> sql.eq(db.bool(True), of: sql.val),
    ])
    |> select.to_query

  let query =
    repo.default()
    |> select.from_query(employees_query)
    |> select.columns([
      sql.column("name"),
      sql.column("department"),
    ])
    |> select.where([
      sql.column("name")
      |> column.like("%John%"),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [db.true, db.text("%John%")] == query.values
}

pub fn complex_queried_with_aggregation_test() {
  let expected =
    "SELECT department, total_salary FROM (SELECT department, SUM(salary) AS total_salary FROM employees GROUP BY department HAVING COUNT(*) > ?) WHERE total_salary > ?"
  let employees = sql.table("employees")

  let department_stats_query =
    repo.default()
    |> select.from(employees)
    |> select.columns([
      sql.column("department"),
      sql.sum("salary") |> column.alias("total_salary"),
    ])
    |> select.group_by(["department"])
    |> select.having([
      sql.count("*")
      |> sql.gt(db.int(10), of: sql.val),
    ])
    |> select.to_query

  let query =
    repo.default()
    |> select.from_query(department_stats_query)
    |> select.columns([
      sql.column("department"),
      sql.column("total_salary"),
    ])
    |> select.where([
      sql.column("total_salary")
      |> sql.gt(db.float(1_000_000.0), of: sql.val),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [db.int(10), db.float(1_000_000.0)] == query.values
}

pub fn for_update_test() {
  let expected = "SELECT * FROM users WHERE id = ? FOR UPDATE"
  let users = sql.table("users")

  let query =
    repo.default()
    |> select.from(users)
    |> select.where([
      sql.column("id")
      |> sql.eq(db.int(1), of: sql.val),
    ])
    |> select.for_update
    |> select.to_query

  assert expected == query.sql
  assert [db.int(1)] == query.values
}

pub fn complex_for_update_test() {
  let expected =
    "SELECT id, name, balance FROM accounts WHERE user_id = ? AND balance >= ? ORDER BY balance DESC LIMIT ? FOR UPDATE"
  let accounts = sql.table("accounts")

  let query =
    repo.default()
    |> select.from(accounts)
    |> select.columns([
      sql.column("id"),
      sql.column("name"),
      sql.column("balance"),
    ])
    |> select.where([
      sql.column("user_id")
        |> sql.eq(db.int(5), of: sql.val),
      sql.column("balance")
        |> sql.gt_eq(db.float(1000.0), of: sql.val),
    ])
    |> select.order_by(["balance"])
    |> select.desc
    |> select.limit(3)
    |> select.for_update
    |> select.to_query

  assert expected == query.sql
  assert [db.int(5), db.float(1000.0), db.int(3)] == query.values
}

pub fn format_placeholders_test() {
  let fmt =
    repo.default()
    |> repo.on_placeholder(fn(idx) { "$" <> int.to_string(idx) })

  let expected = "SELECT * FROM users WHERE id = $1 AND name = $2"
  let users = sql.table("users")

  let query =
    fmt
    |> select.from(users)
    |> select.where([
      sql.column("id")
        |> sql.eq(db.int(1), of: sql.val),
      sql.column("name")
        |> sql.eq(db.text("John"), of: sql.val),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [db.int(1), db.text("John")] == query.values
}

pub fn format_identifier_test() {
  let repo =
    repo.default()
    |> repo.on_identifier({ fn(val) { "\"" <> val <> "\"" } })

  let expected = "SELECT * FROM \"users\" WHERE \"id\" = ? AND \"name\" = ?"
  let users = sql.table("users")

  let query =
    repo
    |> select.from(users)
    |> select.where([
      sql.column("id")
        |> sql.eq(db.int(1), of: sql.val),
      sql.column("name")
        |> sql.eq(db.text("John"), of: sql.val),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [db.int(1), db.text("John")] == query.values
}

pub fn for_update_with_join_test() {
  let expected =
    "SELECT orders.id, orders.amount, users.name FROM orders INNER JOIN users ON orders.user_id = users.id WHERE orders.status = ? FOR UPDATE"
  let users = sql.table("users")
  let orders = sql.table("orders")

  let query =
    repo.default()
    |> select.from(orders)
    |> select.columns([
      sql.column("id") |> column.for(orders),
      sql.column("amount") |> column.for(orders),
      sql.column("name") |> column.for(users),
    ])
    |> select.join(users, on: [
      sql.column("orders.user_id")
      |> sql.eq(sql.column("users.id"), of: sql.col),
    ])
    |> select.where([
      sql.column("orders.status")
      |> sql.eq(db.text("pending"), of: sql.val),
    ])
    |> select.for_update
    |> select.to_query

  assert expected == query.sql
  assert [db.text("pending")] == query.values
}

pub fn date_time_types_test() {
  let assert Ok(time) = timestamp.parse_rfc3339("2025-04-09T12:00:00Z")
  let date = calendar.Date(2023, calendar.December, 31)
  let events = sql.table("events")

  let query =
    repo.default()
    |> select.from(events)
    |> select.where([
      sql.column("event_date")
        |> sql.eq(db.date(date), of: sql.val),
      sql.column("event_timestamp")
        |> sql.eq(db.timestamp(time), of: sql.val),
    ])
    |> select.to_query

  assert [db.date(date), db.timestamp(time)] == query.values
}

pub fn time_type_test() {
  let time_of_day =
    calendar.TimeOfDay(
      hours: 14,
      minutes: 30,
      seconds: 45,
      nanoseconds: 123_456_789,
    )
  let events = sql.table("events")

  let query =
    repo.default()
    |> select.from(events)
    |> select.where([
      sql.column("event_time")
      |> sql.eq(db.time(time_of_day), of: sql.val),
    ])
    |> select.to_query

  assert [db.time(time_of_day)] == query.values
}

pub fn duration_type_test() {
  let interval = interval.seconds(3661)
  let events = sql.table("events")

  let query =
    repo.default()
    |> select.from(events)
    |> select.where([
      sql.column("event_duration")
      |> sql.eq(db.interval(interval), of: sql.val),
    ])
    |> select.to_query

  assert [db.interval(interval)] == query.values
}

pub fn different_value_types_test() {
  let products = sql.table("products")

  let query =
    repo.default()
    |> select.from(products)
    |> select.where([
      sql.column("id")
        |> sql.eq(db.int(123), of: sql.val),
      sql.column("price")
        |> sql.gt(db.float(19.99), of: sql.val),
      sql.column("is_active")
        |> sql.eq(db.bool(True), of: sql.val),
      sql.column("description")
        |> sql.eq(db.null, of: sql.val),
    ])
    |> select.to_query

  assert [db.int(123), db.float(19.99), db.true, db.null] == query.values
}

pub fn between_test() {
  let expected = "SELECT * FROM products WHERE price BETWEEN ? AND ?"
  let products = sql.table("products")

  let query =
    repo.default()
    |> select.from(products)
    |> select.where([
      sql.column("price")
      |> sql.between(db.float(10.0), db.float(50.0), of: sql.val),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [db.float(10.0), db.float(50.0)] == query.values
}

pub fn complex_between_test() {
  let expected =
    "SELECT * FROM orders WHERE total BETWEEN ? AND ? AND created_at BETWEEN ? AND ? AND status = ?"

  let date_start = calendar.Date(2023, calendar.January, 1)
  let date_end = calendar.Date(2023, calendar.December, 31)
  let orders = sql.table("orders")

  let query =
    repo.default()
    |> select.from(orders)
    |> select.where([
      sql.column("total")
        |> sql.between(db.float(100.0), db.float(1000.0), of: sql.val),
      sql.column("created_at")
        |> sql.between(db.date(date_start), db.date(date_end), of: sql.val),
      sql.column("status")
        |> sql.eq(db.text("completed"), of: sql.val),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [
      db.float(100.0),
      db.float(1000.0),
      db.date(date_start),
      db.date(date_end),
      db.text("completed"),
    ]
    == query.values
}

pub fn not_between_test() {
  let expected = "SELECT * FROM products WHERE NOT price BETWEEN ? AND ?"
  let products = sql.table("products")

  let query =
    repo.default()
    |> select.from(products)
    |> select.where([
      sql.not(
        sql.column("price")
        |> sql.between(db.float(10.0), db.float(50.0), of: sql.val),
      ),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [db.float(10.0), db.float(50.0)] == query.values
}

pub fn raw_sql_where_test() {
  let expected =
    "SELECT * FROM products WHERE id = 10 AND NOT price BETWEEN ? AND ?"
  let products = sql.table("products")

  let query =
    repo.default()
    |> select.from(products)
    |> select.where([
      sql.raw("id = 10"),
      sql.not(
        sql.column("price")
        |> sql.between(db.float(10.0), db.float(50.0), of: sql.val),
      ),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [db.float(10.0), db.float(50.0)] == query.values
}

pub fn raw_sql_join_test() {
  let expected =
    "SELECT * FROM users INNER JOIN posts ON users.id = posts.user_id AND posts.title LIKE ?"

  let users = sql.table("users")
  let posts = sql.table("posts")

  let query =
    repo.default()
    |> select.from(users)
    |> select.join(posts, on: [
      sql.raw("users.id = posts.user_id"),
      sql.column("posts.title")
        |> sql.like("%gleam%"),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [db.text("%gleam%")] == query.values
}

pub fn exists_test() {
  let expected =
    "SELECT id, username FROM users WHERE EXISTS (SELECT 1 FROM posts WHERE users.id = posts.user_id)"

  let users = sql.table("users")
  let posts = sql.table("posts")

  let query =
    repo.default()
    |> select.from(users)
    |> select.columns([sql.column("id"), sql.column("username")])
    |> select.where_exists({
      repo.default()
      |> select.from(posts)
      |> select.columns([sql.column("1")])
      |> select.where([
        sql.column("users.id")
        |> sql.eq(sql.column("posts.user_id"), sql.col),
      ])
    })
    |> select.to_query

  assert expected == query.sql
  assert [] == query.values
}

pub fn any_test() {
  let expected =
    "SELECT title FROM posts WHERE user_id = ANY (SELECT id FROM users WHERE created_at > ?)"

  let users = sql.table("users")
  let posts = sql.table("posts")

  let feb_1_2026 =
    calendar.Date(year: 2026, month: calendar.February, day: 1)
    |> db.date

  let users_subquery =
    repo.default()
    |> select.from(users)
    |> select.columns([sql.column("id")])
    |> select.where([
      sql.column("created_at") |> sql.gt(feb_1_2026, sql.val),
    ])

  let query =
    repo.default()
    |> select.from(posts)
    |> select.columns([sql.column("title")])
    |> select.where([
      sql.column("user_id") |> sql.eq(users_subquery, of: select.any),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [feb_1_2026] == query.values
}

pub fn all_test() {
  let expected =
    "SELECT * FROM posts WHERE views > ALL (SELECT views FROM posts WHERE category = ?)"

  let posts = sql.table("posts")

  let users_subquery =
    repo.default()
    |> select.from(posts)
    |> select.columns([sql.column("views")])
    |> select.where([
      sql.column("category") |> sql.eq(db.text("Gleam"), sql.val),
    ])

  let query =
    repo.default()
    |> select.from(posts)
    |> select.columns([sql.all])
    |> select.where([
      sql.column("views") |> sql.gt(users_subquery, of: select.all),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [db.text("Gleam")] == query.values
}

pub fn date_to_string_test() {
  let expected = "SELECT * FROM events WHERE event_date = '2023-12-31'"
  let date = calendar.Date(2023, calendar.December, 31)
  let events = sql.table("events")

  let result =
    repo.default()
    |> select.from(events)
    |> select.where([
      sql.column("event_date")
      |> sql.eq(db.date(date), of: sql.val),
    ])
    |> select.to_string

  assert expected == result
}

pub fn time_to_string_test() {
  let expected = "SELECT * FROM events WHERE event_time = '14:30:45.123'"
  let time_of_day =
    calendar.TimeOfDay(
      hours: 14,
      minutes: 30,
      seconds: 45,
      nanoseconds: 123_000_000,
    )
  let events = sql.table("events")

  let result =
    repo.default()
    |> select.from(events)
    |> select.where([
      sql.column("event_time")
      |> sql.eq(db.time(time_of_day), of: sql.val),
    ])
    |> select.to_string

  assert expected == result
}

pub fn time_to_string_no_milliseconds_test() {
  let expected = "SELECT * FROM events WHERE event_time = '08:05:00'"
  let time_of_day =
    calendar.TimeOfDay(hours: 8, minutes: 5, seconds: 0, nanoseconds: 0)
  let events = sql.table("events")

  let result =
    repo.default()
    |> select.from(events)
    |> select.where([
      sql.column("event_time")
      |> sql.eq(db.time(time_of_day), of: sql.val),
    ])
    |> select.to_string

  assert expected == result
}

pub fn datetime_to_string_test() {
  let expected = "SELECT * FROM events WHERE created_at = '2023-12-31 14:30:45'"
  let date = calendar.Date(2023, calendar.December, 31)
  let time_of_day =
    calendar.TimeOfDay(hours: 14, minutes: 30, seconds: 45, nanoseconds: 0)
  let events = sql.table("events")

  let result =
    repo.default()
    |> select.from(events)
    |> select.where([
      sql.column("created_at")
      |> sql.eq(db.datetime(date, time_of_day), of: sql.val),
    ])
    |> select.to_string

  assert expected == result
}

pub fn datetime_with_milliseconds_to_string_test() {
  let expected =
    "SELECT * FROM events WHERE created_at = '2023-01-15 09:05:03.042'"
  let date = calendar.Date(2023, calendar.January, 15)
  let time_of_day =
    calendar.TimeOfDay(
      hours: 9,
      minutes: 5,
      seconds: 3,
      nanoseconds: 42_000_000,
    )
  let events = sql.table("events")

  let result =
    repo.default()
    |> select.from(events)
    |> select.where([
      sql.column("created_at")
      |> sql.eq(db.datetime(date, time_of_day), of: sql.val),
    ])
    |> select.to_string

  assert expected == result
}

pub fn select_chained_where_test() {
  let expected =
    "SELECT * FROM users WHERE age > ? AND status = ? AND name LIKE ?"
  let users = sql.table("users")

  let query =
    repo.default()
    |> select.from(users)
    |> select.where([
      sql.column("age")
        |> sql.gt(db.int(18), of: sql.val),
      sql.column("status")
        |> sql.eq(db.text("active"), of: sql.val),
    ])
    |> select.where([sql.column("name") |> sql.like("%John%")])
    |> select.to_query

  assert expected == query.sql
  assert [db.int(18), db.text("active"), db.text("%John%")] == query.values
}

pub fn select_offset_without_limit_test() {
  let expected = "SELECT * FROM users OFFSET ?"
  let users = sql.table("users")

  let query =
    repo.default()
    |> select.from(users)
    |> select.offset(10)
    |> select.to_query

  assert expected == query.sql
  assert [db.int(10)] == query.values
}

pub fn select_offset_before_limit_test() {
  let expected = "SELECT * FROM users LIMIT ? OFFSET ?"
  let users = sql.table("users")

  let query =
    repo.default()
    |> select.from(users)
    |> select.offset(50)
    |> select.limit(10)
    |> select.to_query

  assert expected == query.sql
  assert [db.int(10), db.int(50)] == query.values
}

pub fn select_offset_without_limit_with_where_test() {
  let expected = "SELECT * FROM users WHERE active IS TRUE OFFSET ?"
  let users = sql.table("users")

  let query =
    repo.default()
    |> select.from(users)
    |> select.where([sql.column("active") |> column.is(True)])
    |> select.offset(25)
    |> select.to_query

  assert expected == query.sql
  assert [db.int(25)] == query.values
}

pub fn select_offset_without_limit_to_string_test() {
  let expected = "SELECT * FROM users OFFSET 10"
  let users = sql.table("users")

  let result =
    repo.default()
    |> select.from(users)
    |> select.offset(10)
    |> select.to_string

  assert expected == result
}
