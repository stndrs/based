import based
import based/sql
import based/sql/column
import based/sql/select
import based/value
import gleam/int
import gleam/time/calendar
import gleam/time/duration
import gleam/time/timestamp

pub fn select_test() {
  let expected = "SELECT id, name FROM users"

  let users = sql.table("users")

  let query =
    value.repo()
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
    value.repo()
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
    value.repo()
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
    value.repo()
    |> select.from(users)
    |> select.columns([sql.column("title")])
    |> select.where([
      sql.column("created_at")
        |> sql.gt(value.text("2024-01-01"), of: sql.val),

      sql.column("user_id")
        |> sql.eq(value.int(10), of: sql.val),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [value.text("2024-01-01"), value.int(10)] == query.values
}

pub fn select_subquery_test() {
  let expected =
    "SELECT title FROM posts WHERE created_at > ? AND user_id = (SELECT id FROM users WHERE name = ?)"
  let users = sql.table("users")
  let posts = sql.table("posts")

  let subquery =
    value.repo()
    |> select.from(users)
    |> select.columns([sql.column("id")])
    |> select.where([
      sql.column("name")
      |> sql.eq(value.text("Human Person"), of: sql.val),
    ])

  let query =
    value.repo()
    |> select.from(posts)
    |> select.columns([sql.column("title")])
    |> select.where([
      sql.column("created_at")
        |> sql.gt(value.text("2024-01-01"), of: sql.val),
      sql.column("user_id")
        |> sql.eq(subquery, of: select.subquery),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [value.text("2024-01-01"), value.text("Human Person")] == query.values
}

pub fn select_or_test() {
  let expected = "SELECT * FROM users WHERE name = ? OR email = ?"
  let users = sql.table("users")

  let query =
    value.repo()
    |> select.from(users)
    |> select.where([
      sql.column("name")
      |> sql.eq(value.text("Human Person"), of: sql.val)
      |> sql.or(
        sql.column("email")
        |> sql.eq(value.text("human.person@example.com"), of: sql.val),
      ),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [
      value.text("Human Person"),
      value.text("human.person@example.com"),
    ]
    == query.values
}

pub fn select_where_not_test() {
  let expected = "SELECT * FROM users WHERE NOT email = ?"
  let users = sql.table("users")

  let query =
    value.repo()
    |> select.from(users)
    |> select.where_not([
      sql.column("email")
      |> sql.eq(value.text("Human Person"), of: sql.val),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [value.text("Human Person")] == query.values
}

pub fn select_where_not_like_test() {
  let expected = "SELECT * FROM users WHERE email NOT LIKE ?"
  let users = sql.table("users")

  let query =
    value.repo()
    |> select.from(users)
    |> select.where([
      sql.column("email")
      |> sql.not_like("Human Person"),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [value.text("Human Person")] == query.values
}

pub fn select_distinct_test() {
  let expected = "SELECT DISTINCT value FROM users"
  let users = sql.table("users")

  let query =
    value.repo()
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
    value.repo()
    |> select.from(users)
    |> select.join(posts, on: [
      sql.column("users.id")
        |> sql.eq(sql.column("posts.user_id"), of: sql.col),
      sql.column("posts.title")
        |> sql.like("%gleam%"),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [value.text("%gleam%")] == query.values
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
    value.repo()
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
  assert [value.text("%gleam%")] == query.values
}

pub fn select_with_in_test() {
  let expected = "SELECT * FROM users WHERE id IN (?, ?, ?)"
  let users = sql.table("users")

  let query =
    value.repo()
    |> select.from(users)
    |> select.where([
      sql.column("id")
      |> sql.in([1, 2, 3], of: sql.list(of: value.int)),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [value.int(1), value.int(2), value.int(3)] == query.values
}

// pub fn select_with_in_tuples_test() {
//   let expected =
//     "SELECT * FROM posts WHERE (id, user_id) IN ((?, ?), (?, ?), (?, ?))"
//   let posts = sql.table("posts")
// 
//   let query =
//     value.repo()
//     |> select.from(posts)
//     |> select.where([
//       sql.columns(["id", "user_id"])
//       |> sql.in(
//         sql.tuples([
//           [sql.val(value.int(1)), sql.val(value.int(10))],
//           [sql.val(value.int(2)), sql.val(value.int(10))],
//           [sql.val(value.int(3)), sql.val(value.int(10))],
//         ]),
//       ),
//     ])
//     |> select.to_query
// 
//   assert expected == query.sql
//   assert [
//       value.int(1),
//       value.int(10),
//       value.int(2),
//       value.int(10),
//       value.int(3),
//       value.int(10),
//     ]
//     == query.values
// }

pub fn select_with_is_null_test() {
  let expected = "SELECT * FROM users WHERE deleted_at IS NULL"
  let users = sql.table("users")

  let query =
    value.repo()
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
    value.repo()
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
    value.repo()
    |> select.from(users)
    |> select.where([sql.column("active") |> column.is_not_null])
    |> select.to_query

  assert expected == query.sql
  assert [] == query.values
}

pub fn select_wildcard_test() {
  let expected = "SELECT * FROM users"
  let users = sql.table("users")

  let query =
    value.repo()
    |> select.from(users)
    |> select.to_query

  assert expected == query.sql
  assert [] == query.values
}

pub fn where_lt_test() {
  let expected = "SELECT * FROM users WHERE age < ?"
  let users = sql.table("users")

  let query =
    value.repo()
    |> select.from(users)
    |> select.where([
      sql.column("age")
      |> sql.lt(value.int(65), of: sql.val),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [value.int(65)] == query.values
}

pub fn where_lt_eq_test() {
  let expected = "SELECT * FROM users WHERE age <= ?"
  let users = sql.table("users")

  let query =
    value.repo()
    |> select.from(users)
    |> select.where([
      sql.column("age")
      |> sql.lt_eq(value.int(65), of: sql.val),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [value.int(65)] == query.values
}

pub fn where_not_eq_test() {
  let expected = "SELECT * FROM users WHERE status <> ?"
  let users = sql.table("users")

  let query =
    value.repo()
    |> select.from(users)
    |> select.where([
      sql.column("status")
      |> sql.not_eq(value.text("inactive"), of: sql.val),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [value.text("inactive")] == query.values
}

pub fn multiple_where_test() {
  let expected = "SELECT * FROM users WHERE age > ? AND status = ?"
  let users = sql.table("users")

  let query =
    value.repo()
    |> select.from(users)
    |> select.where([
      sql.column("age")
        |> sql.gt(value.int(18), of: sql.val),
      sql.column("status")
        |> sql.eq(value.text("active"), of: sql.val),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [value.int(18), value.text("active")] == query.values
}

pub fn join_with_multiple_conditions_to_string_test() {
  let expected =
    "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id AND orders.status = 'completed' WHERE users.age > 20 AND users.active = TRUE"

  let users = sql.table("users")
  let orders = sql.table("orders")

  let query =
    value.repo()
    |> select.from(users)
    |> select.join(orders, on: [
      sql.column("users.id")
        |> sql.eq(sql.column("orders.user_id"), of: sql.col),
      sql.column("orders.status")
        |> sql.eq(value.text("completed"), of: sql.val),
    ])
    |> select.where([
      sql.column("users.age")
        |> sql.gt(value.int(20), of: sql.val),
      sql.column("users.active")
        |> sql.eq(value.bool(True), of: sql.val),
    ])
    |> select.to_string

  assert expected == query
}

pub fn select_with_limit_test() {
  let expected = "SELECT * FROM users LIMIT ?"
  let users = sql.table("users")

  let query =
    value.repo()
    |> select.from(users)
    |> select.limit(10, of: value.int)
    |> select.to_query

  assert expected == query.sql
  assert [value.int(10)] == query.values
}

pub fn select_with_limit_and_offset_test() {
  let expected = "SELECT * FROM users LIMIT ? OFFSET ?"
  let users = sql.table("users")

  let query =
    value.repo()
    |> select.from(users)
    |> select.limit(20, of: value.int)
    |> select.offset(10, of: value.int)
    |> select.to_query

  assert expected == query.sql
  assert [value.int(20), value.int(10)] == query.values
}

pub fn select_with_offset_test() {
  let expected = "SELECT * FROM users LIMIT ? OFFSET ?"
  let users = sql.table("users")

  let query =
    value.repo()
    |> select.from(users)
    |> select.limit(100, of: value.int)
    |> select.offset(50, of: value.int)
    |> select.to_query

  assert expected == query.sql
  assert [value.int(100), value.int(50)] == query.values
}

pub fn group_by_test() {
  let expected =
    "SELECT department, COUNT(*) FROM employees GROUP BY department"
  let employees = sql.table("employees")

  let query =
    value.repo()
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
    value.repo()
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
    value.repo()
    |> select.from(employees)
    |> select.columns([sql.column("department"), sql.count("*")])
    |> select.group_by(["department"])
    |> select.having([
      sql.count("*")
      |> sql.gt(value.int(5), of: sql.val),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [value.int(5)] == query.values
}

pub fn multiple_having_test() {
  let expected =
    "SELECT department, AVG(salary) FROM employees GROUP BY department HAVING COUNT(*) > ? AND AVG(salary) > ?"
  let employees = sql.table("employees")

  let query =
    value.repo()
    |> select.from(employees)
    |> select.columns([sql.column("department"), sql.avg("salary")])
    |> select.group_by(["department"])
    |> select.having([
      sql.count("*")
        |> sql.gt(value.int(5), of: sql.val),
      sql.avg("salary")
        |> sql.gt(value.float(50_000.0), of: sql.val),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [value.int(5), value.float(50_000.0)] == query.values
}

pub fn order_by_with_asc_test() {
  let expected = "SELECT * FROM users ORDER BY name ASC"
  let users = sql.table("users")

  let query =
    value.repo()
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
    value.repo()
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
    value.repo()
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
    value.repo()
    |> select.from(employees)
    |> select.columns([
      sql.column("department"),
      sql.count("*"),
    ])
    |> select.where([
      sql.column("active")
      |> sql.eq(value.bool(True), of: sql.val),
    ])
    |> select.group_by(["department"])
    |> select.having([
      sql.count("*")
      |> sql.gt(value.int(10), of: sql.val),
    ])
    |> select.order_by(["COUNT(*)"])
    |> select.desc
    |> select.limit(5, of: value.int)
    |> select.offset(0, of: value.int)
    |> select.to_query

  assert expected == query.sql
  assert [value.true, value.int(10), value.int(5), value.int(0)] == query.values
}

pub fn from_subquery_test() {
  let expected =
    "SELECT name, department FROM (SELECT id, name, department FROM employees WHERE active = ?) WHERE name LIKE ?"
  let employees = sql.table("employees")

  let employees_query =
    value.repo()
    |> select.from(employees)
    |> select.columns([
      sql.column("id"),
      sql.column("name"),
      sql.column("department"),
    ])
    |> select.where([
      sql.column("active")
      |> sql.eq(value.bool(True), of: sql.val),
    ])
    |> select.to_query

  let query =
    value.repo()
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
  assert [value.true, value.text("%John%")] == query.values
}

pub fn complex_queried_with_aggregation_test() {
  let expected =
    "SELECT department, total_salary FROM (SELECT department, SUM(salary) AS total_salary FROM employees GROUP BY department HAVING COUNT(*) > ?) WHERE total_salary > ?"
  let employees = sql.table("employees")

  let department_stats_query =
    value.repo()
    |> select.from(employees)
    |> select.columns([
      sql.column("department"),
      sql.sum("salary") |> column.alias("total_salary"),
    ])
    |> select.group_by(["department"])
    |> select.having([
      sql.count("*")
      |> sql.gt(value.int(10), of: sql.val),
    ])
    |> select.to_query

  let query =
    value.repo()
    |> select.from_query(department_stats_query)
    |> select.columns([
      sql.column("department"),
      sql.column("total_salary"),
    ])
    |> select.where([
      sql.column("total_salary")
      |> sql.gt(value.float(1_000_000.0), of: sql.val),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [value.int(10), value.float(1_000_000.0)] == query.values
}

pub fn for_update_test() {
  let expected = "SELECT * FROM users WHERE id = ? FOR UPDATE"
  let users = sql.table("users")

  let query =
    value.repo()
    |> select.from(users)
    |> select.where([
      sql.column("id")
      |> sql.eq(value.int(1), of: sql.val),
    ])
    |> select.for_update
    |> select.to_query

  assert expected == query.sql
  assert [value.int(1)] == query.values
}

pub fn complex_for_update_test() {
  let expected =
    "SELECT id, name, balance FROM accounts WHERE user_id = ? AND balance >= ? ORDER BY balance DESC LIMIT ? FOR UPDATE"
  let accounts = sql.table("accounts")

  let query =
    value.repo()
    |> select.from(accounts)
    |> select.columns([
      sql.column("id"),
      sql.column("name"),
      sql.column("balance"),
    ])
    |> select.where([
      sql.column("user_id")
        |> sql.eq(value.int(5), of: sql.val),
      sql.column("balance")
        |> sql.gt_eq(value.float(1000.0), of: sql.val),
    ])
    |> select.order_by(["balance"])
    |> select.desc
    |> select.limit(3, of: value.int)
    |> select.for_update
    |> select.to_query

  assert expected == query.sql
  assert [value.int(5), value.float(1000.0), value.int(3)] == query.values
}

pub fn format_placeholders_test() {
  let fmt =
    value.repo()
    |> based.on_placeholder(fn(idx) { "$" <> int.to_string(idx) })

  let expected = "SELECT * FROM users WHERE id = $1 AND name = $2"
  let users = sql.table("users")

  let query =
    fmt
    |> select.from(users)
    |> select.where([
      sql.column("id")
        |> sql.eq(value.int(1), of: sql.val),
      sql.column("name")
        |> sql.eq(value.text("John"), of: sql.val),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [value.int(1), value.text("John")] == query.values
}

pub fn format_identifier_test() {
  let repo =
    value.repo()
    |> based.on_identifier({ fn(val) { "\"" <> val <> "\"" } })

  let expected = "SELECT * FROM \"users\" WHERE \"id\" = ? AND \"name\" = ?"
  let users = sql.table("users")

  let query =
    repo
    |> select.from(users)
    |> select.where([
      sql.column("id")
        |> sql.eq(value.int(1), of: sql.val),
      sql.column("name")
        |> sql.eq(value.text("John"), of: sql.val),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [value.int(1), value.text("John")] == query.values
}

pub fn for_update_with_join_test() {
  let expected =
    "SELECT orders.id, orders.amount, users.name FROM orders INNER JOIN users ON orders.user_id = users.id WHERE orders.status = ? FOR UPDATE"
  let users = sql.table("users")
  let orders = sql.table("orders")

  let query =
    value.repo()
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
      |> sql.eq(value.text("pending"), of: sql.val),
    ])
    |> select.for_update
    |> select.to_query

  assert expected == query.sql
  assert [value.text("pending")] == query.values
}

pub fn date_time_types_test() {
  let assert Ok(time) = timestamp.parse_rfc3339("2025-04-09T12:00:00Z")
  let date = calendar.Date(2023, calendar.December, 31)
  let events = sql.table("events")

  let query =
    value.repo()
    |> select.from(events)
    |> select.where([
      sql.column("event_date")
        |> sql.eq(value.date(date), of: sql.val),
      sql.column("event_timestamp")
        |> sql.eq(value.timestamp(time), of: sql.val),
    ])
    |> select.to_query

  assert [value.date(date), value.timestamp(time)] == query.values
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
    value.repo()
    |> select.from(events)
    |> select.where([
      sql.column("event_time")
      |> sql.eq(value.time(time_of_day), of: sql.val),
    ])
    |> select.to_query

  assert [value.time(time_of_day)] == query.values
}

pub fn duration_type_test() {
  let dur = duration.seconds(3661)
  let events = sql.table("events")

  let query =
    value.repo()
    |> select.from(events)
    |> select.where([
      sql.column("event_duration")
      |> sql.eq(value.interval(dur), of: sql.val),
    ])
    |> select.to_query

  assert [value.interval(dur)] == query.values
}

pub fn different_value_types_test() {
  let products = sql.table("products")

  let query =
    value.repo()
    |> select.from(products)
    |> select.where([
      sql.column("id")
        |> sql.eq(value.int(123), of: sql.val),
      sql.column("price")
        |> sql.gt(value.float(19.99), of: sql.val),
      sql.column("is_active")
        |> sql.eq(value.bool(True), of: sql.val),
      sql.column("description")
        |> sql.eq(value.null, of: sql.val),
    ])
    |> select.to_query

  assert [value.int(123), value.float(19.99), value.true, value.null]
    == query.values
}

pub fn between_test() {
  let expected = "SELECT * FROM products WHERE price BETWEEN ? AND ?"
  let products = sql.table("products")

  let query =
    value.repo()
    |> select.from(products)
    |> select.where([
      sql.column("price")
      |> sql.between(value.float(10.0), value.float(50.0), of: sql.val),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [value.float(10.0), value.float(50.0)] == query.values
}

pub fn complex_between_test() {
  let expected =
    "SELECT * FROM orders WHERE total BETWEEN ? AND ? AND created_at BETWEEN ? AND ? AND status = ?"

  let date_start = calendar.Date(2023, calendar.January, 1)
  let date_end = calendar.Date(2023, calendar.December, 31)
  let orders = sql.table("orders")

  let query =
    value.repo()
    |> select.from(orders)
    |> select.where([
      sql.column("total")
        |> sql.between(value.float(100.0), value.float(1000.0), of: sql.val),
      sql.column("created_at")
        |> sql.between(
          value.date(date_start),
          value.date(date_end),
          of: sql.val,
        ),
      sql.column("status")
        |> sql.eq(value.text("completed"), of: sql.val),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [
      value.float(100.0),
      value.float(1000.0),
      value.date(date_start),
      value.date(date_end),
      value.text("completed"),
    ]
    == query.values
}

pub fn not_between_test() {
  let expected = "SELECT * FROM products WHERE NOT price BETWEEN ? AND ?"
  let products = sql.table("products")

  let query =
    value.repo()
    |> select.from(products)
    |> select.where([
      sql.not(
        sql.column("price")
        |> sql.between(value.float(10.0), value.float(50.0), of: sql.val),
      ),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [value.float(10.0), value.float(50.0)] == query.values
}

pub fn raw_sql_where_test() {
  let expected =
    "SELECT * FROM products WHERE id = 10 AND NOT price BETWEEN ? AND ?"
  let products = sql.table("products")

  let query =
    value.repo()
    |> select.from(products)
    |> select.where([
      sql.raw("id = 10"),
      sql.not(
        sql.column("price")
        |> sql.between(value.float(10.0), value.float(50.0), of: sql.val),
      ),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [value.float(10.0), value.float(50.0)] == query.values
}

pub fn raw_sql_join_test() {
  let expected =
    "SELECT * FROM users INNER JOIN posts ON users.id = posts.user_id AND posts.title LIKE ?"

  let users = sql.table("users")
  let posts = sql.table("posts")

  let query =
    value.repo()
    |> select.from(users)
    |> select.join(posts, on: [
      sql.raw("users.id = posts.user_id"),
      sql.column("posts.title")
        |> sql.like("%gleam%"),
    ])
    |> select.to_query

  assert expected == query.sql
  assert [value.text("%gleam%")] == query.values
}
