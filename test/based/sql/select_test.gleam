import based/sql
import based/sql/select
import based/value
import gleam/int
import gleam/time/calendar
import gleam/time/duration
import gleam/time/timestamp
import gleeunit/should

pub fn select_test() {
  let expected = "SELECT id, name FROM users"

  let users = sql.identifier("users")

  let query =
    value.sql()
    |> select.from(users, of: sql.table)
    |> select.columns(["id", "name"])
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn select_alias_test() {
  let expected = "SELECT user_id AS id FROM user_posts"
  let user_posts = sql.identifier("user_posts")

  let query =
    value.sql()
    |> select.from(user_posts, of: sql.table)
    |> select.columns(["user_id AS id"])
    |> select.to_query

  query.sql |> should.equal(expected)
}

pub fn select_distincts_test() {
  let expected = "SELECT DISTINCT id, name FROM users"
  let users = sql.identifier("users")

  let query =
    value.sql()
    |> select.from(users, of: sql.table)
    |> select.columns(["id, name"])
    |> select.distinct
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn select_subquery_test() {
  let expected =
    "SELECT title FROM posts WHERE created_at > ? AND user_id = (SELECT id FROM users WHERE name = ?)"
  let users = sql.identifier("users")
  let posts = sql.identifier("posts")

  let subquery =
    value.sql()
    |> select.from(users, of: sql.table)
    |> select.columns(["id"])
    |> select.where([
      sql.identifier("name")
      |> sql.column
      |> sql.eq(sql.value(value.text("Human Person"))),
    ])
    |> select.to_subquery

  let query =
    value.sql()
    |> select.from(posts, of: sql.table)
    |> select.columns(["title"])
    |> select.where([
      sql.identifier("created_at")
        |> sql.column
        |> sql.gt(sql.value(value.text("2024-01-01"))),
      sql.identifier("user_id")
        |> sql.column
        |> sql.eq(subquery),
    ])
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values
  |> should.equal([value.text("2024-01-01"), value.text("Human Person")])
}

pub fn select_or_test() {
  let expected = "SELECT * FROM users WHERE name = ? OR email = ?"
  let users = sql.identifier("users")

  let query =
    value.sql()
    |> select.from(users, of: sql.table)
    |> select.where([
      sql.identifier("name")
      |> sql.column
      |> sql.eq(sql.value(value.text("Human Person")))
      |> sql.or(
        sql.identifier("email")
        |> sql.column
        |> sql.eq(sql.value(value.text("human.person@example.com"))),
      ),
    ])
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values
  |> should.equal([
    value.text("Human Person"),
    value.text("human.person@example.com"),
  ])
}

pub fn select_where_not_test() {
  let expected = "SELECT * FROM users WHERE NOT email = ?"
  let users = sql.identifier("users")

  let query =
    value.sql()
    |> select.from(users, of: sql.table)
    |> select.where_not([
      sql.identifier("email")
      |> sql.column
      |> sql.eq(sql.value(value.text("Human Person"))),
    ])
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("Human Person")])
}

pub fn select_where_not_like_test() {
  let expected = "SELECT * FROM users WHERE email NOT LIKE ?"
  let users = sql.identifier("users")

  let query =
    value.sql()
    |> select.from(users, of: sql.table)
    |> select.where([
      sql.identifier("email")
      |> sql.column
      |> sql.not_like("Human Person", of: value.text),
    ])
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("Human Person")])
}

pub fn select_distinct_test() {
  let expected = "SELECT DISTINCT value FROM users"
  let users = sql.identifier("users")

  let query =
    value.sql()
    |> select.from(users, of: sql.table)
    |> select.columns(["value"])
    |> select.distinct
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn select_with_join_test() {
  let expected =
    "SELECT * FROM users INNER JOIN posts ON users.id = posts.user_id AND posts.title LIKE ?"

  let users = sql.identifier("users")
  let posts = sql.identifier("posts")

  let query =
    value.sql()
    |> select.from(users, of: sql.table)
    |> select.join(posts, of: sql.table, on: [
      sql.identifier("users.id")
        |> sql.column
        |> sql.eq(sql.identifier("posts.user_id") |> sql.column),
      sql.identifier("posts.title")
        |> sql.column
        |> sql.like("%gleam%", of: value.text),
    ])
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("%gleam%")])
}

pub fn select_with_multiple_joins_test() {
  let expected =
    "SELECT * FROM users INNER JOIN posts ON users.id = posts.user_id AND posts.title LIKE ? LEFT JOIN comments ON posts.comment_id = comments.id RIGHT JOIN tags ON tags.id = posts.tag_id FULL OUTER JOIN followers ON followers.user_id = users.id"

  let users = sql.identifier("users")
  let posts = sql.identifier("posts")
  let comments = sql.identifier("comments")
  let tags = sql.identifier("tags")
  let followers = sql.identifier("followers")

  let query =
    value.sql()
    |> select.from(users, of: sql.table)
    |> select.join(posts, of: sql.table, on: [
      sql.identifier("users.id")
        |> sql.column
        |> sql.eq(sql.identifier("posts.user_id") |> sql.column),
      sql.identifier("posts.title")
        |> sql.column
        |> sql.like("%gleam%", of: value.text),
    ])
    |> select.left_join(comments, of: sql.table, on: [
      sql.identifier("posts.comment_id")
      |> sql.column
      |> sql.eq(sql.identifier("comments.id") |> sql.column),
    ])
    |> select.right_join(tags, of: sql.table, on: [
      sql.identifier("tags.id")
      |> sql.column
      |> sql.eq(sql.identifier("posts.tag_id") |> sql.column),
    ])
    |> select.full_join(followers, of: sql.table, on: [
      sql.identifier("followers.user_id")
      |> sql.column
      |> sql.eq(sql.identifier("users.id") |> sql.column),
    ])
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("%gleam%")])
}

pub fn select_with_in_test() {
  let expected = "SELECT * FROM users WHERE id IN (?, ?, ?)"
  let users = sql.identifier("users")

  let query =
    value.sql()
    |> select.from(users, of: sql.table)
    |> select.where([
      sql.identifier("id")
      |> sql.column
      |> sql.in(sql.list([1, 2, 3], of: value.int)),
    ])
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(1), value.int(2), value.int(3)])
}

pub fn select_with_in_tuples_test() {
  let expected =
    "SELECT * FROM posts WHERE (id, user_id) IN ((?, ?), (?, ?), (?, ?))"
  let posts = sql.identifier("posts")

  let query =
    value.sql()
    |> select.from(posts, of: sql.table)
    |> select.where([
      sql.columns(["id", "user_id"])
      |> sql.in(
        sql.tuples([
          [sql.value(value.int(1)), sql.value(value.int(10))],
          [sql.value(value.int(2)), sql.value(value.int(10))],
          [sql.value(value.int(3)), sql.value(value.int(10))],
        ]),
      ),
    ])
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values
  |> should.equal([
    value.int(1),
    value.int(10),
    value.int(2),
    value.int(10),
    value.int(3),
    value.int(10),
  ])
}

pub fn select_with_is_null_test() {
  let expected = "SELECT * FROM users WHERE deleted_at IS NULL"
  let users = sql.identifier("users")

  let query =
    value.sql()
    |> select.from(users, of: sql.table)
    |> select.where([
      sql.identifier("deleted_at")
      |> sql.column
      |> sql.is_null,
    ])
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn select_with_is_true_test() {
  let expected = "SELECT * FROM users WHERE deleted_at IS TRUE"
  let users = sql.identifier("users")

  let query =
    value.sql()
    |> select.from(users, of: sql.table)
    |> select.where([
      sql.identifier("deleted_at")
      |> sql.column
      |> sql.is(True),
    ])
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn select_with_is_not_null_test() {
  let expected = "SELECT * FROM users WHERE active IS NOT NULL"
  let users = sql.identifier("users")

  let query =
    value.sql()
    |> select.from(users, of: sql.table)
    |> select.where([
      sql.identifier("active")
      |> sql.column
      |> sql.is_not_null,
    ])
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn select_wildcard_test() {
  let expected = "SELECT * FROM users"
  let users = sql.identifier("users")

  let query =
    value.sql()
    |> select.from(users, of: sql.table)
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn where_lt_test() {
  let expected = "SELECT * FROM users WHERE age < ?"
  let users = sql.identifier("users")

  let query =
    value.sql()
    |> select.from(users, of: sql.table)
    |> select.where([
      sql.identifier("age")
      |> sql.column
      |> sql.lt(sql.value(value.int(65))),
    ])
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(65)])
}

pub fn where_lt_eq_test() {
  let expected = "SELECT * FROM users WHERE age <= ?"
  let users = sql.identifier("users")

  let query =
    value.sql()
    |> select.from(users, of: sql.table)
    |> select.where([
      sql.identifier("age")
      |> sql.column
      |> sql.lt_eq(sql.value(value.int(65))),
    ])
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(65)])
}

pub fn where_not_eq_test() {
  let expected = "SELECT * FROM users WHERE status <> ?"
  let users = sql.identifier("users")

  let query =
    value.sql()
    |> select.from(users, of: sql.table)
    |> select.where([
      sql.identifier("status")
      |> sql.column
      |> sql.not_eq(sql.value(value.text("inactive"))),
    ])
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("inactive")])
}

pub fn multiple_where_test() {
  let expected = "SELECT * FROM users WHERE age > ? AND status = ?"
  let users = sql.identifier("users")

  let query =
    value.sql()
    |> select.from(users, of: sql.table)
    |> select.where([
      sql.identifier("age")
        |> sql.column
        |> sql.gt(sql.value(value.int(18))),
      sql.identifier("status")
        |> sql.column
        |> sql.eq(sql.value(value.text("active"))),
    ])
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(18), value.text("active")])
}

pub fn join_with_multiple_conditions_to_string_test() {
  let expected =
    "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id AND orders.status = 'completed' WHERE users.age > 20 AND users.active = TRUE"

  let users = sql.identifier("users")
  let orders = sql.identifier("orders")

  let query =
    value.sql()
    |> select.from(users, of: sql.table)
    |> select.join(orders, of: sql.table, on: [
      sql.identifier("users.id")
        |> sql.column
        |> sql.eq(sql.identifier("orders.user_id") |> sql.column),
      sql.identifier("orders.status")
        |> sql.column
        |> sql.eq(sql.value(value.text("completed"))),
    ])
    |> select.where([
      sql.identifier("users.age")
        |> sql.column
        |> sql.gt(sql.value(value.int(20))),
      sql.identifier("users.active")
        |> sql.column
        |> sql.eq(sql.value(value.bool(True))),
    ])
    |> select.to_string

  query |> should.equal(expected)
}

pub fn select_with_limit_test() {
  let expected = "SELECT * FROM users LIMIT ?"
  let users = sql.identifier("users")

  let query =
    value.sql()
    |> select.from(users, of: sql.table)
    |> select.limit(10, of: value.int)
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(10)])
}

pub fn select_with_limit_and_offset_test() {
  let expected = "SELECT * FROM users LIMIT ? OFFSET ?"
  let users = sql.identifier("users")

  let query =
    value.sql()
    |> select.from(users, of: sql.table)
    |> select.limit(20, of: value.int)
    |> select.offset(10, of: value.int)
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(20), value.int(10)])
}

pub fn select_with_offset_test() {
  let expected = "SELECT * FROM users LIMIT ? OFFSET ?"
  let users = sql.identifier("users")

  let query =
    value.sql()
    |> select.from(users, of: sql.table)
    |> select.limit(100, of: value.int)
    |> select.offset(50, of: value.int)
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(100), value.int(50)])
}

pub fn group_by_test() {
  let expected =
    "SELECT department, COUNT(*) FROM employees GROUP BY department"
  let employees = sql.identifier("employees")

  let query =
    value.sql()
    |> select.from(employees, of: sql.table)
    |> select.columns(["department", "COUNT(*)"])
    |> select.group_by(["department"])
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn multiple_group_by_test() {
  let expected =
    "SELECT department, location, COUNT(*) FROM employees GROUP BY department, location"
  let employees = sql.identifier("employees")

  let query =
    value.sql()
    |> select.from(employees, of: sql.table)
    |> select.columns(["department", "location", "COUNT(*)"])
    |> select.group_by(["department", "location"])
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn having_test() {
  let expected =
    "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > ?"
  let employees = sql.identifier("employees")

  let query =
    value.sql()
    |> select.from(employees, of: sql.table)
    |> select.columns(["department", "COUNT(*)"])
    |> select.group_by(["department"])
    |> select.having([
      sql.identifier("COUNT(*)")
      |> sql.column
      |> sql.gt(sql.value(value.int(5))),
    ])
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(5)])
}

pub fn multiple_having_test() {
  let expected =
    "SELECT department, AVG(salary) FROM employees GROUP BY department HAVING COUNT(*) > ? AND AVG(salary) > ?"
  let employees = sql.identifier("employees")

  let query =
    value.sql()
    |> select.from(employees, of: sql.table)
    |> select.columns(["department", "AVG(salary)"])
    |> select.group_by(["department"])
    |> select.having([
      sql.identifier("COUNT(*)")
        |> sql.column
        |> sql.gt(sql.value(value.int(5))),
      sql.identifier("AVG(salary)")
        |> sql.column
        |> sql.gt(sql.value(value.float(50_000.0))),
    ])
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(5), value.float(50_000.0)])
}

pub fn order_by_with_asc_test() {
  let expected = "SELECT * FROM users ORDER BY name ASC"
  let users = sql.identifier("users")

  let query =
    value.sql()
    |> select.from(users, of: sql.table)
    |> select.order_by(["name"])
    |> select.asc
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn order_by_with_desc_test() {
  let expected = "SELECT * FROM users ORDER BY created_at DESC"
  let users = sql.identifier("users")

  let query =
    value.sql()
    |> select.from(users, of: sql.table)
    |> select.order_by(["created_at"])
    |> select.desc
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn multiple_order_by_columns_test() {
  let expected = "SELECT * FROM users ORDER BY department, name ASC"
  let users = sql.identifier("users")

  let query =
    value.sql()
    |> select.from(users, of: sql.table)
    |> select.order_by(["department", "name"])
    |> select.asc
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn complex_query_with_order_by_test() {
  let expected =
    "SELECT department, COUNT(*) FROM employees WHERE active = ? GROUP BY department HAVING COUNT(*) > ? ORDER BY COUNT(*) DESC LIMIT ? OFFSET ?"
  let employees = sql.identifier("employees")

  let query =
    value.sql()
    |> select.from(employees, of: sql.table)
    |> select.columns(["department", "COUNT(*)"])
    |> select.where([
      sql.identifier("active")
      |> sql.column
      |> sql.eq(sql.value(value.bool(True))),
    ])
    |> select.group_by(["department"])
    |> select.having([
      sql.identifier("COUNT(*)")
      |> sql.column
      |> sql.gt(sql.value(value.int(10))),
    ])
    |> select.order_by(["COUNT(*)"])
    |> select.desc
    |> select.limit(5, of: value.int)
    |> select.offset(0, of: value.int)
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values
  |> should.equal([
    value.true,
    value.int(10),
    value.int(5),
    value.int(0),
  ])
}

pub fn from_subquery_test() {
  let expected =
    "SELECT name, department FROM (SELECT id, name, department FROM employees WHERE active = ?) WHERE name LIKE ?"
  let employees = sql.identifier("employees")

  let employees_query =
    value.sql()
    |> select.from(employees, of: sql.table)
    |> select.columns(["id", "name", "department"])
    |> select.where([
      sql.identifier("active")
      |> sql.column
      |> sql.eq(sql.value(value.bool(True))),
    ])
    |> select.to_query

  let query =
    value.sql()
    |> select.from(employees_query, of: sql.subquery)
    |> select.columns(["name", "department"])
    |> select.where([
      sql.identifier("name")
      |> sql.column
      |> sql.like("%John%", of: value.text),
    ])
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.true, value.text("%John%")])
}

pub fn complex_queried_with_aggregation_test() {
  let expected =
    "SELECT department, total_salary FROM (SELECT department, SUM(salary) as total_salary FROM employees GROUP BY department HAVING COUNT(*) > ?) WHERE total_salary > ?"
  let employees = sql.identifier("employees")

  let department_stats_query =
    value.sql()
    |> select.from(employees, of: sql.table)
    |> select.columns(["department", "SUM(salary) as total_salary"])
    |> select.group_by(["department"])
    |> select.having([
      sql.identifier("COUNT(*)")
      |> sql.column
      |> sql.gt(sql.value(value.int(10))),
    ])
    |> select.to_query

  let query =
    value.sql()
    |> select.from(department_stats_query, of: sql.subquery)
    |> select.columns(["department", "total_salary"])
    |> select.where([
      sql.identifier("total_salary")
      |> sql.column
      |> sql.gt(sql.value(value.float(1_000_000.0))),
    ])
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(10), value.float(1_000_000.0)])
}

pub fn for_update_test() {
  let expected = "SELECT * FROM users WHERE id = ? FOR UPDATE"
  let users = sql.identifier("users")

  let query =
    value.sql()
    |> select.from(users, of: sql.table)
    |> select.where([
      sql.identifier("id")
      |> sql.column
      |> sql.eq(sql.value(value.int(1))),
    ])
    |> select.for_update
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(1)])
}

pub fn complex_for_update_test() {
  let expected =
    "SELECT id, name, balance FROM accounts WHERE user_id = ? AND balance >= ? ORDER BY balance DESC LIMIT ? FOR UPDATE"
  let accounts = sql.identifier("accounts")

  let query =
    value.sql()
    |> select.from(accounts, of: sql.table)
    |> select.columns(["id", "name", "balance"])
    |> select.where([
      sql.identifier("user_id")
        |> sql.column
        |> sql.eq(sql.value(value.int(5))),
      sql.identifier("balance")
        |> sql.column
        |> sql.gt_eq(sql.value(value.float(1000.0))),
    ])
    |> select.order_by(["balance"])
    |> select.desc
    |> select.limit(3, of: value.int)
    |> select.for_update
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values
  |> should.equal([value.int(5), value.float(1000.0), value.int(3)])
}

pub fn format_placeholders_test() {
  let fmt =
    value.sql()
    |> sql.on_placeholder(fn(idx) { "$" <> int.to_string(idx) })

  let expected = "SELECT * FROM users WHERE id = $1 AND name = $2"
  let users = sql.identifier("users")

  let query =
    fmt
    |> select.from(users, of: sql.table)
    |> select.where([
      sql.identifier("id")
        |> sql.column
        |> sql.eq(sql.value(value.int(1))),
      sql.identifier("name")
        |> sql.column
        |> sql.eq(sql.value(value.text("John"))),
    ])
    |> select.to_query

  query.sql |> should.equal(expected)

  query.values
  |> should.equal([value.int(1), value.text("John")])
}

pub fn format_identifier_test() {
  let sql =
    value.sql()
    |> sql.on_identifier({ fn(value) { "\"" <> value <> "\"" } })

  let expected = "SELECT * FROM \"users\" WHERE \"id\" = ? AND \"name\" = ?"
  let users = sql.identifier("users")

  let query =
    sql
    |> select.from(users, of: sql.table)
    |> select.where([
      sql.identifier("id")
        |> sql.column
        |> sql.eq(sql.value(value.int(1))),
      sql.identifier("name")
        |> sql.column
        |> sql.eq(sql.value(value.text("John"))),
    ])
    |> select.to_query

  query.sql |> should.equal(expected)

  query.values
  |> should.equal([value.int(1), value.text("John")])
}

pub fn for_update_with_join_test() {
  let expected =
    "SELECT orders.id, orders.amount, users.name FROM orders INNER JOIN users ON orders.user_id = users.id WHERE orders.status = ? FOR UPDATE"
  let users = sql.identifier("users")
  let orders = sql.identifier("orders")

  let query =
    value.sql()
    |> select.from(orders, of: sql.table)
    |> select.columns(["orders.id", "orders.amount", "users.name"])
    |> select.join(users, of: sql.table, on: [
      sql.identifier("orders.user_id")
      |> sql.column
      |> sql.eq(sql.identifier("users.id") |> sql.column),
    ])
    |> select.where([
      sql.identifier("orders.status")
      |> sql.column
      |> sql.eq(sql.value(value.text("pending"))),
    ])
    |> select.for_update
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("pending")])
}

pub fn date_time_types_test() {
  let assert Ok(time) = timestamp.parse_rfc3339("2025-04-09T12:00:00Z")
  let date = calendar.Date(2023, calendar.December, 31)
  let events = sql.identifier("events")

  let query =
    value.sql()
    |> select.from(events, of: sql.table)
    |> select.where([
      sql.identifier("event_date")
        |> sql.column
        |> sql.eq(sql.value(value.date(date))),
      sql.identifier("event_timestamp")
        |> sql.column
        |> sql.eq(sql.value(value.timestamp(time))),
    ])
    |> select.to_query

  query.values
  |> should.equal([value.date(date), value.timestamp(time)])
}

pub fn time_type_test() {
  let time_of_day =
    calendar.TimeOfDay(
      hours: 14,
      minutes: 30,
      seconds: 45,
      nanoseconds: 123_456_789,
    )
  let events = sql.identifier("events")

  let query =
    value.sql()
    |> select.from(events, of: sql.table)
    |> select.where([
      sql.identifier("event_time")
      |> sql.column
      |> sql.eq(sql.value(value.time(time_of_day))),
    ])
    |> select.to_query

  query.values
  |> should.equal([value.time(time_of_day)])
}

pub fn duration_type_test() {
  let dur = duration.seconds(3661)
  let events = sql.identifier("events")

  let query =
    value.sql()
    |> select.from(events, of: sql.table)
    |> select.where([
      sql.identifier("event_duration")
      |> sql.column
      |> sql.eq(sql.value(value.interval(dur))),
    ])
    |> select.to_query

  query.values
  |> should.equal([value.interval(dur)])
}

pub fn different_value_types_test() {
  let products = sql.identifier("products")

  let query =
    value.sql()
    |> select.from(products, of: sql.table)
    |> select.where([
      sql.identifier("id")
        |> sql.column
        |> sql.eq(sql.value(value.int(123))),
      sql.identifier("price")
        |> sql.column
        |> sql.gt(sql.value(value.float(19.99))),
      sql.identifier("is_active")
        |> sql.column
        |> sql.eq(sql.value(value.bool(True))),
      sql.identifier("description")
        |> sql.column
        |> sql.eq(sql.value(value.null)),
    ])
    |> select.to_query

  query.values
  |> should.equal([
    value.int(123),
    value.float(19.99),
    value.true,
    value.null,
  ])
}

pub fn between_test() {
  let expected = "SELECT * FROM products WHERE price BETWEEN ? AND ?"
  let products = sql.identifier("products")

  let query =
    value.sql()
    |> select.from(products, of: sql.table)
    |> select.where([
      sql.identifier("price")
      |> sql.column
      |> sql.between(sql.value(value.float(10.0)), sql.value(value.float(50.0))),
    ])
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.float(10.0), value.float(50.0)])
}

pub fn complex_between_test() {
  let expected =
    "SELECT * FROM orders WHERE total BETWEEN ? AND ? AND created_at BETWEEN ? AND ? AND status = ?"

  let date_start = calendar.Date(2023, calendar.January, 1)
  let date_end = calendar.Date(2023, calendar.December, 31)
  let orders = sql.identifier("orders")

  let query =
    value.sql()
    |> select.from(orders, of: sql.table)
    |> select.where([
      sql.identifier("total")
        |> sql.column
        |> sql.between(
          sql.value(value.float(100.0)),
          sql.value(value.float(1000.0)),
        ),
      sql.identifier("created_at")
        |> sql.column
        |> sql.between(
          sql.value(value.date(date_start)),
          sql.value(value.date(date_end)),
        ),
      sql.identifier("status")
        |> sql.column
        |> sql.eq(sql.value(value.text("completed"))),
    ])
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values
  |> should.equal([
    value.float(100.0),
    value.float(1000.0),
    value.date(date_start),
    value.date(date_end),
    value.text("completed"),
  ])
}

pub fn not_between_test() {
  let expected = "SELECT * FROM products WHERE NOT price BETWEEN ? AND ?"
  let products = sql.identifier("products")

  let query =
    value.sql()
    |> select.from(products, of: sql.table)
    |> select.where([
      sql.not(
        sql.identifier("price")
        |> sql.column
        |> sql.between(
          sql.value(value.float(10.0)),
          sql.value(value.float(50.0)),
        ),
      ),
    ])
    |> select.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.float(10.0), value.float(50.0)])
}
