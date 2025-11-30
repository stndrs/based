import based/format
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

  let users = sql.table("users")

  let query =
    sql.select(["id", "name"])
    |> select.from(users)
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn select_alias_test() {
  let expected = "SELECT user_id AS id FROM user_posts"

  let users_table = sql.table("user_posts")
  let query =
    sql.select(["user_id AS id"])
    |> select.from(users_table)
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
}

pub fn select_distincts_test() {
  let expected = "SELECT DISTINCT id, name FROM users"

  let users_table = sql.table("users")
  let query =
    sql.select(["id, name"])
    |> select.distinct
    |> select.from(users_table)
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn select_subquery_test() {
  let expected =
    "SELECT title FROM posts WHERE created_at > ? AND user_id = (SELECT id FROM users WHERE name = ?)"

  let users_table = sql.table("users")
  let posts_table = sql.table("posts")

  let subquery =
    sql.select(["id"])
    |> select.from(users_table)
    |> select.where([sql.column("name") |> sql.eq(sql.text("Human Person"))])
    |> select.to_subquery(value.format())

  let query =
    sql.select(["title"])
    |> select.from(posts_table)
    |> select.where([
      sql.column("created_at") |> sql.gt(sql.text("2024-01-01")),
      sql.column("user_id") |> sql.eq(subquery),
    ])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values
  |> should.equal([value.text("2024-01-01"), value.text("Human Person")])
}

pub fn select_or_test() {
  let expected = "SELECT * FROM users WHERE name = ? OR email = ?"

  let users_table = sql.table("users")
  let query =
    sql.select(["*"])
    |> select.from(users_table)
    |> select.where([
      sql.column("name")
      |> sql.eq(sql.text("Human Person"))
      |> sql.or(
        sql.column("email") |> sql.eq(sql.text("human.person@example.com")),
      ),
    ])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values
  |> should.equal([
    value.text("Human Person"),
    value.text("human.person@example.com"),
  ])
}

pub fn select_where_not_test() {
  let expected = "SELECT * FROM users WHERE NOT email = ?"

  let users = sql.table("users")
  let query =
    sql.select(["*"])
    |> select.from(users)
    |> select.where_not([
      sql.column("email") |> sql.eq(sql.text("Human Person")),
    ])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("Human Person")])
}

pub fn select_where_not_like_test() {
  let expected = "SELECT * FROM users WHERE email NOT LIKE ?"

  let users = sql.table("users")
  let query =
    sql.select(["*"])
    |> select.from(users)
    |> select.where([
      sql.column("email") |> sql.not_like("Human Person", of: sql.text),
    ])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("Human Person")])
}

pub fn select_distinct_test() {
  let expected = "SELECT DISTINCT value FROM users"

  let users = sql.table("users")
  let query =
    sql.select(["value"])
    |> select.distinct
    |> select.from(users)
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn select_with_join_test() {
  let expected =
    "SELECT * FROM users INNER JOIN posts ON users.id = posts.user_id AND posts.title LIKE ?"

  let users = sql.table("users")
  let posts = sql.table("posts")
  let query =
    sql.select(["*"])
    |> select.from(users)
    |> select.join(posts, [
      sql.column("users.id") |> sql.eq(sql.column("posts.user_id")),
      sql.column("posts.title") |> sql.like("%gleam%", of: sql.text),
    ])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("%gleam%")])
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
    sql.select(["*"])
    |> select.from(users)
    |> select.join(posts, [
      sql.column("users.id") |> sql.eq(sql.column("posts.user_id")),
      sql.column("posts.title") |> sql.like("%gleam%", of: sql.text),
    ])
    |> select.left_join(comments, [
      sql.column("posts.comment_id") |> sql.eq(sql.column("comments.id")),
    ])
    |> select.right_join(tags, [
      sql.column("tags.id") |> sql.eq(sql.column("posts.tag_id")),
    ])
    |> select.full_join(followers, [
      sql.column("followers.user_id") |> sql.eq(sql.column("users.id")),
    ])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("%gleam%")])
}

pub fn select_with_in_test() {
  let expected = "SELECT * FROM users WHERE id IN (?, ?, ?)"

  let users = sql.table("users")
  let query =
    sql.select(["*"])
    |> select.from(users)
    |> select.where([
      sql.column("id") |> sql.in(sql.list([1, 2, 3], of: sql.int)),
    ])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(1), value.int(2), value.int(3)])
}

pub fn select_with_in_tuples_test() {
  let expected =
    "SELECT * FROM posts WHERE (id, user_id) IN ((?, ?), (?, ?), (?, ?))"

  let posts = sql.table("posts")
  let query =
    sql.select(["*"])
    |> select.from(posts)
    |> select.where([
      sql.columns(["id", "user_id"])
      |> sql.in(
        sql.tuples([
          [sql.int(1), sql.int(10)],
          [sql.int(2), sql.int(10)],
          [sql.int(3), sql.int(10)],
        ]),
      ),
    ])
    |> select.to_query(value.format())

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
  let expected = "SELECT * FROM users WHERE deleted_at IS ?"

  let users = sql.table("users")
  let query =
    sql.select(["*"])
    |> select.from(users)
    |> select.where([sql.column("deleted_at") |> sql.is(sql.null)])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.null])
}

pub fn select_with_is_not_null_test() {
  let expected = "SELECT * FROM users WHERE active IS ?"

  let users = sql.table("users")
  let query =
    sql.select(["*"])
    |> select.from(users)
    |> select.where([sql.column("active") |> sql.is(sql.true)])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.true])
}

pub fn select_wildcard_test() {
  let expected = "SELECT * FROM users"

  let users = sql.table("users")
  let query =
    sql.select(["*"])
    |> select.from(users)
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn where_lt_test() {
  let expected = "SELECT * FROM users WHERE age < ?"

  let users = sql.table("users")
  let query =
    sql.select(["*"])
    |> select.from(users)
    |> select.where([sql.column("age") |> sql.lt(sql.int(65))])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(65)])
}

pub fn where_lt_eq_test() {
  let expected = "SELECT * FROM users WHERE age <= ?"

  let users = sql.table("users")
  let query =
    sql.select(["*"])
    |> select.from(users)
    |> select.where([sql.column("age") |> sql.lt_eq(sql.int(65))])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(65)])
}

pub fn where_not_eq_test() {
  let expected = "SELECT * FROM users WHERE status <> ?"

  let users = sql.table("users")
  let query =
    sql.select(["*"])
    |> select.from(users)
    |> select.where([sql.column("status") |> sql.not_eq(sql.text("inactive"))])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("inactive")])
}

pub fn multiple_where_test() {
  let expected = "SELECT * FROM users WHERE age > ? AND status = ?"

  let users = sql.table("users")
  let query =
    sql.select(["*"])
    |> select.from(users)
    |> select.where([
      sql.column("age") |> sql.gt(sql.int(18)),
      sql.column("status") |> sql.eq(sql.text("active")),
    ])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(18), value.text("active")])
}

pub fn join_with_multiple_conditions_to_string_test() {
  let expected =
    "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id AND orders.status = 'completed' WHERE users.age > 20 AND users.active = TRUE"

  let users = sql.table("users")
  let orders = sql.table("orders")

  let query =
    sql.select(["*"])
    |> select.from(users)
    |> select.join(orders, [
      sql.column("users.id") |> sql.eq(sql.column("orders.user_id")),
      sql.column("orders.status") |> sql.eq(sql.text("completed")),
    ])
    |> select.where([
      sql.column("users.age") |> sql.gt(sql.int(20)),
      sql.column("users.active") |> sql.eq(sql.true),
    ])
    |> select.to_string(value.format())

  query |> should.equal(expected)
}

pub fn select_with_limit_test() {
  let expected = "SELECT * FROM users LIMIT ?"

  let users = sql.table("users")
  let query =
    sql.select(["*"])
    |> select.from(users)
    |> select.limit(10, of: value.int)
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(10)])
}

pub fn select_with_limit_and_offset_test() {
  let expected = "SELECT * FROM users LIMIT ? OFFSET ?"

  let users = sql.table("users")
  let query =
    sql.select(["*"])
    |> select.from(users)
    |> select.limit(20, of: value.int)
    |> select.offset(10, of: value.int)
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(20), value.int(10)])
}

pub fn select_with_offset_test() {
  let expected = "SELECT * FROM users LIMIT ? OFFSET ?"

  let users = sql.table("users")
  let query =
    sql.select(["*"])
    |> select.from(users)
    |> select.limit(100, of: value.int)
    |> select.offset(50, of: value.int)
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(100), value.int(50)])
}

pub fn group_by_test() {
  let expected =
    "SELECT department, COUNT(*) FROM employees GROUP BY department"

  let employees = sql.table("employees")
  let query =
    sql.select(["department", "COUNT(*)"])
    |> select.from(employees)
    |> select.group_by(["department"])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn multiple_group_by_test() {
  let expected =
    "SELECT department, location, COUNT(*) FROM employees GROUP BY department, location"

  let employees = sql.table("employees")
  let query =
    sql.select(["department", "location", "COUNT(*)"])
    |> select.from(employees)
    |> select.group_by(["department", "location"])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn having_test() {
  let expected =
    "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > ?"

  let employees = sql.table("employees")
  let query =
    sql.select(["department", "COUNT(*)"])
    |> select.from(employees)
    |> select.group_by(["department"])
    |> select.having([sql.column("COUNT(*)") |> sql.gt(sql.int(5))])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(5)])
}

pub fn multiple_having_test() {
  let expected =
    "SELECT department, AVG(salary) FROM employees GROUP BY department HAVING COUNT(*) > ? AND AVG(salary) > ?"

  let employees = sql.table("employees")
  let query =
    sql.select(["department", "AVG(salary)"])
    |> select.from(employees)
    |> select.group_by(["department"])
    |> select.having([
      sql.column("COUNT(*)") |> sql.gt(sql.int(5)),
      sql.column("AVG(salary)") |> sql.gt(sql.float(50_000.0)),
    ])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(5), value.float(50_000.0)])
}

pub fn order_by_with_asc_test() {
  let expected = "SELECT * FROM users ORDER BY name ASC"

  let users = sql.table("users")
  let query =
    sql.select(["*"])
    |> select.from(users)
    |> select.order_by(["name"])
    |> select.asc
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn order_by_with_desc_test() {
  let expected = "SELECT * FROM users ORDER BY created_at DESC"

  let users = sql.table("users")
  let query =
    sql.select(["*"])
    |> select.from(users)
    |> select.order_by(["created_at"])
    |> select.desc
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn multiple_order_by_columns_test() {
  let expected = "SELECT * FROM users ORDER BY department, name ASC"

  let users = sql.table("users")
  let query =
    sql.select(["*"])
    |> select.from(users)
    |> select.order_by(["department", "name"])
    |> select.asc
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn complex_query_with_order_by_test() {
  let expected =
    "SELECT department, COUNT(*) FROM employees WHERE active = ? GROUP BY department HAVING COUNT(*) > ? ORDER BY COUNT(*) DESC LIMIT ? OFFSET ?"

  let employees = sql.table("employees")
  let query =
    sql.select(["department", "COUNT(*)"])
    |> select.from(employees)
    |> select.where([sql.column("active") |> sql.eq(sql.true)])
    |> select.group_by(["department"])
    |> select.having([sql.column("COUNT(*)") |> sql.gt(sql.int(10))])
    |> select.order_by(["COUNT(*)"])
    |> select.desc
    |> select.limit(5, of: value.int)
    |> select.offset(0, of: value.int)
    |> select.to_query(value.format())

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

  let employees = sql.table("employees")

  let employees_query =
    sql.select(["id", "name", "department"])
    |> select.from(employees)
    |> select.where([sql.column("active") |> sql.eq(sql.true)])
    |> select.to_table(value.format())

  let query =
    sql.select(["name", "department"])
    |> select.from(employees_query)
    |> select.where([sql.column("name") |> sql.like("%John%", of: sql.text)])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.true, value.text("%John%")])
}

pub fn complex_queried_with_aggregation_test() {
  let expected =
    "SELECT department, total_salary FROM (SELECT department, SUM(salary) as total_salary FROM employees GROUP BY department HAVING COUNT(*) > ?) WHERE total_salary > ?"

  let employees = sql.table("employees")

  let department_stats_query =
    sql.select(["department", "SUM(salary) as total_salary"])
    |> select.from(employees)
    |> select.group_by(["department"])
    |> select.having([sql.column("COUNT(*)") |> sql.gt(sql.int(10))])
    |> select.to_table(value.format())

  let query =
    sql.select(["department", "total_salary"])
    |> select.from(department_stats_query)
    |> select.where([
      sql.column("total_salary") |> sql.gt(sql.float(1_000_000.0)),
    ])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(10), value.float(1_000_000.0)])
}

pub fn for_update_test() {
  let expected = "SELECT * FROM users WHERE id = ? FOR UPDATE"

  let users = sql.table("users")
  let query =
    sql.select(["*"])
    |> select.from(users)
    |> select.where([sql.column("id") |> sql.eq(sql.int(1))])
    |> select.for_update
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(1)])
}

pub fn complex_for_update_test() {
  let expected =
    "SELECT id, name, balance FROM accounts WHERE user_id = ? AND balance >= ? ORDER BY balance DESC LIMIT ? FOR UPDATE"

  let accounts = sql.table("accounts")
  let query =
    sql.select(["id", "name", "balance"])
    |> select.from(accounts)
    |> select.where([
      sql.column("user_id") |> sql.eq(sql.int(5)),
      sql.column("balance") |> sql.gt_eq(sql.float(1000.0)),
    ])
    |> select.order_by(["balance"])
    |> select.desc
    |> select.limit(3, of: value.int)
    |> select.for_update
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values
  |> should.equal([value.int(5), value.float(1000.0), value.int(3)])
}

pub fn format_placeholders_test() {
  let fmt =
    value.format()
    |> format.on_placeholder(fn(idx) { "$" <> int.to_string(idx) })

  let expected = "SELECT * FROM users WHERE id = $1 AND name = $2"

  let query =
    sql.select(["*"])
    |> select.from(sql.table("users"))
    |> select.where([
      sql.column("id") |> sql.eq(sql.int(1)),
      sql.column("name") |> sql.eq(sql.text("John")),
    ])
    |> select.to_query(fmt)

  query.sql |> should.equal(expected)

  query.values
  |> should.equal([value.int(1), value.text("John")])
}

pub fn format_identifier_test() {
  let fmt =
    value.format()
    |> format.on_identifier({ fn(value) { "\"" <> value <> "\"" } })

  let expected = "SELECT * FROM \"users\" WHERE \"id\" = ? AND \"name\" = ?"

  let query =
    sql.select(["*"])
    |> select.from(sql.table("users"))
    |> select.where([
      sql.column("id") |> sql.eq(sql.int(1)),
      sql.column("name") |> sql.eq(sql.text("John")),
    ])
    |> select.to_query(fmt)

  query.sql |> should.equal(expected)

  query.values
  |> should.equal([value.int(1), value.text("John")])
}

pub fn for_update_with_join_test() {
  let expected =
    "SELECT orders.id, orders.amount, users.name FROM orders INNER JOIN users ON orders.user_id = users.id WHERE orders.status = ? FOR UPDATE"

  let orders = sql.table("orders")
  let users = sql.table("users")

  let query =
    sql.select(["orders.id", "orders.amount", "users.name"])
    |> select.from(orders)
    |> select.join(users, [
      sql.column("orders.user_id") |> sql.eq(sql.column("users.id")),
    ])
    |> select.where([sql.column("orders.status") |> sql.eq(sql.text("pending"))])
    |> select.for_update
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("pending")])
}

pub fn date_time_types_test() {
  let assert Ok(time) = timestamp.parse_rfc3339("2025-04-09T12:00:00Z")
  let date = calendar.Date(2023, calendar.December, 31)

  let query =
    sql.select(["*"])
    |> select.from(sql.table("events"))
    |> select.where([
      sql.column("event_date") |> sql.eq(sql.date(date)),
      sql.column("event_timestamp")
        |> sql.eq(sql.timestamp(time)),
    ])
    |> select.to_query(value.format())

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

  let query =
    sql.select(["*"])
    |> select.from(sql.table("events"))
    |> select.where([sql.column("event_time") |> sql.eq(sql.time(time_of_day))])
    |> select.to_query(value.format())

  query.values
  |> should.equal([value.time(time_of_day)])
}

pub fn datetime_type_test() {
  let date = calendar.Date(2025, calendar.April, 13)
  let time_of_day =
    calendar.TimeOfDay(
      hours: 14,
      minutes: 30,
      seconds: 45,
      nanoseconds: 123_456_789,
    )

  let query =
    sql.select(["*"])
    |> select.from(sql.table("events"))
    |> select.where([
      sql.column("event_datetime") |> sql.eq(sql.datetime(date, time_of_day)),
    ])
    |> select.to_query(value.format())

  query.values
  |> should.equal([value.datetime(date, time_of_day)])
}

pub fn duration_type_test() {
  let dur = duration.seconds(3661)

  let query =
    sql.select(["*"])
    |> select.from(sql.table("events"))
    |> select.where([sql.column("event_duration") |> sql.eq(sql.interval(dur))])
    |> select.to_query(value.format())

  query.values
  |> should.equal([value.interval(dur)])
}

pub fn different_value_types_test() {
  let query =
    sql.select(["*"])
    |> select.from(sql.table("products"))
    |> select.where([
      sql.column("id") |> sql.eq(sql.int(123)),
      sql.column("price") |> sql.gt(sql.float(19.99)),
      sql.column("is_active") |> sql.eq(sql.true),
      sql.column("description") |> sql.eq(sql.null),
    ])
    |> select.to_query(value.format())

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

  let query =
    sql.select(["*"])
    |> select.from(sql.table("products"))
    |> select.where([
      sql.column("price") |> sql.between(sql.float(10.0), sql.float(50.0)),
    ])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.float(10.0), value.float(50.0)])
}

pub fn complex_between_test() {
  let expected =
    "SELECT * FROM orders WHERE total BETWEEN ? AND ? AND created_at BETWEEN ? AND ? AND status = ?"

  let date_start = calendar.Date(2023, calendar.January, 1)
  let date_end = calendar.Date(2023, calendar.December, 31)

  let query =
    sql.select(["*"])
    |> select.from(sql.table("orders"))
    |> select.where([
      sql.column("total") |> sql.between(sql.float(100.0), sql.float(1000.0)),
      sql.column("created_at")
        |> sql.between(sql.date(date_start), sql.date(date_end)),
      sql.column("status") |> sql.eq(sql.text("completed")),
    ])
    |> select.to_query(value.format())

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

  let query =
    sql.select(["*"])
    |> select.from(sql.table("products"))
    |> select.where([
      sql.not(
        sql.column("price") |> sql.between(sql.float(10.0), sql.float(50.0)),
      ),
    ])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.float(10.0), value.float(50.0)])
}
