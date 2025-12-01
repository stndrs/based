import based/sql
import based/sql/select
import based/value
import gleam/int
import gleam/string_tree
import gleam/time/calendar
import gleam/time/duration
import gleam/time/timestamp
import gleeunit/should

pub fn select_test() {
  let expected = "SELECT id, name FROM users"

  let users = sql.name("users") |> sql.table

  let query =
    select.from(users)
    |> select.columns(["id", "name"])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn select_alias_test() {
  let expected = "SELECT user_id AS id FROM user_posts"
  let user_posts = sql.name("user_posts") |> sql.table

  let query =
    select.from(user_posts)
    |> select.columns(["user_id AS id"])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
}

pub fn select_distincts_test() {
  let expected = "SELECT DISTINCT id, name FROM users"
  let users = sql.name("users") |> sql.table

  let query =
    select.from(users)
    |> select.columns(["id, name"])
    |> select.distinct
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn select_subquery_test() {
  let expected =
    "SELECT title FROM posts WHERE created_at > ? AND user_id = (SELECT id FROM users WHERE name = ?)"
  let users = sql.name("users") |> sql.table
  let posts = sql.name("posts") |> sql.table

  let subquery =
    select.from(users)
    |> select.columns(["id"])
    |> select.where([
      sql.name("name")
      |> sql.column
      |> sql.eq(sql.value("Human Person", of: value.text)),
    ])
    |> select.to_subquery(value.format())

  let query =
    select.from(posts)
    |> select.columns(["title"])
    |> select.where([
      sql.name("created_at")
        |> sql.column
        |> sql.gt(sql.value("2024-01-01", of: value.text)),
      sql.name("user_id")
        |> sql.column
        |> sql.eq(subquery),
    ])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values
  |> should.equal([value.text("2024-01-01"), value.text("Human Person")])
}

pub fn select_or_test() {
  let expected = "SELECT * FROM users WHERE name = ? OR email = ?"
  let users = sql.name("users") |> sql.table

  let query =
    select.from(users)
    |> select.where([
      sql.name("name")
      |> sql.column
      |> sql.eq(sql.value("Human Person", of: value.text))
      |> sql.or(
        sql.name("email")
        |> sql.column
        |> sql.eq(sql.value("human.person@example.com", of: value.text)),
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
  let users = sql.name("users") |> sql.table

  let query =
    select.from(users)
    |> select.where_not([
      sql.name("email")
      |> sql.column
      |> sql.eq(sql.value("Human Person", of: value.text)),
    ])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("Human Person")])
}

pub fn select_where_not_like_test() {
  let expected = "SELECT * FROM users WHERE email NOT LIKE ?"
  let users = sql.name("users") |> sql.table

  let query =
    select.from(users)
    |> select.where([
      sql.name("email")
      |> sql.column
      |> sql.not_like("Human Person", of: sql.value(_, value.text)),
    ])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("Human Person")])
}

pub fn select_distinct_test() {
  let expected = "SELECT DISTINCT value FROM users"
  let users = sql.name("users") |> sql.table

  let query =
    select.from(users)
    |> select.columns(["value"])
    |> select.distinct
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn select_with_join_test() {
  let expected =
    "SELECT * FROM users INNER JOIN posts ON users.id = posts.user_id AND posts.title LIKE ?"

  let users = sql.name("users") |> sql.table
  let posts = sql.name("posts") |> sql.table

  let query =
    select.from(users)
    |> select.join(posts, [
      sql.name("users.id")
        |> sql.column
        |> sql.eq(sql.name("posts.user_id") |> sql.column),
      sql.name("posts.title")
        |> sql.column
        |> sql.like("%gleam%", of: sql.value(_, value.text)),
    ])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("%gleam%")])
}

pub fn select_with_multiple_joins_test() {
  let expected =
    "SELECT * FROM users INNER JOIN posts ON users.id = posts.user_id AND posts.title LIKE ? LEFT JOIN comments ON posts.comment_id = comments.id RIGHT JOIN tags ON tags.id = posts.tag_id FULL OUTER JOIN followers ON followers.user_id = users.id"

  let users = sql.name("users") |> sql.table
  let posts = sql.name("posts") |> sql.table
  let comments = sql.name("comments") |> sql.table
  let tags = sql.name("tags") |> sql.table
  let followers = sql.name("followers") |> sql.table

  let query =
    select.from(users)
    |> select.join(posts, [
      sql.name("users.id")
        |> sql.column
        |> sql.eq(sql.name("posts.user_id") |> sql.column),
      sql.name("posts.title")
        |> sql.column
        |> sql.like("%gleam%", of: sql.value(_, value.text)),
    ])
    |> select.left_join(comments, [
      sql.name("posts.comment_id")
      |> sql.column
      |> sql.eq(sql.name("comments.id") |> sql.column),
    ])
    |> select.right_join(tags, [
      sql.name("tags.id")
      |> sql.column
      |> sql.eq(sql.name("posts.tag_id") |> sql.column),
    ])
    |> select.full_join(followers, [
      sql.name("followers.user_id")
      |> sql.column
      |> sql.eq(sql.name("users.id") |> sql.column),
    ])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("%gleam%")])
}

pub fn select_with_in_test() {
  let expected = "SELECT * FROM users WHERE id IN (?, ?, ?)"
  let users = sql.name("users") |> sql.table

  let query =
    select.from(users)
    |> select.where([
      sql.name("id")
      |> sql.column
      |> sql.in(sql.list([1, 2, 3], of: sql.value(_, value.int))),
    ])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(1), value.int(2), value.int(3)])
}

pub fn select_with_in_tuples_test() {
  let expected =
    "SELECT * FROM posts WHERE (id, user_id) IN ((?, ?), (?, ?), (?, ?))"
  let posts = sql.name("posts") |> sql.table

  let query =
    select.from(posts)
    |> select.where([
      sql.columns(["id", "user_id"])
      |> sql.in(
        sql.tuples([
          [sql.value(1, of: value.int), sql.value(10, of: value.int)],
          [sql.value(2, of: value.int), sql.value(10, of: value.int)],
          [sql.value(3, of: value.int), sql.value(10, of: value.int)],
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
  let users = sql.name("users") |> sql.table

  let query =
    select.from(users)
    |> select.where([
      sql.name("deleted_at")
      |> sql.column
      |> sql.is(sql.value(Nil, value.null)),
    ])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.null(Nil)])
}

pub fn select_with_is_not_null_test() {
  let expected = "SELECT * FROM users WHERE active IS ?"
  let users = sql.name("users") |> sql.table

  let query =
    select.from(users)
    |> select.where([
      sql.name("active")
      |> sql.column
      |> sql.is(sql.value(True, value.bool)),
    ])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.true])
}

pub fn select_wildcard_test() {
  let expected = "SELECT * FROM users"
  let users = sql.name("users") |> sql.table

  let query =
    select.from(users)
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn where_lt_test() {
  let expected = "SELECT * FROM users WHERE age < ?"
  let users = sql.name("users") |> sql.table

  let query =
    select.from(users)
    |> select.where([
      sql.name("age")
      |> sql.column
      |> sql.lt(sql.value(65, of: value.int)),
    ])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(65)])
}

pub fn where_lt_eq_test() {
  let expected = "SELECT * FROM users WHERE age <= ?"
  let users = sql.name("users") |> sql.table

  let query =
    select.from(users)
    |> select.where([
      sql.name("age")
      |> sql.column
      |> sql.lt_eq(sql.value(65, of: value.int)),
    ])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(65)])
}

pub fn where_not_eq_test() {
  let expected = "SELECT * FROM users WHERE status <> ?"
  let users = sql.name("users") |> sql.table

  let query =
    select.from(users)
    |> select.where([
      sql.name("status")
      |> sql.column
      |> sql.not_eq(sql.value("inactive", of: value.text)),
    ])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("inactive")])
}

pub fn multiple_where_test() {
  let expected = "SELECT * FROM users WHERE age > ? AND status = ?"
  let users = sql.name("users") |> sql.table

  let query =
    select.from(users)
    |> select.where([
      sql.name("age")
        |> sql.column
        |> sql.gt(sql.value(18, of: value.int)),
      sql.name("status")
        |> sql.column
        |> sql.eq(sql.value("active", of: value.text)),
    ])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(18), value.text("active")])
}

pub fn join_with_multiple_conditions_to_string_test() {
  let expected =
    "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id AND orders.status = 'completed' WHERE users.age > 20 AND users.active = TRUE"

  let users = sql.name("users") |> sql.table
  let orders = sql.name("orders") |> sql.table

  let query =
    select.from(users)
    |> select.join(orders, [
      sql.name("users.id")
        |> sql.column
        |> sql.eq(sql.name("orders.user_id") |> sql.column),
      sql.name("orders.status")
        |> sql.column
        |> sql.eq(sql.value("completed", of: value.text)),
    ])
    |> select.where([
      sql.name("users.age")
        |> sql.column
        |> sql.gt(sql.value(20, of: value.int)),
      sql.name("users.active")
        |> sql.column
        |> sql.eq(sql.value(True, value.bool)),
    ])
    |> select.to_string(value.format())

  query |> should.equal(expected)
}

pub fn select_with_limit_test() {
  let expected = "SELECT * FROM users LIMIT ?"
  let users = sql.name("users") |> sql.table

  let query =
    select.from(users)
    |> select.limit(10, of: value.int)
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(10)])
}

pub fn select_with_limit_and_offset_test() {
  let expected = "SELECT * FROM users LIMIT ? OFFSET ?"
  let users = sql.name("users") |> sql.table

  let query =
    select.from(users)
    |> select.limit(20, of: value.int)
    |> select.offset(10, of: value.int)
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(20), value.int(10)])
}

pub fn select_with_offset_test() {
  let expected = "SELECT * FROM users LIMIT ? OFFSET ?"
  let users = sql.name("users") |> sql.table

  let query =
    select.from(users)
    |> select.limit(100, of: value.int)
    |> select.offset(50, of: value.int)
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(100), value.int(50)])
}

pub fn group_by_test() {
  let expected =
    "SELECT department, COUNT(*) FROM employees GROUP BY department"
  let employees = sql.name("employees") |> sql.table

  let query =
    select.from(employees)
    |> select.columns(["department", "COUNT(*)"])
    |> select.group_by(["department"])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn multiple_group_by_test() {
  let expected =
    "SELECT department, location, COUNT(*) FROM employees GROUP BY department, location"
  let employees = sql.name("employees") |> sql.table

  let query =
    select.from(employees)
    |> select.columns(["department", "location", "COUNT(*)"])
    |> select.group_by(["department", "location"])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn having_test() {
  let expected =
    "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > ?"
  let employees = sql.name("employees") |> sql.table

  let query =
    select.from(employees)
    |> select.columns(["department", "COUNT(*)"])
    |> select.group_by(["department"])
    |> select.having([
      sql.name("COUNT(*)")
      |> sql.column
      |> sql.gt(sql.value(5, of: value.int)),
    ])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(5)])
}

pub fn multiple_having_test() {
  let expected =
    "SELECT department, AVG(salary) FROM employees GROUP BY department HAVING COUNT(*) > ? AND AVG(salary) > ?"
  let employees = sql.name("employees") |> sql.table

  let query =
    select.from(employees)
    |> select.columns(["department", "AVG(salary)"])
    |> select.group_by(["department"])
    |> select.having([
      sql.name("COUNT(*)")
        |> sql.column
        |> sql.gt(sql.value(5, of: value.int)),
      sql.name("AVG(salary)")
        |> sql.column
        |> sql.gt(sql.value(50_000.0, of: value.float)),
    ])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(5), value.float(50_000.0)])
}

pub fn order_by_with_asc_test() {
  let expected = "SELECT * FROM users ORDER BY name ASC"
  let users = sql.name("users") |> sql.table

  let query =
    select.from(users)
    |> select.order_by(["name"])
    |> select.asc
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn order_by_with_desc_test() {
  let expected = "SELECT * FROM users ORDER BY created_at DESC"
  let users = sql.name("users") |> sql.table

  let query =
    select.from(users)
    |> select.order_by(["created_at"])
    |> select.desc
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn multiple_order_by_columns_test() {
  let expected = "SELECT * FROM users ORDER BY department, name ASC"
  let users = sql.name("users") |> sql.table

  let query =
    select.from(users)
    |> select.order_by(["department", "name"])
    |> select.asc
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([])
}

pub fn complex_query_with_order_by_test() {
  let expected =
    "SELECT department, COUNT(*) FROM employees WHERE active = ? GROUP BY department HAVING COUNT(*) > ? ORDER BY COUNT(*) DESC LIMIT ? OFFSET ?"
  let employees = sql.name("employees") |> sql.table

  let query =
    select.from(employees)
    |> select.columns(["department", "COUNT(*)"])
    |> select.where([
      sql.name("active")
      |> sql.column
      |> sql.eq(sql.value(True, of: value.bool)),
    ])
    |> select.group_by(["department"])
    |> select.having([
      sql.name("COUNT(*)")
      |> sql.column
      |> sql.gt(sql.value(10, of: value.int)),
    ])
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
  let employees = sql.name("employees") |> sql.table

  let employees_query =
    select.from(employees)
    |> select.columns(["id", "name", "department"])
    |> select.where([
      sql.name("active")
      |> sql.column
      |> sql.eq(sql.value(True, of: value.bool)),
    ])
    |> select.to_table(value.format())

  let query =
    select.from(employees_query)
    |> select.columns(["name", "department"])
    |> select.where([
      sql.name("name")
      |> sql.column
      |> sql.like("%John%", of: sql.value(_, value.text)),
    ])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.true, value.text("%John%")])
}

pub fn complex_queried_with_aggregation_test() {
  let expected =
    "SELECT department, total_salary FROM (SELECT department, SUM(salary) as total_salary FROM employees GROUP BY department HAVING COUNT(*) > ?) WHERE total_salary > ?"
  let employees = sql.name("employees") |> sql.table

  let department_stats_query =
    select.from(employees)
    |> select.columns(["department", "SUM(salary) as total_salary"])
    |> select.group_by(["department"])
    |> select.having([
      sql.name("COUNT(*)")
      |> sql.column
      |> sql.gt(sql.value(10, of: value.int)),
    ])
    |> select.to_table(value.format())

  let query =
    select.from(department_stats_query)
    |> select.columns(["department", "total_salary"])
    |> select.where([
      sql.name("total_salary")
      |> sql.column
      |> sql.gt(sql.value(1_000_000.0, of: value.float)),
    ])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(10), value.float(1_000_000.0)])
}

pub fn for_update_test() {
  let expected = "SELECT * FROM users WHERE id = ? FOR UPDATE"
  let users = sql.name("users") |> sql.table

  let query =
    select.from(users)
    |> select.where([
      sql.name("id")
      |> sql.column
      |> sql.eq(sql.value(1, of: value.int)),
    ])
    |> select.for_update
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.int(1)])
}

pub fn complex_for_update_test() {
  let expected =
    "SELECT id, name, balance FROM accounts WHERE user_id = ? AND balance >= ? ORDER BY balance DESC LIMIT ? FOR UPDATE"
  let accounts = sql.name("accounts") |> sql.table

  let query =
    select.from(accounts)
    |> select.columns(["id", "name", "balance"])
    |> select.where([
      sql.name("user_id")
        |> sql.column
        |> sql.eq(sql.value(5, of: value.int)),
      sql.name("balance")
        |> sql.column
        |> sql.gt_eq(sql.value(1000.0, of: value.float)),
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
    |> sql.on_placeholder(fn(i) {
      string_tree.from_string("$")
      |> string_tree.append(int.to_string(i))
    })

  let expected = "SELECT * FROM users WHERE id = $1 AND name = $2"
  let users = sql.name("users") |> sql.table

  let query =
    select.from(users)
    |> select.where([
      sql.name("id")
        |> sql.column
        |> sql.eq(sql.value(1, of: value.int)),
      sql.name("name")
        |> sql.column
        |> sql.eq(sql.value("John", of: value.text)),
    ])
    |> select.to_query(fmt)

  query.sql |> should.equal(expected)

  query.values
  |> should.equal([value.int(1), value.text("John")])
}

pub fn format_identifier_test() {
  let fmt =
    value.format()
    |> sql.on_identifier(fn(s) {
      string_tree.from_string("\"")
      |> string_tree.append_tree(s)
      |> string_tree.append("\"")
    })

  let expected = "SELECT * FROM \"users\" WHERE \"id\" = ? AND \"name\" = ?"
  let users = sql.name("users") |> sql.table

  let query =
    select.from(users)
    |> select.where([
      sql.name("id")
        |> sql.column
        |> sql.eq(sql.value(1, of: value.int)),
      sql.name("name")
        |> sql.column
        |> sql.eq(sql.value("John", of: value.text)),
    ])
    |> select.to_query(fmt)

  query.sql |> should.equal(expected)

  query.values
  |> should.equal([value.int(1), value.text("John")])
}

pub fn for_update_with_join_test() {
  let expected =
    "SELECT orders.id, orders.amount, users.name FROM orders INNER JOIN users ON orders.user_id = users.id WHERE orders.status = ? FOR UPDATE"
  let users = sql.name("users") |> sql.table
  let orders = sql.name("orders") |> sql.table

  let query =
    select.from(orders)
    |> select.columns(["orders.id", "orders.amount", "users.name"])
    |> select.join(users, [
      sql.name("orders.user_id")
      |> sql.column
      |> sql.eq(sql.name("users.id") |> sql.column),
    ])
    |> select.where([
      sql.name("orders.status")
      |> sql.column
      |> sql.eq(sql.value("pending", of: value.text)),
    ])
    |> select.for_update
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.text("pending")])
}

pub fn date_time_types_test() {
  let assert Ok(time) = timestamp.parse_rfc3339("2025-04-09T12:00:00Z")
  let date = calendar.Date(2023, calendar.December, 31)
  let events = sql.name("events") |> sql.table

  let query =
    select.from(events)
    |> select.where([
      sql.name("event_date")
        |> sql.column
        |> sql.eq(sql.value(date, of: value.date)),
      sql.name("event_timestamp")
        |> sql.column
        |> sql.eq(sql.value(time, of: value.timestamp)),
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
  let events = sql.name("events") |> sql.table

  let query =
    select.from(events)
    |> select.where([
      sql.name("event_time")
      |> sql.column
      |> sql.eq(sql.value(time_of_day, of: value.time)),
    ])
    |> select.to_query(value.format())

  query.values
  |> should.equal([value.time(time_of_day)])
}

pub fn duration_type_test() {
  let dur = duration.seconds(3661)
  let events = sql.name("events") |> sql.table

  let query =
    select.from(events)
    |> select.where([
      sql.name("event_duration")
      |> sql.column
      |> sql.eq(sql.value(dur, value.interval)),
    ])
    |> select.to_query(value.format())

  query.values
  |> should.equal([value.interval(dur)])
}

pub fn different_value_types_test() {
  let products = sql.name("products") |> sql.table

  let query =
    select.from(products)
    |> select.where([
      sql.name("id")
        |> sql.column
        |> sql.eq(sql.value(123, of: value.int)),
      sql.name("price")
        |> sql.column
        |> sql.gt(sql.value(19.99, of: value.float)),
      sql.name("is_active")
        |> sql.column
        |> sql.eq(sql.value(True, of: value.bool)),
      sql.name("description")
        |> sql.column
        |> sql.eq(sql.value(Nil, value.null)),
    ])
    |> select.to_query(value.format())

  query.values
  |> should.equal([
    value.int(123),
    value.float(19.99),
    value.true,
    value.null(Nil),
  ])
}

pub fn between_test() {
  let expected = "SELECT * FROM products WHERE price BETWEEN ? AND ?"
  let products = sql.name("products") |> sql.table

  let query =
    select.from(products)
    |> select.where([
      sql.name("price")
      |> sql.column
      |> sql.between(
        sql.value(10.0, of: value.float),
        sql.value(50.0, of: value.float),
      ),
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
  let orders = sql.name("orders") |> sql.table

  let query =
    select.from(orders)
    |> select.where([
      sql.name("total")
        |> sql.column
        |> sql.between(
          sql.value(100.0, of: value.float),
          sql.value(1000.0, of: value.float),
        ),
      sql.name("created_at")
        |> sql.column
        |> sql.between(
          sql.value(date_start, of: value.date),
          sql.value(date_end, of: value.date),
        ),
      sql.name("status")
        |> sql.column
        |> sql.eq(sql.value("completed", of: value.text)),
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
  let products = sql.name("products") |> sql.table

  let query =
    select.from(products)
    |> select.where([
      sql.not(
        sql.name("price")
        |> sql.column
        |> sql.between(
          sql.value(10.0, of: value.float),
          sql.value(50.0, of: value.float),
        ),
      ),
    ])
    |> select.to_query(value.format())

  query.sql |> should.equal(expected)
  query.values |> should.equal([value.float(10.0), value.float(50.0)])
}
