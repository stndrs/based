import based/sql
import based/sql/internal/builder
import based/sql/internal/fmt
import based/sql/internal/join
import based/value
import gleam/int
import gleam/option.{None, Some}
import gleeunit/should

pub fn to_string_test() {
  let format_value = fn(v) {
    case v {
      value.Text(s) -> "'" <> s <> "'"
      value.Int(i) -> ":" <> int.to_string(i)
      _ -> ""
    }
  }

  let format =
    fmt.new()
    |> fmt.on_identifier(fn(s) { "\"" <> s <> "\"" })
    |> fmt.on_placeholder(fn(i) { "$" <> int.to_string(i) })
    |> fmt.on_value(format_value)

  let result = builder.to_string("SELECT * FROM users", [], format)
  should.equal(result, "SELECT * FROM users")

  let sql = "SELECT * FROM users WHERE id = :param AND name = :param"
  let values = [value.Int(1), value.Text("John")]
  let result = builder.to_string(sql, values, format)
  should.equal(result, "SELECT * FROM users WHERE id = :1 AND name = 'John'")
}

pub fn placeholders_test() {
  let tree = "SELECT * FROM users WHERE id = :param AND age = :param"
  let mapper = fn(i) { "$" <> int.to_string(i) }

  let result =
    builder.placeholders(for: tree, on: fmt.placeholder, with: mapper)
  let expected = "SELECT * FROM users WHERE id = $1 AND age = $2"

  result |> should.equal(expected)
}

pub fn append_where_test() {
  let st = "SELECT * FROM users"

  let format =
    fmt.new()
    |> fmt.on_identifier(fn(s) { s })
    |> fmt.on_placeholder(fn(i) { "$" <> int.to_string(i) })
    |> fmt.on_value(fn(v) {
      case v {
        value.Int(i) -> int.to_string(i)
        value.Text(s) -> "'" <> s <> "'"
        _ -> ""
      }
    })

  let left_node = sql.identifier("id") |> sql.column
  let right_node = sql.value(value.int(1))
  let where_exprs = [[sql.eq(left_node, right_node)]]

  let result = builder.append_where(st, where_exprs, format)

  should.equal(result, "SELECT * FROM users WHERE id = :param")
}

pub fn append_group_by_test() {
  let st = "SELECT COUNT(*) FROM users"

  let result = builder.append_group_by(st, [])

  should.equal(result, "SELECT COUNT(*) FROM users")

  let result = builder.append_group_by(st, ["department"])

  should.equal(result, "SELECT COUNT(*) FROM users GROUP BY department")

  let result = builder.append_group_by(st, ["department", "role"])

  should.equal(result, "SELECT COUNT(*) FROM users GROUP BY department, role")
}

pub fn append_having_test() {
  let st = "SELECT department, COUNT(*) FROM users GROUP BY department"

  let format =
    fmt.new()
    |> fmt.on_identifier(fn(s) { s })
    |> fmt.on_placeholder(fn(i) { "$" <> int.to_string(i) })
    |> fmt.on_value(fn(v) {
      case v {
        value.Int(i) -> int.to_string(i)
        _ -> ""
      }
    })

  let count_node = sql.identifier("COUNT(*)") |> sql.column
  let value_node = sql.value(value.int(5))
  let having_exprs = [[sql.gt(count_node, value_node)]]

  let result = builder.append_having(st, having_exprs, format)

  should.equal(
    result,
    "SELECT department, COUNT(*) FROM users GROUP BY department HAVING COUNT(*) > :param",
  )
}

pub fn append_joins_test() {
  let st = "SELECT * FROM users"

  let format =
    fmt.new()
    |> fmt.on_identifier(fn(s) { s })
    |> fmt.on_placeholder(fn(i) { "$" <> int.to_string(i) })
    |> fmt.on_value(fn(_v) { "" })

  let users = sql.identifier("users")
  let posts = sql.identifier("posts")

  let users_id = users |> sql.attr("id")
  let posts_user_id = posts |> sql.attr("user_id")

  let joins = [
    join.Join(type_: join.InnerJoin, table: sql.table(posts), exprs: [
      users_id
      |> sql.column
      |> sql.eq(sql.column(posts_user_id)),
    ]),
  ]

  let result = builder.append_joins(st, joins, format)

  should.equal(
    result,
    "SELECT * FROM users INNER JOIN posts ON users.id = posts.user_id",
  )
}

pub fn append_order_by_test() {
  let st = "SELECT * FROM users"

  let result = builder.append_order_by(st, [], None)

  should.equal(result, "SELECT * FROM users")

  let result = builder.append_order_by(st, ["id"], Some(sql.Asc))

  should.equal(result, "SELECT * FROM users ORDER BY id ASC")

  let result = builder.append_order_by(st, ["created_at"], Some(sql.Desc))

  should.equal(result, "SELECT * FROM users ORDER BY created_at DESC")

  let result = builder.append_order_by(st, ["last_name", "first_name"], None)

  should.equal(result, "SELECT * FROM users ORDER BY last_name, first_name")
}

pub fn append_limit_test() {
  let st = "SELECT * FROM users"

  let result = builder.append_limit(st, None, None)

  should.equal(result, "SELECT * FROM users")

  let result = builder.append_limit(st, Some(10), None)

  should.equal(result, "SELECT * FROM users LIMIT :param")

  let result = builder.append_limit(st, Some(10), Some(20))

  should.equal(result, "SELECT * FROM users LIMIT :param OFFSET :param")
}

pub fn append_returning_test() {
  let st = "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')"

  let result = builder.append_returning(st, [])

  should.equal(
    result,
    "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')",
  )

  let result = builder.append_returning(st, ["id"])

  should.equal(
    result,
    "INSERT INTO users (name, email) VALUES ('John', 'john@example.com') RETURNING id",
  )

  let result = builder.append_returning(st, ["id", "created_at"])

  should.equal(
    result,
    "INSERT INTO users (name, email) VALUES ('John', 'john@example.com') RETURNING id, created_at",
  )
}
