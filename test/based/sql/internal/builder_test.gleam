import based/sql
import based/sql/column
import based/sql/internal/builder
import based/sql/internal/fmt
import based/value
import gleam/int
import gleam/option.{None, Some}

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

  assert "SELECT * FROM users" == result

  let sql = "SELECT * FROM users WHERE id = :param AND name = :param"
  let values = [value.Int(1), value.Text("John")]
  let result = builder.to_string(sql, values, format)

  assert "SELECT * FROM users WHERE id = :1 AND name = 'John'" == result
}

pub fn placeholders_test() {
  let tree = "SELECT * FROM users WHERE id = :param AND age = :param"
  let mapper = fn(i) { "$" <> int.to_string(i) }

  let result =
    builder.placeholders(for: tree, on: fmt.placeholder, with: mapper)
  let expected = "SELECT * FROM users WHERE id = $1 AND age = $2"

  assert expected == result
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

  let left_node = column.new("id")
  let right_node = value.int(1)
  let #(condition, _values) = sql.eq(left_node, right_node, of: sql.value)

  let result = builder.append_where(st, [[condition]], format)

  assert "SELECT * FROM users WHERE id = :param" == result
}

pub fn append_group_by_test() {
  let st = "SELECT COUNT(*) FROM users"

  let result = builder.append_group_by(st, [])

  assert "SELECT COUNT(*) FROM users" == result

  let result = builder.append_group_by(st, ["department"])

  assert "SELECT COUNT(*) FROM users GROUP BY department" == result

  let result = builder.append_group_by(st, ["department", "role"])

  assert "SELECT COUNT(*) FROM users GROUP BY department, role" == result
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

  let count_node = column.new("COUNT(*)")
  let value_node = value.int(5)
  let #(condition, _values) = sql.gt(count_node, value_node, of: sql.value)

  let result = builder.append_having(st, [[condition]], format)

  assert "SELECT department, COUNT(*) FROM users GROUP BY department HAVING COUNT(*) > :param"
    == result
}

pub fn append_joins_test() {
  let st = "SELECT * FROM users"

  let format =
    fmt.new()
    |> fmt.on_identifier(fn(s) { s })
    |> fmt.on_placeholder(fn(i) { "$" <> int.to_string(i) })
    |> fmt.on_value(fn(_v) { "" })

  let users = sql.table("users")
  let posts = sql.table("posts")

  let users_id = column.new("id") |> column.for(users)
  let posts_user_id = column.new("user_id") |> column.for(posts)

  let #(join_condition, _values) =
    users_id |> sql.eq(posts_user_id, of: sql.value)

  let joins = [
    sql.inner_join(posts, [join_condition]),
  ]

  let result = builder.append_joins(st, joins, format)

  assert "SELECT * FROM users INNER JOIN posts ON users.id = :param" == result
}

pub fn append_order_by_test() {
  let st = "SELECT * FROM users"

  let result = builder.append_order_by(st, [], None)

  assert "SELECT * FROM users" == result

  let result = builder.append_order_by(st, ["id"], Some(sql.Asc))

  assert "SELECT * FROM users ORDER BY id ASC" == result

  let result = builder.append_order_by(st, ["created_at"], Some(sql.Desc))

  assert "SELECT * FROM users ORDER BY created_at DESC" == result

  let result = builder.append_order_by(st, ["last_name", "first_name"], None)

  assert "SELECT * FROM users ORDER BY last_name, first_name" == result
}

pub fn append_limit_test() {
  let st = "SELECT * FROM users"

  let result = builder.append_limit(st, None, None)

  assert "SELECT * FROM users" == result

  let result = builder.append_limit(st, Some(10), None)

  assert "SELECT * FROM users LIMIT :param" == result

  let result = builder.append_limit(st, Some(10), Some(20))

  assert "SELECT * FROM users LIMIT :param OFFSET :param" == result
}

pub fn append_returning_test() {
  let st = "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')"

  let result = builder.append_returning(st, [])

  assert "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')"
    == result

  let result = builder.append_returning(st, ["id"])

  assert "INSERT INTO users (name, email) VALUES ('John', 'john@example.com') RETURNING id"
    == result

  let result = builder.append_returning(st, ["id", "created_at"])

  assert "INSERT INTO users (name, email) VALUES ('John', 'john@example.com') RETURNING id, created_at"
    == result
}
