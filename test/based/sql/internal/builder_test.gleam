import based/format
import based/sql/column
import based/sql/expr
import based/sql/internal/builder
import based/sql/join
import based/sql/node
import based/sql/table
import based/value
import gleam/int
import gleam/option.{None, Some}
import gleam/string_tree
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
    format.new()
    |> format.on_identifier(fn(s) { "\"" <> s <> "\"" })
    |> format.on_placeholder(fn(i) { "$" <> int.to_string(i) })
    |> format.on_string(format_value)

  let result = builder.to_string("SELECT * FROM users", [], format)
  should.equal(result, "SELECT * FROM users")

  let sql = "SELECT * FROM users WHERE id = :param AND name = :param"
  let values = [value.Int(1), value.Text("John")]
  let result = builder.to_string(sql, values, format)
  should.equal(result, "SELECT * FROM users WHERE id = :1 AND name = 'John'")
}

pub fn placeholders_test() {
  let tree =
    string_tree.from_string(
      "SELECT * FROM users WHERE id = :param AND age = :param",
    )
  let mapper = fn(i) { "$" <> int.to_string(i) }

  let result = builder.placeholders(for: tree, on: ":param", with: mapper)
  let expected = "SELECT * FROM users WHERE id = $1 AND age = $2"

  string_tree.to_string(result) |> should.equal(expected)
}

pub fn append_where_test() {
  let st = string_tree.from_string("SELECT * FROM users")

  let format =
    format.new()
    |> format.on_identifier(fn(s) { s })
    |> format.on_placeholder(fn(i) { "$" <> int.to_string(i) })
    |> format.on_string(fn(v) {
      case v {
        value.Int(i) -> int.to_string(i)
        value.Text(s) -> "'" <> s <> "'"
        _ -> ""
      }
    })

  let left_col = column.new("id")
  let left_node = node.column(left_col)
  let right_node = node.literal(value.Int(1))
  let where_exprs = [[expr.eq(left_node, right_node)]]

  let result =
    builder.append_where(st, where_exprs, format)
    |> string_tree.to_string

  should.equal(result, "SELECT * FROM users WHERE id = :param")
}

pub fn append_group_by_test() {
  let st = string_tree.from_string("SELECT COUNT(*) FROM users")

  let result =
    builder.append_group_by(st, [])
    |> string_tree.to_string

  should.equal(result, "SELECT COUNT(*) FROM users")

  let result =
    builder.append_group_by(st, ["department"])
    |> string_tree.to_string

  should.equal(result, "SELECT COUNT(*) FROM users GROUP BY department")

  let result =
    builder.append_group_by(st, ["department", "role"])
    |> string_tree.to_string

  should.equal(result, "SELECT COUNT(*) FROM users GROUP BY department, role")
}

pub fn append_having_test() {
  let st =
    string_tree.from_string(
      "SELECT department, COUNT(*) FROM users GROUP BY department",
    )
  let format =
    format.new()
    |> format.on_identifier(fn(s) { s })
    |> format.on_placeholder(fn(i) { "$" <> int.to_string(i) })
    |> format.on_string(fn(v) {
      case v {
        value.Int(i) -> int.to_string(i)
        _ -> ""
      }
    })

  let count_col = column.new("COUNT(*)")
  let count_node = node.column(count_col)
  let value_node = node.literal(value.Int(5))
  let having_exprs = [[expr.gt(count_node, value_node)]]

  let result =
    builder.append_having(st, having_exprs, format)
    |> string_tree.to_string

  should.equal(
    result,
    "SELECT department, COUNT(*) FROM users GROUP BY department HAVING COUNT(*) > :param",
  )
}

pub fn append_joins_test() {
  let st = string_tree.from_string("SELECT * FROM users")
  let format =
    format.new()
    |> format.on_identifier(fn(s) { s })
    |> format.on_placeholder(fn(i) { "$" <> int.to_string(i) })
    |> format.on_string(fn(_v) { "" })

  let users = table.new("users")
  let posts = table.new("posts")

  let users_id = column.new("id") |> column.for(users)
  let posts_user_id = column.new("user_id") |> column.for(posts)

  let joins = [
    join.Join(type_: join.InnerJoin, table: table.new("posts"), exprs: [
      expr.eq(node.column(users_id), node.column(posts_user_id)),
    ]),
  ]

  let result =
    builder.append_joins(st, joins, format)
    |> string_tree.to_string

  should.equal(
    result,
    "SELECT * FROM users INNER JOIN posts ON users.id = posts.user_id",
  )
}

pub fn append_order_by_test() {
  let st = string_tree.from_string("SELECT * FROM users")

  let result =
    builder.append_order_by(st, [], None)
    |> string_tree.to_string

  should.equal(result, "SELECT * FROM users")

  let result =
    builder.append_order_by(st, ["id"], Some(node.Asc))
    |> string_tree.to_string

  should.equal(result, "SELECT * FROM users ORDER BY id ASC")

  let result =
    builder.append_order_by(st, ["created_at"], Some(node.Desc))
    |> string_tree.to_string

  should.equal(result, "SELECT * FROM users ORDER BY created_at DESC")

  let result =
    builder.append_order_by(st, ["last_name", "first_name"], None)
    |> string_tree.to_string

  should.equal(result, "SELECT * FROM users ORDER BY last_name, first_name")
}

pub fn append_limit_test() {
  let st = string_tree.from_string("SELECT * FROM users")

  let result =
    builder.append_limit(st, None, None)
    |> string_tree.to_string

  should.equal(result, "SELECT * FROM users")

  let result =
    builder.append_limit(st, Some(10), None)
    |> string_tree.to_string

  should.equal(result, "SELECT * FROM users LIMIT :param")

  let result =
    builder.append_limit(st, Some(10), Some(20))
    |> string_tree.to_string

  should.equal(result, "SELECT * FROM users LIMIT :param OFFSET :param")
}

pub fn append_optional_test() {
  let st = string_tree.from_string("SELECT * FROM users")

  let result =
    builder.append_optional(st, None, fn(_) {
      string_tree.append(st, " WHERE id = 1")
    })
    |> string_tree.to_string

  should.equal(result, "SELECT * FROM users")

  let result =
    builder.append_optional(st, Some(1), fn(_) {
      string_tree.append(st, " WHERE id = 1")
    })
    |> string_tree.to_string

  should.equal(result, "SELECT * FROM users WHERE id = 1")
}

pub fn append_returning_test() {
  let st =
    string_tree.from_string(
      "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')",
    )

  let result =
    builder.append_returning(st, [])
    |> string_tree.to_string

  should.equal(
    result,
    "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')",
  )

  let result =
    builder.append_returning(st, ["id"])
    |> string_tree.to_string

  should.equal(
    result,
    "INSERT INTO users (name, email) VALUES ('John', 'john@example.com') RETURNING id",
  )

  let result =
    builder.append_returning(st, ["id", "created_at"])
    |> string_tree.to_string

  should.equal(
    result,
    "INSERT INTO users (name, email) VALUES ('John', 'john@example.com') RETURNING id, created_at",
  )
}
