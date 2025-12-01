import based/sql
import based/sql/internal/builder
import based/sql/internal/fmt
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
    |> string_tree.from_string
  }

  let format =
    sql.format()
    |> sql.on_identifier(fn(s) {
      string_tree.from_string("\\")
      |> string_tree.append_tree(s)
      |> string_tree.append("\\")
    })
    |> sql.on_placeholder(fn(i) {
      string_tree.from_string("$")
      |> string_tree.append(int.to_string(i))
    })
    |> sql.on_value(format_value)

  let result =
    "SELECT * FROM users"
    |> string_tree.from_string
    |> builder.to_string([], format)
  should.equal(result, "SELECT * FROM users")

  let sql =
    "SELECT * FROM users WHERE id = :param AND name = :param"
    |> string_tree.from_string

  let values = [value.Int(1), value.Text("John")]
  let result = builder.to_string(sql, values, format)
  should.equal(result, "SELECT * FROM users WHERE id = :1 AND name = 'John'")
}

pub fn placeholders_test() {
  let tree =
    string_tree.from_string(
      "SELECT * FROM users WHERE id = :param AND age = :param",
    )
  let mapper = fn(i) {
    string_tree.from_string("$")
    |> string_tree.append(int.to_string(i))
  }

  let result =
    builder.placeholders(for: tree, on: fmt.placeholder(), with: mapper)
  let expected = "SELECT * FROM users WHERE id = $1 AND age = $2"

  string_tree.to_string(result) |> should.equal(expected)
}

pub fn append_where_test() {
  let st = string_tree.from_string("SELECT * FROM users")

  let format =
    sql.format()
    |> sql.on_identifier(fn(s) { s })
    |> sql.on_placeholder(fn(i) {
      string_tree.from_string("$")
      |> string_tree.append(int.to_string(i))
    })
    |> sql.on_value(fn(v) {
      case v {
        value.Int(i) -> int.to_string(i)
        value.Text(s) -> "'" <> s <> "'"
        _ -> ""
      }
      |> string_tree.from_string
    })

  let left_node = sql.name("id") |> sql.column
  let right_node = sql.value(1, of: value.int)
  let where_exprs = [[sql.eq(left_node, right_node)]]

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
    sql.format()
    |> sql.on_identifier(fn(s) { s })
    |> sql.on_placeholder(fn(i) {
      string_tree.from_string("$")
      |> string_tree.append(int.to_string(i))
    })
    |> sql.on_value(fn(v) {
      case v {
        value.Int(i) -> int.to_string(i)
        _ -> ""
      }
      |> string_tree.from_string
    })

  let count_node = sql.name("COUNT(*)") |> sql.column
  let value_node = sql.value(5, of: value.Int)
  let having_exprs = [[sql.gt(count_node, value_node)]]

  let result =
    builder.append_having(st, having_exprs, format)
    |> string_tree.to_string

  should.equal(
    result,
    "SELECT department, COUNT(*) FROM users GROUP BY department HAVING COUNT(*) > :param",
  )
}

// pub fn append_joins_test() {
//   let st = string_tree.from_string("SELECT * FROM users")
//   let format =
//     sql.format()
//     |> sql.on_identifier(fn(s) { s })
//     |> sql.on_placeholder(fn(i) { "$" <> int.to_string(i) })
//     |> sql.on_value(fn(_v) { "" })
// 
//   let users = sql.table("users")
//   let posts = sql.table("posts")
// 
//   let users_id = column.new("id") |> column.for(users)
//   let posts_user_id = column.new("user_id") |> column.for(posts)
// 
//   let joins = [
//     sql.Join(type_: sql.InnerJoin, table: sql.table("posts"), exprs: [
//       sql.eq(sql.name(users_id), node.column(posts_user_id)),
//     ]),
//   ]
// 
//   let result =
//     builder.append_joins(st, joins, format)
//     |> string_tree.to_string
// 
//   should.equal(
//     result,
//     "SELECT * FROM users INNER JOIN posts ON users.id = posts.user_id",
//   )
// }

pub fn append_order_by_test() {
  let st = string_tree.from_string("SELECT * FROM users")

  let result =
    builder.append_order_by(st, [], None)
    |> string_tree.to_string

  should.equal(result, "SELECT * FROM users")

  let result =
    builder.append_order_by(st, ["id"], Some(sql.Asc))
    |> string_tree.to_string

  should.equal(result, "SELECT * FROM users ORDER BY id ASC")

  let result =
    builder.append_order_by(st, ["created_at"], Some(sql.Desc))
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
