import based/sql/internal/fmt
import gleam/int

pub fn format_test() {
  let int_fmt =
    fmt.new()
    |> fmt.on_identifier(fn(s) { "\"" <> s <> "\"" })
    |> fmt.on_placeholder(fn(i) { "$" <> int.to_string(i) })
    |> fmt.on_value(int.to_string)

  assert "\"column\"" == fmt.to_identifier(int_fmt, "column")
  assert "$1" == fmt.to_placeholder(int_fmt, 1)
  assert "42" == fmt.to_string(int_fmt, 42)
}

pub fn on_placeholder_test() {
  let int_fmt =
    fmt.new()
    |> fmt.on_identifier(fn(s) { s })
    |> fmt.on_placeholder(fn(i) { "$" <> int.to_string(i) })
    |> fmt.on_value(int.to_string)

  let int_fmt = fmt.on_placeholder(int_fmt, fn(_) { "?" })

  assert "?" == fmt.to_placeholder(int_fmt, 1)
  assert "column" == fmt.to_identifier(int_fmt, "column")
  assert "42" == fmt.to_string(int_fmt, 42)
}

pub fn on_node_test() {
  let int_fmt =
    fmt.new()
    |> fmt.on_identifier(fn(s) { s })
    |> fmt.on_placeholder(fn(i) { "$" <> int.to_string(i) })
    |> fmt.on_value(int.to_string)

  let int_fmt = fmt.on_identifier(int_fmt, fn(s) { "[" <> s <> "]" })

  assert "[column]" == fmt.to_identifier(int_fmt, "column")
  assert "$1" == fmt.to_placeholder(int_fmt, 1)
  assert "42" == fmt.to_string(int_fmt, 42)
}

pub fn on_value_test() {
  let int_fmt =
    fmt.new()
    |> fmt.on_identifier(fn(s) { s })
    |> fmt.on_placeholder(fn(i) { "$" <> int.to_string(i) })
    |> fmt.on_value(int.to_string)

  let int_fmt = fmt.on_value(int_fmt, fn(i) { "num:" <> int.to_string(i) })

  assert "num:42" == fmt.to_string(int_fmt, 42)
  assert "column" == fmt.to_identifier(int_fmt, "column")
  assert "$1" == fmt.to_placeholder(int_fmt, 1)
}
