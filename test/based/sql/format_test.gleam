import based/format
import gleam/int
import gleeunit/should

pub fn format_type_test() {
  let int_fmt =
    format.new()
    |> format.on_identifier(fn(s) { "\"" <> s <> "\"" })
    |> format.on_placeholder(fn(i) { "$" <> int.to_string(i) })
    |> format.on_value(int.to_string)

  format.to_identifier(int_fmt, "column") |> should.equal("\"column\"")
  format.to_placeholder(int_fmt, 1) |> should.equal("$1")
  format.to_string(int_fmt, 42) |> should.equal("42")
}

pub fn set_placeholder_test() {
  let int_fmt =
    format.new()
    |> format.on_identifier(fn(s) { s })
    |> format.on_placeholder(fn(i) { "$" <> int.to_string(i) })
    |> format.on_value(int.to_string)

  let int_fmt =
    format.on_placeholder(int_fmt, fn(i) { "?" <> int.to_string(i) })

  format.to_placeholder(int_fmt, 1) |> should.equal("?1")
  format.to_identifier(int_fmt, "column") |> should.equal("column")
  format.to_string(int_fmt, 42) |> should.equal("42")
}

pub fn set_identifier_test() {
  let int_fmt =
    format.new()
    |> format.on_identifier(fn(s) { s })
    |> format.on_placeholder(fn(i) { "$" <> int.to_string(i) })
    |> format.on_value(int.to_string)

  let int_fmt = format.on_identifier(int_fmt, fn(s) { "[" <> s <> "]" })

  format.to_identifier(int_fmt, "column") |> should.equal("[column]")
  format.to_placeholder(int_fmt, 1) |> should.equal("$1")
  format.to_string(int_fmt, 42) |> should.equal("42")
}

pub fn set_to_string_test() {
  let int_fmt =
    format.new()
    |> format.on_identifier(fn(s) { s })
    |> format.on_placeholder(fn(i) { "$" <> int.to_string(i) })
    |> format.on_value(int.to_string)

  let int_fmt = format.on_value(int_fmt, fn(i) { "num:" <> int.to_string(i) })

  format.to_string(int_fmt, 42) |> should.equal("num:42")
  format.to_identifier(int_fmt, "column") |> should.equal("column")
  format.to_placeholder(int_fmt, 1) |> should.equal("$1")
}
