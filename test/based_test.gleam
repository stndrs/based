import based
import based/sql/internal/fmt
import gleam/int
import gleeunit

pub fn main() {
  gleeunit.main()
}

pub fn on_placeholder_test() {
  let repo =
    based.repo()
    |> based.on_placeholder(fn(idx) { "$" <> int.to_string(idx) })

  // Test that placeholder function is set by using fmt
  assert "$1" == fmt.to_placeholder(repo.fmt, 1)
  assert "$42" == fmt.to_placeholder(repo.fmt, 42)
}

pub fn on_placeholder_question_mark_test() {
  let repo =
    based.repo()
    |> based.on_placeholder(fn(_) { "?" })

  assert "?" == fmt.to_placeholder(repo.fmt, 1)
  assert "?" == fmt.to_placeholder(repo.fmt, 99)
}

pub fn on_identifier_test() {
  let repo =
    based.repo()
    |> based.on_identifier(fn(ident) { "\"" <> ident <> "\"" })

  assert "\"users\"" == fmt.to_identifier(repo.fmt, "users")
  assert "\"column_name\"" == fmt.to_identifier(repo.fmt, "column_name")
}

pub fn on_identifier_backtick_test() {
  let repo =
    based.repo()
    |> based.on_identifier(fn(ident) { "`" <> ident <> "`" })

  assert "`users`" == fmt.to_identifier(repo.fmt, "users")
  assert "`table`" == fmt.to_identifier(repo.fmt, "table")
}

pub fn on_value_test() {
  let repo =
    based.repo()
    |> based.on_value(int.to_string)

  assert "42" == fmt.to_string(repo.fmt, 42)
  assert "123" == fmt.to_string(repo.fmt, 123)
}

pub fn chained_configuration_test() {
  let repo =
    based.repo()
    |> based.on_placeholder(fn(idx) { "$" <> int.to_string(idx) })
    |> based.on_identifier(fn(ident) { "\"" <> ident <> "\"" })
    |> based.on_value(int.to_string)

  assert "$1" == fmt.to_placeholder(repo.fmt, 1)
  assert "\"users\"" == fmt.to_identifier(repo.fmt, "users")
  assert "42" == fmt.to_string(repo.fmt, 42)
}
