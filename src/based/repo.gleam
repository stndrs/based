import based/sql/internal/fmt
import based/sql/internal/value

/// Repo must be configured by adapter packages.
///
/// Example:
///
/// A PostgreSQL adapter might configure `Repo` like this:
///
/// ```gleam
/// let repo = based.repo()
///   |> based.on_placeholder(fn(index) { "$" <> int.to_string(index) })
///   |> based.on_identifier(function.identity)
///   |> based.on_value(value.to_string)
/// ```
/// A MariaDB adapter might configure `Repo` like this:
///
/// ```gleam
/// let repo = based.repo()
///   |> based.on_placeholder(fn(_index) { "?" })
///   |> based.on_identifier(fn(ident) { "`" <> ident <> "`" })
///   |> based.on_value(value.to_string)
/// ```
///
pub type Repo(v) {
  Repo(value_mapper: value.Mapper(v), fmt: fmt.Fmt(v))
}

// Decoders
// Value mappers
// query functions
// Formatting

pub fn new() -> Repo(v) {
  Repo(value_mapper: value.mapper(), fmt: fmt.new())
}

/// Sets the placeholder formatting function.
pub fn on_placeholder(
  repo: Repo(v),
  handle_placeholder: fn(Int) -> String,
) -> Repo(v) {
  let fmt = fmt.on_placeholder(repo.fmt, handle_placeholder)

  Repo(..repo, fmt:)
}

/// Set the identifier formatting function.
pub fn on_identifier(
  repo: Repo(v),
  handle_identifier: fn(String) -> String,
) -> Repo(v) {
  let fmt = fmt.on_identifier(repo.fmt, handle_identifier)

  Repo(..repo, fmt:)
}

/// Set the value formatting function.
pub fn on_value(repo: Repo(v), handle_value: fn(v) -> String) -> Repo(v) {
  let fmt = fmt.on_value(repo.fmt, handle_value)

  Repo(..repo, fmt:)
}

/// Set the text to value function.
pub fn on_text(repo: Repo(v), handle_text: fn(String) -> v) -> Repo(v) {
  let value_mapper = value.on_text(repo.value_mapper, handle_text)

  Repo(..repo, value_mapper:)
}

pub fn on_null(repo: Repo(v), handle_null: fn() -> v) -> Repo(v) {
  let value_mapper = value.on_null(repo.value_mapper, handle_null)

  Repo(..repo, value_mapper:)
}
