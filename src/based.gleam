import based/sql/internal/fmt
import gleam/dynamic/decode
import gleam/time/calendar
import gleam/time/timestamp

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
  Repo(
    time_decoder: fn() -> decode.Decoder(calendar.TimeOfDay),
    timestamp_decoder: fn() -> decode.Decoder(timestamp.Timestamp),
    date_decoder: fn() -> decode.Decoder(calendar.Date),
    text_to_value: fn(String) -> v,
    null_to_value: fn() -> v,
    fmt: fmt.Fmt(v),
  )
}

pub fn repo() -> Repo(v) {
  Repo(
    time_decoder: fn() { panic as "based.Repo time_decoder not configured" },
    timestamp_decoder: fn() {
      panic as "based.Repo timestamp_decoder not configured"
    },
    date_decoder: fn() { panic as "based.Repo date_decoder not configured" },
    text_to_value: fn(_) { panic as "based.Repo text_to_value not configured" },
    null_to_value: fn() { panic as "based.Repo null_to_value not configured" },
    fmt: fmt.new(),
  )
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
pub fn on_text(repo: Repo(v), text_to_value: fn(String) -> v) -> Repo(v) {
  Repo(..repo, text_to_value:)
}

pub fn on_null(repo: Repo(v), null_to_value: fn() -> v) -> Repo(v) {
  Repo(..repo, null_to_value:)
}

pub fn time_decoder(
  repo: Repo(v),
  time_decoder: fn() -> decode.Decoder(calendar.TimeOfDay),
) -> Repo(v) {
  Repo(..repo, time_decoder:)
}

pub fn timestamp_decoder(
  repo: Repo(v),
  timestamp_decoder: fn() -> decode.Decoder(timestamp.Timestamp),
) -> Repo(v) {
  Repo(..repo, timestamp_decoder:)
}

pub fn date_decoder(
  repo: Repo(v),
  date_decoder: fn() -> decode.Decoder(calendar.Date),
) -> Repo(v) {
  Repo(..repo, date_decoder:)
}
