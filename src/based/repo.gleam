import based/sql/internal/fmt
import based/sql/internal/value
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
  Repo(decode: Decode, value_mapper: value.ValueMapper(v), fmt: fmt.Fmt(v))
}

pub type Decode {
  Decode(
    time: fn() -> decode.Decoder(calendar.TimeOfDay),
    timestamp: fn() -> decode.Decoder(timestamp.Timestamp),
    date: fn() -> decode.Decoder(calendar.Date),
  )
}

fn decode() -> Decode {
  Decode(
    time: fn() { panic as "based.Decode time not configured" },
    timestamp: fn() { panic as "based.Decode timestamp not configured" },
    date: fn() { panic as "based.Decode date not configured" },
  )
}

// Decoders
// Value mappers
// query functions
// Formatting

pub fn new() -> Repo(v) {
  Repo(decode: decode(), value_mapper: value.new(), fmt: fmt.new())
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

pub fn time_decoder(
  repo: Repo(v),
  time: fn() -> decode.Decoder(calendar.TimeOfDay),
) -> Repo(v) {
  let decode = Decode(..repo.decode, time:)

  Repo(..repo, decode:)
}

pub fn timestamp_decoder(
  repo: Repo(v),
  timestamp: fn() -> decode.Decoder(timestamp.Timestamp),
) -> Repo(v) {
  let decode = Decode(..repo.decode, timestamp:)

  Repo(..repo, decode:)
}

pub fn date_decoder(
  repo: Repo(v),
  date: fn() -> decode.Decoder(calendar.Date),
) -> Repo(v) {
  let decode = Decode(..repo.decode, date:)

  Repo(..repo, decode:)
}
