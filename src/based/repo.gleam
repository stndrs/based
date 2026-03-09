import based/db
import based/interval
import based/sql/internal/fmt
import based/sql/internal/value
import based/uuid
import gleam/bit_array
import gleam/float
import gleam/function
import gleam/int
import gleam/list
import gleam/string
import gleam/time/calendar
import gleam/time/duration
import gleam/time/timestamp

/// Repo must be configured by adapter packages.
///
/// Example:
///
/// A PostgreSQL adapter might configure `Repo` like this:
///
/// ```gleam
/// let repo = repo.new()
///   |> repo.on_placeholder(fn(index) { "$" <> int.to_string(index) })
///   |> repo.on_identifier(function.identity)
///   |> repo.on_value(value.to_string)
/// ```
/// A MariaDB adapter might configure `Repo` like this:
///
/// ```gleam
/// let repo = repo.new()
///   |> repo.on_placeholder(fn(_index) { "?" })
///   |> repo.on_identifier(fn(ident) { "`" <> ident <> "`" })
///   |> repo.on_value(value.to_string)
/// ```
///
pub type Repo(v) {
  Repo(value_mapper: value.Mapper(v), fmt: fmt.Fmt(v))
}

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

pub fn on_int(repo: Repo(v), handle_int: fn(Int) -> v) -> Repo(v) {
  let value_mapper = value.on_int(repo.value_mapper, handle_int)

  Repo(..repo, value_mapper:)
}

pub fn on_null(repo: Repo(v), handle_null: fn() -> v) -> Repo(v) {
  let value_mapper = value.on_null(repo.value_mapper, handle_null)

  Repo(..repo, value_mapper:)
}

/// Returns a `Repo` with preset defaults. See `based/repo` for configuration
/// options. This `Repo` is configured to handle `db.Value` values. Use the
/// `based/repo` module to configure a `Repo` for use with custom value types.
///
/// The `Repo` returned by this function uses "?" as a placeholder in generated
/// SQL. It does not escape identifiers. Adapter packages likely need to
/// specify their desired identifier formatting and placeholders.
pub fn default() -> Repo(db.Value) {
  new()
  |> on_placeholder(fn(_) { "?" })
  |> on_identifier(function.identity)
  |> on_value(value_to_string)
  |> on_text(db.text)
  |> on_int(db.int)
  |> on_null(fn() { db.null })
}

fn value_to_string(value: db.Value) -> String {
  case value {
    db.Uuid(val) -> uuid.to_string(val)
    db.Null -> "NULL"
    db.Bool(val) -> bool_to_string(val)
    db.Int(val) -> int.to_string(val)
    db.Float(val) -> float.to_string(val)
    db.Text(val) -> text_to_string(val)
    db.Bytea(val) -> bytea_to_string(val)
    db.Time(val) -> time_to_string(val)
    db.Date(val) -> date_to_string(val)
    db.Datetime(date, time) -> datetime_to_string(date, time)
    db.Timestamp(val) -> timestamp_to_string(val)
    db.Timestamptz(ts, offset) -> timestamptz_to_string(ts, offset)
    db.Interval(val) -> interval.to_iso8601_string(val)
    db.Array(val) -> array_to_string(val)
  }
}

fn array_to_string(array: List(db.Value)) -> String {
  let elems = case array {
    [] -> ""
    [val] -> value_to_string(val)
    vals -> {
      vals
      |> list.map(value_to_string)
      |> string.join(", ")
    }
  }

  "[" <> elems <> "]"
}

fn text_to_string(val: String) -> String {
  let val = string.replace(in: val, each: "'", with: "\\'")

  single_quote(val)
}

fn bool_to_string(val: Bool) -> String {
  case val {
    True -> "TRUE"
    False -> "FALSE"
  }
}

fn bytea_to_string(val: BitArray) -> String {
  let val = "\\x" <> bit_array.base16_encode(val)

  single_quote(val)
}

fn date_to_string(date: calendar.Date) -> String {
  format_date(date) |> single_quote
}

fn datetime_to_string(dt: calendar.Date, tod: calendar.TimeOfDay) -> String {
  let date = format_date(dt)
  let time = format_time(tod)

  { date <> " " <> time }
  |> single_quote
}

fn time_to_string(tod: calendar.TimeOfDay) -> String {
  format_time(tod) |> single_quote
}

fn format_date(date: calendar.Date) -> String {
  let year = int.to_string(date.year)
  let month = calendar.month_to_int(date.month) |> pad_zero
  let day = pad_zero(date.day)

  year <> "-" <> month <> "-" <> day
}

fn format_time(tod: calendar.TimeOfDay) -> String {
  let hours = pad_zero(tod.hours)
  let minutes = pad_zero(tod.minutes)
  let seconds = pad_zero(tod.seconds)
  let milliseconds = tod.nanoseconds / 1_000_000

  let msecs = case milliseconds < 100 {
    True if milliseconds == 0 -> ""
    True if milliseconds < 10 -> ".00" <> int.to_string(milliseconds)
    True -> ".0" <> int.to_string(milliseconds)
    False -> "." <> int.to_string(milliseconds)
  }

  hours <> ":" <> minutes <> ":" <> seconds <> msecs
}

fn timestamp_to_string(ts: timestamp.Timestamp) -> String {
  timestamp.to_rfc3339(ts, calendar.utc_offset)
  |> single_quote
}

fn timestamptz_to_string(
  timestamp: timestamp.Timestamp,
  offset: db.Offset,
) -> String {
  offset_to_duration(offset)
  |> timestamp.add(timestamp, _)
  |> timestamp_to_string
}

fn offset_to_duration(offset: db.Offset) -> duration.Duration {
  let sign = case offset.hours < 0 {
    True -> 1
    False -> -1
  }

  int.absolute_value(offset.hours)
  |> int.multiply(60)
  |> int.add(offset.minutes)
  |> int.multiply(sign)
  |> duration.minutes
}

fn single_quote(val: String) -> String {
  "'" <> val <> "'"
}

fn pad_zero(n: Int) -> String {
  case n < 10 {
    True -> "0" <> int.to_string(n)
    False -> int.to_string(n)
  }
}
