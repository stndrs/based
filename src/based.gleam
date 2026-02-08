import based/db
import based/interval
import based/repo.{type Repo}
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

/// Returns a `Repo` with preset defaults. See `based/repo` for configuration
/// options. This `Repo` is configured to handle `db.Value` values. Use the
/// `based/repo` module to configure a `Repo` for use with custom value types.
///
/// The `Repo` returned by this function uses "?" as a placeholder in generated
/// SQL. It does not escape identifiers. Adapter packages likely need to
/// specify their desired identifier formatting and placeholders.
pub fn repo() -> Repo(db.Value) {
  repo.new()
  |> repo.on_placeholder(fn(_) { "?" })
  |> repo.on_identifier(function.identity)
  |> repo.on_value(value_to_string)
  |> repo.on_text(db.text)
  |> repo.on_null(fn() { db.null })
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
  let year = int.to_string(date.year)
  let month = calendar.month_to_int(date.month) |> pad_zero
  let day = pad_zero(date.day)

  let date = year <> "-" <> month <> "-" <> day

  single_quote(date)
}

fn datetime_to_string(dt: calendar.Date, tod: calendar.TimeOfDay) -> String {
  date_to_string(dt) <> " " <> time_to_string(tod)
}

fn time_to_string(tod: calendar.TimeOfDay) -> String {
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

  let time = hours <> ":" <> minutes <> ":" <> seconds <> msecs

  single_quote(time)
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
