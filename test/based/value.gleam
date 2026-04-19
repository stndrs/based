import based/sql
import gleam/bit_array
import gleam/float
import gleam/function
import gleam/int
import gleam/list
import gleam/option.{type Option}
import gleam/string
import gleam/time/calendar
import gleam/time/duration
import gleam/time/timestamp

/// Handles all `Value` variants for `to_string` output.
pub fn adapter() -> sql.Adapter(Value) {
  sql.adapter()
  |> sql.on_placeholder(fn(_) { "?" })
  |> sql.on_identifier(function.identity)
  |> sql.on_value(to_string)
  |> sql.on_int(int)
  |> sql.on_text(text)
  |> sql.on_null(fn() { Null })
}

/// Example value type covering common SQL data types.
pub type Value {
  Null
  Bool(Bool)
  Int(Int)
  Float(Float)
  Text(String)
  Bytea(BitArray)
  Date(calendar.Date)
  Time(calendar.TimeOfDay)
  Datetime(calendar.Date, calendar.TimeOfDay)
  Timestamp(timestamp.Timestamp)
  Timestamptz(timestamp.Timestamp, duration.Duration)
  Array(List(Value))
}

/// The SQL `NULL` value.
pub const null = Null

/// The SQL boolean `TRUE` value.
pub const true = Bool(True)

/// The SQL boolean `FALSE` value.
pub const false = Bool(False)

/// Wraps a `Bool` as a `Value`.
pub fn bool(bool: Bool) -> Value {
  Bool(bool)
}

/// Wraps an `Int` as a `Value`.
pub fn int(int: Int) -> Value {
  Int(int)
}

/// Wraps a `Float` as a `Value`.
pub fn float(float: Float) -> Value {
  Float(float)
}

/// Wraps a `String` as a `Value`.
pub fn text(text: String) -> Value {
  Text(text)
}

/// Wraps a `BitArray` as a `Value` for binary/bytea columns.
pub fn bytea(bytea: BitArray) -> Value {
  Bytea(bytea)
}

/// Wraps a `calendar.Date` as a `Value`.
pub fn date(date: calendar.Date) -> Value {
  Date(date)
}

/// Wraps a `calendar.TimeOfDay` as a `Value`.
pub fn time(time_of_day: calendar.TimeOfDay) -> Value {
  Time(time_of_day)
}

/// Wraps a date and time as a `Value` for datetime columns.
pub fn datetime(date: calendar.Date, time: calendar.TimeOfDay) -> Value {
  Datetime(date, time)
}

/// Wraps a `timestamp.Timestamp` as a `Value`.
pub fn timestamp(timestamp: timestamp.Timestamp) -> Value {
  Timestamp(timestamp)
}

/// Wraps a `timestamp.Timestamp` and `Offset` as a `Value` for
/// timestamp-with-timezone columns.
pub fn timestamptz(
  timestamp: timestamp.Timestamp,
  offset: duration.Duration,
) -> Value {
  Timestamptz(timestamp, offset)
}

/// Wraps a list of elements as an `Array` value.
///
/// The `of` parameter specifies how to convert each element to a `Value`.
pub fn array(elements: List(a), of kind: fn(a) -> Value) -> Value {
  elements
  |> list.map(kind)
  |> Array
}

pub fn nullable(value: Option(a), of kind: fn(a) -> Value) -> Value {
  value
  |> option.map(kind)
  |> option.unwrap(Null)
}

pub fn to_string(value: Value) -> String {
  case value {
    Null -> "NULL"
    Bool(val) -> bool_to_string(val)
    Int(val) -> int.to_string(val)
    Float(val) -> float.to_string(val)
    Text(val) -> text_to_string(val)
    Bytea(val) -> bytea_to_string(val)
    Time(val) -> time_to_string(val)
    Date(val) -> date_to_string(val)
    Datetime(date, time) -> datetime_to_string(date, time)
    Timestamp(val) -> timestamp_to_string(val)
    Timestamptz(ts, offset) -> timestamptz_to_string(ts, offset)
    Array(val) -> array_to_string(val)
  }
}

fn array_to_string(array: List(Value)) -> String {
  let elems = case array {
    [] -> ""
    [val] -> to_string(val)
    vals -> {
      vals
      |> list.map(to_string)
      |> string.join(", ")
    }
  }

  "[" <> elems <> "]"
}

fn text_to_string(val: String) -> String {
  let val = string.replace(in: val, each: "'", with: "''")

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
  offset: duration.Duration,
) -> String {
  offset
  |> negate_duration
  |> timestamp.add(timestamp, _)
  |> timestamp_to_string
}

fn negate_duration(d: duration.Duration) -> duration.Duration {
  duration.difference(d, duration.seconds(0))
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
