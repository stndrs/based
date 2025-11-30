import based/sql
import gleam/bit_array
import gleam/float
import gleam/function
import gleam/int
import gleam/option.{type Option, None, Some}
import gleam/string
import gleam/time/calendar
import gleam/time/duration
import gleam/time/timestamp

// Value

pub type Value {
  Text(String)
  Int(Int)
  Float(Float)
  Bool(Bool)
  Bytea(BitArray)
  Date(calendar.Date)
  Time(calendar.TimeOfDay)
  Datetime(date: calendar.Date, time: calendar.TimeOfDay)
  Timestamp(timestamp.Timestamp)
  Interval(duration.Duration)
  Null
}

fn value_to_string(value: Value) -> String {
  case value {
    Text(val) -> text_to_string(val)
    Int(val) -> int.to_string(val)
    Float(val) -> float.to_string(val)
    Bool(val) -> bool_to_string(val)
    Bytea(val) -> bytea_to_string(val)
    Date(val) -> date_to_string(val)
    Time(val) -> time_to_string(val)
    Datetime(date:, time:) -> datetime_to_string(date, time)
    Timestamp(val) -> timestamp_to_string(val)
    Interval(val) -> duration_to_string(val)
    Null -> "NULL"
  }
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

fn datetime_to_string(dt: calendar.Date, tod: calendar.TimeOfDay) -> String {
  date_to_string(dt) <> " " <> time_to_string(tod)
}

fn timestamp_to_string(ts: timestamp.Timestamp) -> String {
  timestamp.to_rfc3339(ts, calendar.utc_offset)
  |> single_quote
}

fn duration_to_string(dur: duration.Duration) -> String {
  duration.to_iso8601_string(dur)
  |> single_quote
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

pub fn text(value: String) -> Value {
  Text(value)
}

pub fn int(value: Int) -> Value {
  Int(value)
}

pub fn float(value: Float) -> Value {
  Float(value)
}

pub fn bool(val: Bool) -> Value {
  Bool(val)
}

pub const true = Bool(True)

pub const false = Bool(False)

pub fn null(_: Nil) -> Value {
  Null
}

pub fn bytea(value: BitArray) -> Value {
  Bytea(value)
}

pub fn timestamp(value: timestamp.Timestamp) -> Value {
  Timestamp(value)
}

pub fn date(value: calendar.Date) -> Value {
  Date(value)
}

pub fn time(value: calendar.TimeOfDay) -> Value {
  Time(value)
}

pub fn datetime(date: calendar.Date, time: calendar.TimeOfDay) -> Value {
  Datetime(date:, time:)
}

pub fn interval(dur: duration.Duration) -> Value {
  Interval(dur)
}

pub fn nullable(inner_type: fn(a) -> Value, value: Option(a)) -> Value {
  case value {
    Some(term) -> inner_type(term)
    None -> Null
  }
}

// Format

pub fn format() -> sql.Format(Value) {
  sql.format()
  |> sql.on_identifier(function.identity)
  |> sql.on_placeholder(handle_placeholder)
  |> sql.on_value(value_to_string)
}

fn handle_placeholder(_: Int) -> String {
  "?"
}
