import based/db
import based/interval.{Interval}
import based/repo
import based/sql/internal/fmt
import based/uuid
import gleam/int
import gleam/time/calendar
import gleam/time/timestamp

pub fn on_placeholder_test() {
  let repo =
    repo.new()
    |> repo.on_placeholder(fn(idx) { "$" <> int.to_string(idx) })

  // Test that placeholder function is set by using fmt
  assert "$1" == fmt.to_placeholder(repo.fmt, 1)
  assert "$42" == fmt.to_placeholder(repo.fmt, 42)
}

pub fn on_placeholder_question_mark_test() {
  let repo =
    repo.new()
    |> repo.on_placeholder(fn(_) { "?" })

  assert "?" == fmt.to_placeholder(repo.fmt, 1)
  assert "?" == fmt.to_placeholder(repo.fmt, 99)
}

pub fn on_identifier_test() {
  let repo =
    repo.new()
    |> repo.on_identifier(fn(ident) { "\"" <> ident <> "\"" })

  assert "\"users\"" == fmt.to_identifier(repo.fmt, "users")
  assert "\"column_name\"" == fmt.to_identifier(repo.fmt, "column_name")
}

pub fn on_identifier_backtick_test() {
  let repo =
    repo.new()
    |> repo.on_identifier(fn(ident) { "`" <> ident <> "`" })

  assert "`users`" == fmt.to_identifier(repo.fmt, "users")
  assert "`table`" == fmt.to_identifier(repo.fmt, "table")
}

pub fn on_value_test() {
  let repo =
    repo.new()
    |> repo.on_value(int.to_string)

  assert "42" == fmt.to_string(repo.fmt, 42)
  assert "123" == fmt.to_string(repo.fmt, 123)
}

pub fn chained_configuration_test() {
  let repo =
    repo.new()
    |> repo.on_placeholder(fn(idx) { "$" <> int.to_string(idx) })
    |> repo.on_identifier(fn(ident) { "\"" <> ident <> "\"" })
    |> repo.on_value(int.to_string)

  assert "$1" == fmt.to_placeholder(repo.fmt, 1)
  assert "\"users\"" == fmt.to_identifier(repo.fmt, "users")
  assert "42" == fmt.to_string(repo.fmt, 42)
}

// value_to_string tests (exercised via repo.default())

fn default_to_string(value: db.Value) -> String {
  let repo = repo.default()

  fmt.to_string(repo.fmt, value)
}

pub fn value_null_test() {
  assert "NULL" == default_to_string(db.Null)
}

pub fn value_bool_true_test() {
  assert "TRUE" == default_to_string(db.Bool(True))
}

pub fn value_bool_false_test() {
  assert "FALSE" == default_to_string(db.Bool(False))
}

pub fn value_int_test() {
  assert "42" == default_to_string(db.Int(42))
}

pub fn value_int_negative_test() {
  assert "-7" == default_to_string(db.Int(-7))
}

pub fn value_float_test() {
  assert "3.14" == default_to_string(db.Float(3.14))
}

pub fn value_text_test() {
  assert "'hello'" == default_to_string(db.Text("hello"))
}

pub fn value_text_with_single_quote_test() {
  assert "'it\\'s'" == default_to_string(db.Text("it's"))
}

pub fn value_uuid_test() {
  let assert Ok(id) = uuid.from_string("550e8400-e29b-11d4-a716-446655440000")

  assert "550e8400-e29b-11d4-a716-446655440000"
    == default_to_string(db.Uuid(id))
}

pub fn value_bytea_test() {
  assert "'\\xDEADBEEF'"
    == default_to_string(db.Bytea(<<0xDE, 0xAD, 0xBE, 0xEF>>))
}

pub fn value_bytea_empty_test() {
  assert "'\\x'" == default_to_string(db.Bytea(<<>>))
}

pub fn value_timestamp_test() {
  // Unix epoch: 2024-01-15 11:30:00 UTC = 1705318200
  let ts = timestamp.from_unix_seconds(1_705_318_200)
  let result = default_to_string(db.Timestamp(ts))

  assert "'2024-01-15T11:30:00Z'" == result
}

pub fn value_timestamptz_positive_offset_test() {
  // 2024-01-15 11:30:00 UTC with +5h offset
  let ts = timestamp.from_unix_seconds(1_705_318_200)
  let offset = db.Offset(hours: 5, minutes: 0)
  let result = default_to_string(db.Timestamptz(ts, offset))

  // offset_to_duration with positive hours uses sign = -1,
  // so it subtracts 5 hours: 11:30 - 5:00 = 06:30 UTC
  assert "'2024-01-15T06:30:00Z'" == result
}

pub fn value_timestamptz_negative_offset_test() {
  // 2024-01-15 11:30:00 UTC with -5h offset
  let ts = timestamp.from_unix_seconds(1_705_318_200)
  let offset = db.Offset(hours: -5, minutes: 0)
  let result = default_to_string(db.Timestamptz(ts, offset))

  // offset_to_duration with negative hours uses sign = 1,
  // so it adds 5 hours: 11:30 + 5:00 = 16:30 UTC
  assert "'2024-01-15T16:30:00Z'" == result
}

pub fn value_interval_test() {
  let val = Interval(months: 2, days: 10, seconds: 30, microseconds: 500_000)

  assert "P2M10DT30.5S" == default_to_string(db.Interval(val))
}

pub fn value_array_empty_test() {
  assert "[]" == default_to_string(db.Array([]))
}

pub fn value_array_single_test() {
  assert "[42]" == default_to_string(db.Array([db.Int(42)]))
}

pub fn value_array_multiple_test() {
  assert "[1, 2, 3]"
    == default_to_string(db.Array([db.Int(1), db.Int(2), db.Int(3)]))
}

pub fn value_array_mixed_types_test() {
  assert "['hello', 42, TRUE]"
    == default_to_string(
      db.Array([db.Text("hello"), db.Int(42), db.Bool(True)]),
    )
}

pub fn value_array_nested_test() {
  assert "[[1, 2], [3, 4]]"
    == default_to_string(
      db.Array([
        db.Array([db.Int(1), db.Int(2)]),
        db.Array([db.Int(3), db.Int(4)]),
      ]),
    )
}

pub fn value_date_test() {
  let date = calendar.Date(year: 2024, month: calendar.March, day: 5)

  assert "'2024-03-05'" == default_to_string(db.Date(date))
}

pub fn value_time_test() {
  let time =
    calendar.TimeOfDay(hours: 8, minutes: 5, seconds: 3, nanoseconds: 0)

  assert "'08:05:03'" == default_to_string(db.Time(time))
}

pub fn value_time_with_milliseconds_test() {
  let time =
    calendar.TimeOfDay(
      hours: 14,
      minutes: 30,
      seconds: 45,
      nanoseconds: 123_000_000,
    )

  assert "'14:30:45.123'" == default_to_string(db.Time(time))
}

pub fn value_datetime_test() {
  let date = calendar.Date(year: 2024, month: calendar.January, day: 15)
  let time =
    calendar.TimeOfDay(hours: 9, minutes: 5, seconds: 3, nanoseconds: 0)

  assert "'2024-01-15 09:05:03'" == default_to_string(db.Datetime(date, time))
}
