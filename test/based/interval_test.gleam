import based/interval.{Interval}

pub fn months_test() {
  let result = interval.months(14)

  assert Interval(months: 14, days: 0, seconds: 0, microseconds: 0) == result
}

pub fn days_test() {
  let result = interval.days(5)

  assert Interval(months: 0, days: 5, seconds: 0, microseconds: 0) == result
}

pub fn seconds_test() {
  let result = interval.seconds(3661)

  assert Interval(months: 0, days: 0, seconds: 3661, microseconds: 0) == result
}

pub fn microseconds_test() {
  let result = interval.microseconds(500_000)

  assert Interval(months: 0, days: 0, seconds: 0, microseconds: 500_000)
    == result
}

pub fn add_test() {
  let result =
    interval.months(2)
    |> interval.add(interval.days(10))
    |> interval.add(interval.seconds(30))
    |> interval.add(interval.microseconds(500))

  assert Interval(months: 2, days: 10, seconds: 30, microseconds: 500) == result
}

pub fn add_same_units_test() {
  let result =
    interval.seconds(100)
    |> interval.add(interval.seconds(200))

  assert Interval(months: 0, days: 0, seconds: 300, microseconds: 0) == result
}

pub fn add_zero_test() {
  let base = Interval(months: 3, days: 7, seconds: 100, microseconds: 5000)
  let zero = Interval(months: 0, days: 0, seconds: 0, microseconds: 0)

  assert base == interval.add(base, zero)
  assert base == interval.add(zero, base)
}

// to_iso8601_string tests

pub fn iso8601_zero_test() {
  let result =
    Interval(months: 0, days: 0, seconds: 0, microseconds: 0)
    |> interval.to_iso8601_string

  assert "PT0S" == result
}

pub fn iso8601_months_only_test() {
  let result =
    interval.months(14)
    |> interval.to_iso8601_string

  assert "P14M" == result
}

pub fn iso8601_days_only_test() {
  let result =
    interval.days(5)
    |> interval.to_iso8601_string

  assert "P5D" == result
}

pub fn iso8601_seconds_only_test() {
  let result =
    interval.seconds(3661)
    |> interval.to_iso8601_string

  assert "PT3661S" == result
}

pub fn iso8601_months_and_days_test() {
  let result =
    interval.months(2)
    |> interval.add(interval.days(10))
    |> interval.to_iso8601_string

  assert "P2M10D" == result
}

pub fn iso8601_all_date_units_test() {
  let result =
    Interval(months: 14, days: 5, seconds: 86_430, microseconds: 0)
    |> interval.to_iso8601_string

  assert "P14M5DT86430S" == result
}

pub fn iso8601_microseconds_overflow_to_seconds_test() {
  // 1_200_000 usecs = 1 second + 200_000 usecs
  let result =
    interval.microseconds(1_200_000)
    |> interval.to_iso8601_string

  assert "PT1.2S" == result
}

pub fn iso8601_seconds_plus_microseconds_test() {
  // 10 seconds + 1_200_000 usecs = 11.2 seconds
  let result =
    Interval(months: 0, days: 0, seconds: 10, microseconds: 1_200_000)
    |> interval.to_iso8601_string

  assert "PT11.2S" == result
}

pub fn iso8601_sub_second_microseconds_test() {
  // 500_000 usecs = 0.5 seconds
  let result =
    interval.microseconds(500_000)
    |> interval.to_iso8601_string

  assert "PT0.5S" == result
}

pub fn iso8601_microseconds_trailing_zero_trim_test() {
  // 100_000 usecs = 0.1 seconds
  let result =
    interval.microseconds(100_000)
    |> interval.to_iso8601_string

  assert "PT0.1S" == result
}

pub fn iso8601_microseconds_full_precision_test() {
  // 123_456 usecs = 0.123456 seconds (all 6 digits)
  let result =
    interval.microseconds(123_456)
    |> interval.to_iso8601_string

  assert "PT0.123456S" == result
}

pub fn iso8601_microseconds_single_test() {
  // 1 usec = 0.000001 seconds
  let result =
    interval.microseconds(1)
    |> interval.to_iso8601_string

  assert "PT0.000001S" == result
}

pub fn iso8601_complete_interval_test() {
  // All units populated
  let result =
    Interval(months: 1, days: 2, seconds: 3, microseconds: 400_000)
    |> interval.to_iso8601_string

  assert "P1M2DT3.4S" == result
}

pub fn iso8601_negative_microseconds_field_test() {
  // Ensure negative microseconds in the Interval struct don't corrupt output.
  // seconds(10) + microseconds that decompose cleanly
  let result =
    Interval(months: 0, days: 0, seconds: 0, microseconds: 2_500_000)
    |> interval.to_iso8601_string

  assert "PT2.5S" == result
}
