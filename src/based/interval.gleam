//// ISO 8601 duration/interval type with months, days, seconds, and
//// microseconds. Includes construction, addition, and string formatting.

import gleam/bool
import gleam/int
import gleam/string

/// An interval representing a duration with separate month, day, second,
/// and microsecond components. Each component is stored independently
/// without cross-unit conversion (e.g. 90 days is not converted to 3 months).
pub type Interval {
  Interval(months: Int, days: Int, seconds: Int, microseconds: Int)
}

/// Returns an Interval with the provided number of months.
pub fn months(months: Int) -> Interval {
  Interval(months:, days: 0, seconds: 0, microseconds: 0)
}

/// Returns an Interval with the provided number of days.
pub fn days(days: Int) -> Interval {
  Interval(months: 0, days:, seconds: 0, microseconds: 0)
}

/// Returns an Interval with the provided number of seconds.
pub fn seconds(seconds: Int) -> Interval {
  Interval(months: 0, days: 0, seconds:, microseconds: 0)
}

/// Returns an Interval with the provided number of microseconds.
pub fn microseconds(microseconds: Int) -> Interval {
  Interval(months: 0, days: 0, seconds: 0, microseconds:)
}

/// Returns an Interval with the summed values of each provided Interval.
pub fn add(left: Interval, right: Interval) -> Interval {
  let Interval(
    months: l_months,
    days: l_days,
    seconds: l_secs,
    microseconds: l_usecs,
  ) = left

  let Interval(
    months: r_months,
    days: r_days,
    seconds: r_secs,
    microseconds: r_usecs,
  ) = right

  Interval(
    months: l_months + r_months,
    days: l_days + r_days,
    seconds: l_secs + r_secs,
    microseconds: l_usecs + r_usecs,
  )
}

/// Converts an interval to an [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601#Durations)
/// formatted string. This function avoids converting the Interval's units,
/// except when the number of microseconds includes whole seconds. If more
/// than `1_000_000` microseconds are provided, whole seconds will be derived
/// from the microseconds value. The remaining microseconds will be appended
/// as a decimal in the formatted string.
///
/// ```gleam
/// Interval(months: 14, days: 0, seconds: 86430, microseconds: 0)
/// |> to_iso8601_string
/// // -> "P14MT86430S"
///
/// Interval(months: 0, days: 0, seconds: 10, microseconds: 1_200_000)
/// |> to_iso8601_string
/// // -> "PT11.2S"
/// ```
pub fn to_iso8601_string(interval: Interval) -> String {
  case interval {
    Interval(0, 0, 0, 0) -> "PT0S"
    Interval(months, days, secs, usecs) -> {
      let iso8601 =
        "P"
        |> append_to_iso8601_string(months, "M")
        |> append_to_iso8601_string(days, "D")

      let #(seconds, usecs) = to_seconds_and_microseconds(usecs)
      let seconds = seconds + secs

      case seconds, usecs {
        0, 0 -> iso8601
        _, 0 -> {
          iso8601
          |> string.append("T")
          |> append_to_iso8601_string(seconds, "S")
        }
        _, _ -> {
          iso8601
          |> string.append("T")
          |> string.append(int.to_string(seconds))
          |> string.append(".")
          |> string.append(microsecond_digits(usecs, 0, ""))
          |> string.append("S")
        }
      }
    }
  }
}

fn append_to_iso8601_string(iso8601: String, count: Int, unit: String) -> String {
  use <- bool.guard(count == 0, return: iso8601)

  iso8601 <> int.to_string(count) <> unit
}

fn to_seconds_and_microseconds(usecs: Int) -> #(Int, Int) {
  let seconds = usecs / 1_000_000
  let remainder = usecs - { seconds * 1_000_000 }

  case remainder < 0 {
    True -> #(seconds - 1, remainder + 1_000_000)
    False -> #(seconds, remainder)
  }
}

fn microsecond_digits(n: Int, position: Int, acc: String) -> String {
  case position {
    6 -> acc
    _ if acc == "" && n % 10 == 0 -> {
      microsecond_digits(n / 10, position + 1, acc)
    }
    _ -> {
      let acc = int.to_string(n % 10) <> acc
      microsecond_digits(n / 10, position + 1, acc)
    }
  }
}
