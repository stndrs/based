pub opaque type Format(v) {
  Format(
    handle_identifier: fn(String) -> String,
    handle_placeholder: fn(Int) -> String,
    handle_string: fn(v) -> String,
  )
}

pub fn new() -> Format(v) {
  Format(
    handle_identifier: fn(_) { "" },
    handle_placeholder: fn(_) { "" },
    handle_string: fn(_) { "" },
  )
}

pub fn to_identifier(fmt: Format(v), value: String) -> String {
  fmt.handle_identifier(value)
}

pub fn to_string(fmt: Format(v), value: v) -> String {
  fmt.handle_string(value)
}

pub fn to_placeholder(fmt: Format(v), value: Int) -> String {
  fmt.handle_placeholder(value)
}

pub fn on_placeholder(
  fmt: Format(v),
  handle_placeholder: fn(Int) -> String,
) -> Format(v) {
  Format(..fmt, handle_placeholder:)
}

pub fn on_identifier(
  fmt: Format(v),
  handle_identifier: fn(String) -> String,
) -> Format(v) {
  Format(..fmt, handle_identifier:)
}

pub fn on_string(fmt: Format(v), handle_string: fn(v) -> String) -> Format(v) {
  Format(..fmt, handle_string:)
}
