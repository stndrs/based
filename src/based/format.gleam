//// Database adapter packages can use this module to define how SQL for
//// their respective databases should be formatted.
////
//// For example, MySQL uses `?` placeholders while PostgreSQL uses
//// positional placeholders with a dollar sign and number like 
//// `$1, $2, $3, ...`.

import gleam/function

/// Format must be configured by adapter packages.
///
/// Example:
///
/// A PostgreSQL adapter might configure `Format` like this:
///
/// ```gleam
/// let fmt = format.new()
///   |> format.on_placeholder(fn(index) { "$" <> int.to_string(index) })
///   |> format.on_identifier(function.identifier)
///   |> format.on_value(value.to_string)
/// ```
/// A MariaDB adapter might configure `Format` like this:
///
/// ```gleam
/// let fmt = format.new()
///   |> format.on_placeholder(fn(_index) { "?" })
///   |> format.on_identifier(fn(ident) { "`" <> ident <> "`" })
///   |> format.on_value(value.to_string)
/// ```
///
pub opaque type Format(v) {
  Format(
    handle_identifier: fn(String) -> String,
    handle_placeholder: fn(Int) -> String,
    handle_value: fn(v) -> String,
  )
}

/// Returns a `Format(v)` record with handlers that does not apply any
/// formatting to identifiers, and returns `?` as placeholders. The value
/// handler's default behaviour is to panic since it handles a generic type.
pub fn new() -> Format(v) {
  Format(
    handle_identifier: function.identity,
    handle_placeholder: fn(_) { "?" },
    handle_value: fn(_) { panic as "based/format.Format not configured" },
  )
}

/// Apply the configured identifier format function to the provided identifier.
pub fn to_identifier(fmt: Format(v), identifier: String) -> String {
  fmt.handle_identifier(identifier)
}

/// Apply the configured value format function to the provided value.
pub fn to_string(fmt: Format(v), value: v) -> String {
  fmt.handle_value(value)
}

/// Apply the configured placeholder format function to the provided
/// placeholder index.
pub fn to_placeholder(fmt: Format(v), value: Int) -> String {
  fmt.handle_placeholder(value)
}

/// Sets the placeholder formatting function.
pub fn on_placeholder(
  fmt: Format(v),
  handle_placeholder: fn(Int) -> String,
) -> Format(v) {
  Format(..fmt, handle_placeholder:)
}

/// Set the identifier formatting function.
pub fn on_identifier(
  fmt: Format(v),
  handle_identifier: fn(String) -> String,
) -> Format(v) {
  Format(..fmt, handle_identifier:)
}

/// Set the value formatting function.
pub fn on_value(fmt: Format(v), handle_value: fn(v) -> String) -> Format(v) {
  Format(..fmt, handle_value:)
}
