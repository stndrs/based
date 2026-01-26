pub opaque type ValueMapper(v) {
  ValueMapper(handle_text: fn(String) -> v, handle_null: fn() -> v)
}

pub fn new() -> ValueMapper(v) {
  ValueMapper(
    handle_text: fn(_) { panic as "ValueMapper handle_text not configured" },
    handle_null: fn() { panic as "ValueMapper handle_null not configured" },
  )
}

pub fn on_text(
  mapper: ValueMapper(v),
  handle_text: fn(String) -> v,
) -> ValueMapper(v) {
  ValueMapper(..mapper, handle_text:)
}

pub fn on_null(mapper: ValueMapper(v), handle_null: fn() -> v) -> ValueMapper(v) {
  ValueMapper(..mapper, handle_null:)
}

pub fn from_text(text: String, mapper: ValueMapper(v)) -> v {
  mapper.handle_text(text)
}

pub fn null(mapper: ValueMapper(v)) -> v {
  mapper.handle_null()
}
