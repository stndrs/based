pub opaque type Mapper(v) {
  Mapper(
    handle_int: fn(Int) -> v,
    handle_text: fn(String) -> v,
    handle_null: fn() -> v,
  )
}

pub fn mapper() -> Mapper(v) {
  Mapper(
    handle_int: fn(_) { panic as "Mapper handle_int not configured" },
    handle_text: fn(_) { panic as "Mapper handle_text not configured" },
    handle_null: fn() { panic as "Mapper handle_null not configured" },
  )
}

pub fn on_int(mapper: Mapper(v), handle_int: fn(Int) -> v) -> Mapper(v) {
  Mapper(..mapper, handle_int:)
}

pub fn on_text(mapper: Mapper(v), handle_text: fn(String) -> v) -> Mapper(v) {
  Mapper(..mapper, handle_text:)
}

pub fn on_null(mapper: Mapper(v), handle_null: fn() -> v) -> Mapper(v) {
  Mapper(..mapper, handle_null:)
}

pub fn from_int(int: Int, mapper: Mapper(v)) -> v {
  mapper.handle_int(int)
}

pub fn from_text(text: String, mapper: Mapper(v)) -> v {
  mapper.handle_text(text)
}

pub fn null(mapper: Mapper(v)) -> v {
  mapper.handle_null()
}
