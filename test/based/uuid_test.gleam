import based/uuid
import gleam/time/timestamp

pub fn v4_test() {
  let uuid_v4 = uuid.v4()

  assert Ok(uuid.V4) == uuid.version(uuid_v4)
}

pub fn v4_from_string_test() {
  let assert Ok(uuid_v4) =
    uuid.v4()
    |> uuid.to_string
    |> uuid.from_string

  assert Ok(uuid.V4) == uuid.version(uuid_v4)
}

pub fn v7_test() {
  let uuid_v7 = uuid.v7()

  assert Ok(uuid.V7) == uuid.version(uuid_v7)
}

pub fn v7_from_string_test() {
  let assert Ok(uuid_v7) =
    uuid.v7()
    |> uuid.to_string
    |> uuid.from_string

  assert Ok(uuid.V7) == uuid.version(uuid_v7)
}

pub fn from_timestamp_test() {
  let uuid_v7 =
    timestamp.system_time()
    |> uuid.from_timestamp

  assert Ok(uuid.V7) == uuid.version(uuid_v7)
}

pub fn version_unknown_test() {
  // UUID with version bits set to 1 (v1 UUID format)
  let assert Ok(uuid_v1) =
    uuid.from_string("550e8400-e29b-11d4-a716-446655440000")

  assert Error(Nil) == uuid.version(uuid_v1)
}

pub fn version_nil_test() {
  assert Error(Nil) == uuid.version(uuid.nil)
}
