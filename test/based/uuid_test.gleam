import based/uuid
import gleam/bit_array
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

pub fn nil_to_string_test() {
  assert uuid.nil_string == uuid.to_string(uuid.nil)
}

pub fn to_bit_array_round_trip_test() {
  let id = uuid.v4()
  let bits = uuid.to_bit_array(id)

  // A UUID is 128 bits = 16 bytes
  assert 16 == bit_array.byte_size(bits)

  // Verify the string round-trip is consistent
  let str = uuid.to_string(id)
  let assert Ok(parsed) = uuid.from_string(str)
  assert bits == uuid.to_bit_array(parsed)
}
