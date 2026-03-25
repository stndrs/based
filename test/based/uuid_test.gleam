import based/uuid
import gleam/bit_array
import gleam/order
import gleam/string
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

pub fn from_string_empty_error_test() {
  assert Error(Nil) == uuid.from_string("")
}

pub fn from_string_too_short_error_test() {
  assert Error(Nil) == uuid.from_string("550e8400")
}

pub fn from_string_too_long_error_test() {
  assert Error(Nil) == uuid.from_string("550e8400-e29b-41d4-a716-4466554400001")
}

pub fn from_string_invalid_chars_error_test() {
  assert Error(Nil) == uuid.from_string("gggggggg-gggg-gggg-gggg-gggggggggggg")
}

pub fn from_string_not_a_uuid_error_test() {
  assert Error(Nil) == uuid.from_string("not-a-uuid")
}

pub fn v7_timestamp_ordering_test() {
  let earlier = timestamp.from_unix_seconds(1_000_000)
  let later = timestamp.from_unix_seconds(2_000_000)

  let uuid1 = uuid.from_timestamp(earlier)
  let uuid2 = uuid.from_timestamp(later)

  let s1 = uuid.to_string(uuid1)
  let s2 = uuid.to_string(uuid2)

  // v7 UUIDs embed millisecond timestamp in the first 48 bits,
  // so earlier timestamps produce lexicographically smaller strings
  assert string.compare(s1, s2) == order.Lt
}

pub fn from_bit_array_round_trip_test() {
  let id = uuid.v4()
  let bits = uuid.to_bit_array(id)
  let assert Ok(parsed) = uuid.from_bit_array(bits)

  assert uuid.to_string(id) == uuid.to_string(parsed)
}

pub fn from_bit_array_wrong_size_test() {
  assert Error(Nil) == uuid.from_bit_array(<<1, 2, 3>>)
}

pub fn from_bit_array_empty_test() {
  assert Error(Nil) == uuid.from_bit_array(<<>>)
}
