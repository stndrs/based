import based/uuid
import gleam/time/timestamp

pub fn v4_test() {
  let uuid_v4 = uuid.v4()

  assert uuid.V4 == uuid.version(uuid_v4)
}

pub fn v4_from_string_test() {
  let assert Ok(uuid_v4) =
    uuid.v4()
    |> uuid.to_string
    |> uuid.from_string

  assert uuid.V4 == uuid.version(uuid_v4)
}

pub fn v7_test() {
  let uuid_v7 = uuid.v7()

  assert uuid.V7 == uuid.version(uuid_v7)
}

pub fn v7_from_string_test() {
  let assert Ok(uuid_v7) =
    uuid.v7()
    |> uuid.to_string
    |> uuid.from_string

  assert uuid.V7 == uuid.version(uuid_v7)
}

pub fn from_timestamp_test() {
  let uuid_v7 =
    timestamp.system_time()
    |> uuid.from_timestamp

  assert uuid.V7 == uuid.version(uuid_v7)
}
