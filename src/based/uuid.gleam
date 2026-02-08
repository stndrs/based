import gleam/crypto
import gleam/int
import gleam/result
import gleam/string
import gleam/time/timestamp

pub opaque type Uuid {
  Uuid(uuid: BitArray)
}

pub type Version {
  V4
  V7
}

const rfc_variant = 2

const v4_version = 4

const v7_version = 7

pub fn v4() -> Uuid {
  let assert <<a:size(48), _:size(4), b:size(12), _:size(2), c:size(62)>> =
    crypto.strong_random_bytes(16)

  let uuid = <<
    a:size(48), v4_version:size(4), b:size(12), rfc_variant:size(2), c:size(62),
  >>

  Uuid(uuid: uuid)
}

pub fn to_string(uuid: Uuid) -> String {
  do_to_string(uuid.uuid, 0, "", "-")
}

pub fn to_bit_array(uuid: Uuid) -> BitArray {
  uuid.uuid
}

fn do_to_string(
  ints: BitArray,
  position: Int,
  acc: String,
  separator: String,
) -> String {
  case position {
    8 | 13 | 18 | 23 ->
      do_to_string(ints, position + 1, acc <> separator, separator)
    _ ->
      case ints {
        <<i:size(4), rest:bits>> -> {
          let string = int.to_base16(i) |> string.lowercase
          do_to_string(rest, position + 1, acc <> string, separator)
        }
        _ -> acc
      }
  }
}

pub fn v7() -> Uuid {
  timestamp.system_time() |> from_timestamp
}

pub fn from_timestamp(ts: timestamp.Timestamp) -> Uuid {
  let assert <<a:size(12), b:size(62), _:size(6)>> =
    crypto.strong_random_bytes(10)

  let #(sec, ns) = timestamp.to_unix_seconds_and_nanoseconds(ts)

  let ts = {
    sec * 1000 + ns / 1_000_000
  }

  let uuid = <<ts:48, v7_version:4, a:12, rfc_variant:2, b:62>>

  Uuid(uuid: uuid)
}

pub fn from_string(value: String) -> Result(Uuid, Nil) {
  use uuid <- result.map(do_from_string(value, 0, <<>>))

  Uuid(uuid:)
}

fn do_from_string(
  str: String,
  index: Int,
  acc: BitArray,
) -> Result(BitArray, Nil) {
  case string.pop_grapheme(str) {
    Error(Nil) if index == 32 -> Ok(acc)
    Ok(#("-", rest)) if index < 32 -> do_from_string(rest, index, acc)
    Ok(#(c, rest)) if index < 32 ->
      case hex_to_int(c) {
        Ok(i) -> do_from_string(rest, index + 1, <<acc:bits, i:size(4)>>)
        Error(_) -> Error(Nil)
      }
    _ -> Error(Nil)
  }
}

fn hex_to_int(c: String) -> Result(Int, Nil) {
  let i = case c {
    "0" -> 0
    "1" -> 1
    "2" -> 2
    "3" -> 3
    "4" -> 4
    "5" -> 5
    "6" -> 6
    "7" -> 7
    "8" -> 8
    "9" -> 9
    "a" | "A" -> 10
    "b" | "B" -> 11
    "c" | "C" -> 12
    "d" | "D" -> 13
    "e" | "E" -> 14
    "f" | "F" -> 15
    _ -> 16
  }
  case i {
    16 -> Error(Nil)
    x -> Ok(x)
  }
}

pub const nil: Uuid = Uuid(uuid: <<0:128>>)

pub const nil_string: String = "00000000-0000-0000-0000-000000000000"

pub fn version(uuid: Uuid) -> Version {
  let assert <<_:48, ver:4, _:76>> = uuid.uuid

  case ver {
    4 -> V4
    7 -> V7
    _ -> panic as "unexpected Uuid version"
  }
}
