import based
import based/sql
import gleam/dynamic/decode.{type Decoder}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result

pub type BatchError {
  BatchError(message: String, error: based.BasedError)
  DecodeError(message: String, errors: List(decode.DecodeError))
}

pub opaque type Batch(t, v) {
  Batch(
    queries: List(sql.Query(v)),
    decode: fn(List(based.Queried)) -> Result(t, BatchError),
  )
}

/// Terminate a batch chain with a value. This is used as the final step
/// when building a batch with `add`.
pub fn ready(value: t) -> Batch(t, v) {
  Batch(queries: [], decode: fn(_) { Ok(value) })
}

/// Add a query to a batch. The query results will be decoded using the
/// provided decoder, and the decoded rows are passed to the `next`
/// continuation function.
pub fn list(
  q: sql.Query(v),
  decoder: Decoder(a),
  next: fn(List(a)) -> Batch(final, v),
) -> Batch(final, v) {
  let next_batch = next([])

  let queries = list.prepend(next_batch.queries, q)

  let decode = fn(results: List(based.Queried)) {
    case results {
      [] -> next_batch.decode([])
      [first, ..rest] -> {
        first.rows
        |> list.try_map(decode.run(_, decoder))
        |> result.map_error(DecodeError("Failed to decode list", _))
        |> result.try(fn(rows) {
          let next_batch = next(rows)

          next_batch.decode(rest)
        })
      }
    }
  }

  Batch(queries:, decode:)
}

// const empty_result = BatchError(message: "Empty results", error: based.NotFound)

// Add a query to a batch that expects zero or one row. The query result
// will be decoded using the provided decoder, and the decoded value is
// passed to the `next` continuation function as `Some(a)`. If the query
// returns zero rows, `None` is passed instead.
// pub fn one(
//   q: sql.Query(v),
//   decoder: Decoder(a),
//   next: fn(a) -> Batch(final, v),
// ) -> Batch(final, v) {
//   // Can we pass `Dynamic` to `next`?
//   let next_batch = next(todo)
// 
//   let queries = list.prepend(next_batch.queries, q)
// 
//   let decode = fn(results: List(based.Queried)) {
//     case results {
//       [] -> Error(empty_result)
//       [first, ..rest] -> {
//         case first.rows {
//           [] -> Error(empty_result)
//           [row, ..] -> {
//             decode.run(row, decoder)
//             |> result.map_error(DecodeError("Failed to decode row", _))
//             |> result.try(fn(value) {
//               let next_batch = next(value)
// 
//               next_batch.decode(rest)
//             })
//           }
//         }
//       }
//     }
//   }
// 
//   Batch(queries:, decode:)
// }

pub fn optional(
  q: sql.Query(v),
  decoder: Decoder(a),
  next: fn(Option(a)) -> Batch(final, v),
) -> Batch(final, v) {
  let next_batch = next(None)

  let queries = list.prepend(next_batch.queries, q)

  let decode = fn(results: List(based.Queried)) {
    case results {
      [] -> next_batch.decode([])
      [first, ..rest] -> {
        case first.rows {
          [] -> next_batch.decode(rest)
          [row, ..] -> {
            decode.run(row, decoder)
            |> result.map_error(DecodeError("Failed to decode optional row", _))
            |> result.try(fn(value) {
              let next_batch = next(Some(value))

              next_batch.decode(rest)
            })
          }
        }
      }
    }
  }

  Batch(queries:, decode:)
}

pub fn run(batch: Batch(a, v), db: based.Db(v, conn)) -> Result(a, BatchError) {
  batch.queries
  |> based.batch(db)
  |> result.map_error(BatchError("Failed to perform batch query", _))
  |> result.try(fn(queried) { batch.decode(queried) })
}
