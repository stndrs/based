import based
import based/batch
import based/sql
import based/value.{type Value}
import gleam/dynamic
import gleam/dynamic/decode
import gleam/int

fn user_decoder() -> decode.Decoder(#(Int, String)) {
  use id <- decode.field(0, decode.int)
  use name <- decode.field(1, decode.string)

  decode.success(#(id, name))
}

fn sql_adapter() -> sql.Adapter(Value) {
  value.adapter()
  |> sql.on_placeholder(fn(idx) { "$" <> int.to_string(idx) })
}

type Conn {
  Conn
}

pub fn batch_empty_test() {
  let database =
    based.driver(
      Conn,
      on_query: fn(_, _) { Ok(based.Queried(0, [], [])) },
      on_execute: fn(_, _) { Ok(0) },
      on_batch: fn(_, _) { Ok([]) },
    )
    |> based.new(sql_adapter())

  let query1 =
    sql.query("SELECT * FROM users WHERE id=$1;")
    |> sql.params([value.int(1)])

  let batch = {
    use decoded <- batch.list(query1, decode.dynamic)

    batch.ready(decoded)
  }

  assert Ok([]) == batch.run(batch, database)
}

pub fn batch_test() {
  let rows1 = [dynamic.array([dynamic.int(1), dynamic.string("Steve")])]
  let rows2 = [dynamic.array([dynamic.int(2), dynamic.string("Billiam")])]

  let returning =
    Ok([
      based.Queried(count: 1, fields: ["id", "name"], rows: rows1),
      based.Queried(count: 1, fields: ["id", "name"], rows: rows2),
    ])

  let database =
    based.driver(
      Conn,
      on_query: fn(_, _) { Ok(based.Queried(0, [], [])) },
      on_execute: fn(_, _) { Ok(0) },
      on_batch: fn(_, _) { returning },
    )
    |> based.new(sql_adapter())

  let decoder = {
    use id <- decode.field(0, decode.int)
    decode.success(#(id))
  }

  let query1 =
    sql.query("SELECT * FROM users WHERE id=$1;")
    |> sql.params([value.int(1)])

  let query2 =
    sql.query("SELECT * FROM users WHERE id=$1;")
    |> sql.params([value.int(2)])

  let batch = {
    use result1 <- batch.list(query1, decoder)
    use result2 <- batch.list(query2, decoder)

    batch.ready(#(result1, result2))
  }

  let assert Ok(#([#(1)], [#(2)])) = batch.run(batch, database)
}

pub fn batch_different_test() {
  let rows1 = [dynamic.array([dynamic.int(1), dynamic.string("Steve")])]
  let rows2 = [dynamic.array([dynamic.int(2), dynamic.string("Billiam")])]

  let returning =
    Ok([
      based.Queried(count: 1, fields: ["id", "name"], rows: rows1),
      based.Queried(count: 1, fields: ["id", "name"], rows: rows2),
    ])

  let database =
    based.driver(
      Conn,
      on_query: fn(_, _) { Ok(based.Queried(0, [], [])) },
      on_execute: fn(_, _) { Ok(0) },
      on_batch: fn(_, _) { returning },
    )
    |> based.new(sql_adapter())

  let decoder1 = {
    use id <- decode.field(0, decode.int)
    decode.success(#(id))
  }

  let decoder2 = {
    use name <- decode.field(1, decode.string)
    decode.success(#(name))
  }

  let query1 =
    sql.query("SELECT * FROM users WHERE id=$1;")
    |> sql.params([value.int(1)])

  let query2 =
    sql.query("SELECT * FROM users WHERE id=$1;")
    |> sql.params([value.int(2)])

  let batch = {
    use result1 <- batch.list(query1, decoder1)
    use result2 <- batch.list(query2, decoder2)

    batch.ready(#(result1, result2))
  }

  let assert Ok(#([#(1)], [#("Billiam")])) = batch.run(batch, database)
}

pub fn batch_add_one_test() {
  let rows1 = [dynamic.array([dynamic.int(1), dynamic.string("Steve")])]
  let rows2 = [dynamic.array([dynamic.int(2), dynamic.string("Billiam")])]

  let returning =
    Ok([
      based.Queried(count: 1, fields: ["id", "name"], rows: rows1),
      based.Queried(count: 1, fields: ["id", "name"], rows: rows2),
    ])

  let database =
    based.driver(
      Conn,
      on_query: fn(_, _) { Ok(based.Queried(0, [], [])) },
      on_execute: fn(_, _) { Ok(0) },
      on_batch: fn(_, _) { returning },
    )
    |> based.new(sql_adapter())

  let query1 =
    sql.query("SELECT * FROM users WHERE id=$1;")
    |> sql.params([value.int(1)])

  let query2 =
    sql.query("SELECT * FROM users WHERE id=$1;")
    |> sql.params([value.int(2)])

  let batch = {
    use users <- batch.list(query1, user_decoder())
    use admin <- batch.one(query2, user_decoder())

    batch.ready(#(users, admin))
  }

  let assert Ok(#([#(1, "Steve")], #(2, "Billiam"))) =
    batch.run(batch, database)
}

pub fn batch_add_one_not_found_test() {
  let rows1 = [dynamic.array([dynamic.int(1), dynamic.string("Steve")])]

  let returning =
    Ok([
      based.Queried(count: 1, fields: ["id", "name"], rows: rows1),
      based.Queried(count: 0, fields: ["id", "name"], rows: []),
    ])

  let database =
    based.driver(
      Conn,
      on_query: fn(_, _) { Ok(based.Queried(0, [], [])) },
      on_execute: fn(_, _) { Ok(0) },
      on_batch: fn(_, _) { returning },
    )
    |> based.new(sql_adapter())

  let query1 =
    sql.query("SELECT * FROM users WHERE id=$1;")
    |> sql.params([value.int(1)])

  let query2 =
    sql.query("SELECT * FROM users WHERE id=$1;")
    |> sql.params([value.int(2)])

  let batch = {
    use users <- batch.list(query1, user_decoder())
    use admin <- batch.one(query2, user_decoder())

    batch.ready(#(users, admin))
  }

  let assert Error(batch.BatchError("Empty results", based.NotFound)) =
    batch.run(batch, database)
}

pub fn batch_add_one_only_test() {
  let rows = [dynamic.array([dynamic.int(1), dynamic.string("Steve")])]

  let returning =
    Ok([
      based.Queried(count: 1, fields: ["id", "name"], rows:),
    ])

  let database =
    based.driver(
      Conn,
      on_query: fn(_, _) { Ok(based.Queried(0, [], [])) },
      on_execute: fn(_, _) { Ok(0) },
      on_batch: fn(_, _) { returning },
    )
    |> based.new(sql_adapter())

  let query =
    sql.query("SELECT * FROM users WHERE id=$1;")
    |> sql.params([value.int(1)])

  let batch = {
    use user <- batch.one(query, user_decoder())

    batch.ready(user)
  }

  let assert Ok(#(1, "Steve")) = batch.run(batch, database)
}
