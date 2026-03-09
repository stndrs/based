import based/db
import based/repo
import based/sql
import based/sql/insert
import based/uuid
import gleam/list
import gleeunit/should

pub fn basic_insert_test() {
  let expected = "INSERT INTO users (name, email) VALUES (?, ?)"
  let users = sql.table("users")

  let query =
    repo.default()
    |> insert.into(users)
    |> insert.values([
      {
        use <- insert.value("name", db.text("John"))
        insert.final("email", db.text("john@example.com"))
      },
    ])
    |> insert.to_query

  query.sql |> should.equal(expected)
  query.values
  |> should.equal([db.text("John"), db.text("john@example.com")])
}

pub fn insert_multiple_columns_test() {
  let expected =
    "INSERT INTO users (name, email, age, active) VALUES (?, ?, ?, ?)"
  let users = sql.table("users")

  let query =
    repo.default()
    |> insert.into(users)
    |> insert.values([
      {
        use <- insert.value("name", db.text("John"))
        use <- insert.value("email", db.text("john@example.com"))
        use <- insert.value("age", db.int(30))
        insert.final("active", db.bool(True))
      },
    ])
    |> insert.to_query

  query.sql |> should.equal(expected)
  query.values
  |> should.equal([
    db.text("John"),
    db.text("john@example.com"),
    db.int(30),
    db.true,
  ])
}

pub fn insert_returning_test() {
  let expected = "INSERT INTO users (name) VALUES (?) RETURNING id, name"
  let users = sql.table("users")

  let query =
    repo.default()
    |> insert.into(users)
    |> insert.values([insert.final("name", db.text("John"))])
    |> insert.returning([sql.column("id"), sql.column("name")])
    |> insert.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([db.text("John")])
}

pub fn insert_to_string_test() {
  let expected =
    "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')"
  let users = sql.table("users")

  let query =
    repo.default()
    |> insert.into(users)
    |> insert.values([
      {
        use <- insert.value("name", db.text("John"))
        insert.final("email", db.text("john@example.com"))
      },
    ])

  insert.to_string(query) |> should.equal(expected)
}

pub fn insert_multiple_rows_test() {
  let expected = "INSERT INTO users (name, email) VALUES (?, ?), (?, ?)"
  let users = sql.table("users")

  let values =
    [
      #("John", "john@example.com"),
      #("Jane", "jane@example.com"),
    ]
    |> list.map(fn(user) {
      use <- insert.value("name", db.text(user.0))
      insert.final("email", db.text(user.1))
    })

  let query =
    repo.default()
    |> insert.into(users)
    |> insert.values(values)
    |> insert.to_query

  query.sql |> should.equal(expected)
  query.values
  |> should.equal([
    db.text("John"),
    db.text("john@example.com"),
    db.text("Jane"),
    db.text("jane@example.com"),
  ])
}

pub fn insert_with_null_test() {
  let expected = "INSERT INTO users (name, middle_name) VALUES (?, ?)"
  let users = sql.table("users")

  let query =
    repo.default()
    |> insert.into(users)
    |> insert.values([
      {
        use <- insert.value("name", db.text("John"))
        insert.final("middle_name", db.null)
      },
    ])
    |> insert.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([db.text("John"), db.null])
}

pub fn insert_with_different_value_types_test() {
  let expected =
    "INSERT INTO products (id, price, is_active, description) VALUES (?, ?, ?, ?)"
  let products = sql.table("products")

  let query =
    repo.default()
    |> insert.into(products)
    |> insert.values([
      {
        use <- insert.value("id", db.int(123))
        use <- insert.value("price", db.float(19.99))
        use <- insert.value("is_active", db.true)
        insert.final("description", db.null)
      },
    ])
    |> insert.to_query

  query.sql
  |> should.equal(expected)

  query.values
  |> should.equal([
    db.int(123),
    db.float(19.99),
    db.true,
    db.null,
  ])
}

pub fn insert_on_conflict_test() {
  let expected =
    "INSERT INTO counts (id, identifier, quantity) VALUES (?, ?, ?) ON CONFLICT (identifier) DO UPDATE SET quantity = excluded.quantity"

  let counts = sql.table("counts")

  let identifier = uuid.v4()

  let query =
    repo.default()
    |> insert.into(counts)
    |> insert.values([
      {
        use <- insert.value("id", db.int(123))
        use <- insert.value("identifier", db.uuid(identifier))
        insert.final("quantity", db.int(10))
      },
    ])
    |> insert.on_conflict(
      "identifier",
      do: insert.update([insert.set("quantity", "excluded.quantity")]),
      where: [],
    )
    |> insert.to_query

  assert expected == query.sql
  assert [db.int(123), db.uuid(identifier), db.int(10)] == query.values
}

pub fn insert_on_conflict_do_nothing_test() {
  let expected =
    "INSERT INTO counts (id, identifier, quantity) VALUES (?, ?, ?) ON CONFLICT (identifier) DO NOTHING"

  let counts = sql.table("counts")

  let identifier = uuid.v4()

  let query =
    repo.default()
    |> insert.into(counts)
    |> insert.values([
      {
        use <- insert.value("id", db.int(123))
        use <- insert.value("identifier", db.uuid(identifier))
        insert.final("quantity", db.int(10))
      },
    ])
    |> insert.on_conflict("identifier", do: insert.nothing, where: [])
    |> insert.to_query

  assert expected == query.sql
  assert [db.int(123), db.uuid(identifier), db.int(10)] == query.values
}

pub fn insert_on_conflict_returning_test() {
  let expected =
    "INSERT INTO counts (id, quantity) VALUES (?, ?) ON CONFLICT (id) DO NOTHING RETURNING id"

  let counts = sql.table("counts")

  let query =
    repo.default()
    |> insert.into(counts)
    |> insert.values([
      {
        use <- insert.value("id", db.int(123))
        insert.final("quantity", db.int(10))
      },
    ])
    |> insert.on_conflict("id", do: insert.nothing, where: [])
    |> insert.returning([sql.column("id")])
    |> insert.to_query

  assert expected == query.sql
}

pub fn insert_on_conflict_where_test() {
  let expected =
    "INSERT INTO counts (id, quantity) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET quantity = excluded.quantity WHERE quantity > ?"

  let counts = sql.table("counts")

  let query =
    repo.default()
    |> insert.into(counts)
    |> insert.values([
      {
        use <- insert.value("id", db.int(123))
        insert.final("quantity", db.int(10))
      },
    ])
    |> insert.on_conflict(
      "id",
      do: insert.update([insert.set("quantity", "excluded.quantity")]),
      where: [sql.gt(sql.column("quantity"), db.int(5), of: sql.val)],
    )
    |> insert.to_query

  query.sql |> should.equal(expected)
  query.values |> should.equal([db.int(123), db.int(10), db.int(5)])
}

pub fn insert_on_conflict_where_to_string_test() {
  let expected =
    "INSERT INTO counts (id, quantity) VALUES (123, 10) ON CONFLICT (id) DO UPDATE SET quantity = excluded.quantity WHERE quantity > 5"

  let counts = sql.table("counts")

  let result =
    repo.default()
    |> insert.into(counts)
    |> insert.values([
      {
        use <- insert.value("id", db.int(123))
        insert.final("quantity", db.int(10))
      },
    ])
    |> insert.on_conflict(
      "id",
      do: insert.update([insert.set("quantity", "excluded.quantity")]),
      where: [sql.gt(sql.column("quantity"), db.int(5), of: sql.val)],
    )
    |> insert.to_string

  result |> should.equal(expected)
}

pub fn insert_on_conflict_do_nothing_to_string_test() {
  let expected =
    "INSERT INTO counts (id, quantity) VALUES (123, 10) ON CONFLICT (id) DO NOTHING"

  let counts = sql.table("counts")

  let result =
    repo.default()
    |> insert.into(counts)
    |> insert.values([
      {
        use <- insert.value("id", db.int(123))
        insert.final("quantity", db.int(10))
      },
    ])
    |> insert.on_conflict("id", do: insert.nothing, where: [])
    |> insert.to_string

  result |> should.equal(expected)
}

pub fn insert_with_explicit_columns_test() {
  let expected = "INSERT INTO users (id, name, email) VALUES (?, ?, ?)"
  let users = sql.table("users")

  let query =
    repo.default()
    |> insert.into(users)
    |> insert.columns(["id", "name", "email"])
    |> insert.values([
      {
        use <- insert.value("id", db.int(1))
        use <- insert.value("name", db.text("John"))
        insert.final("email", db.text("john@example.com"))
      },
    ])
    |> insert.to_query

  query.sql |> should.equal(expected)
  query.values
  |> should.equal([db.int(1), db.text("John"), db.text("john@example.com")])
}
