//// A type-safe SQL query builder for Gleam.
////
//// Queries are built using a pipeline API that starts with a table source and
//// chains modifiers to construct SELECT, INSERT, UPDATE, and DELETE statements.
//// Phantom types ensure that only valid modifiers can be applied to each query
//// kind at compile time.
////
//// ## Quick example
////
//// ```gleam
//// import based/sql
////
//// let adapter = sql.adapter()
////
//// let query =
////   sql.from(sql.table("users"))
////   |> sql.select([sql.col("name"), sql.col("email")])
////   |> sql.where(sql.eq(sql.col("active"), sql.true, of: sql.value))
////   |> sql.order_by(sql.col("name"), sql.asc)
////   |> sql.limit(10)
////   |> sql.to_query(adapter)
////
//// query.sql
//// // -> "SELECT name, email FROM users WHERE active = $1 ORDER BY name ASC LIMIT 10"
////
//// query.values
//// // -> [sql.Boolean(True)]
//// ```
////
//// ## Custom adapters
////
//// Use `adapter()` with builder functions to configure how queries are rendered.
//// This controls placeholder style, identifier quoting, and value type mapping.
////
//// ```gleam
//// let mysql_adapter =
////   sql.adapter()
////   |> sql.on_null(with: fn() { sql.null })
////   |> sql.on_int(with: fn(i) { sql.int(i) })
////   |> sql.on_text(with: fn(s) { sql.text(s) })
////   |> sql.on_placeholder(with: fn(_) { "?" })
////   |> sql.on_value(with: my_value_to_string)
////   |> sql.on_identifier(with: fn(name) { "`" <> name <> "`" })
//// ```
////
//// ## Phantom types
////
//// The `QueryBuilder(kind, v)` type uses phantom types (`Select`, `Insert`,
//// `Update`, `Delete`, `From`) to restrict which modifier functions can be
//// called. For example, `join` only accepts `QueryBuilder(Select, v)`, and
//// `set` only accepts `QueryBuilder(Update, v)`. This prevents invalid SQL
//// from being constructed at compile time.
////

import based/internal/fmt as sqlfmt
import based/interval
import based/uuid
import gleam/bit_array
import gleam/float
import gleam/function
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/string
import gleam/time/calendar
import gleam/time/duration
import gleam/time/timestamp

/// The result of rendering a query builder. Contains a parameterized SQL string
/// and an ordered list of values corresponding to the placeholders.
///
/// ```gleam
/// let q = sql.from(sql.table("users"))
///   |> sql.select([sql.col("name")])
///   |> sql.to_query(adapter)
///
/// q.sql     // -> "SELECT name FROM users"
/// q.values  // -> []
/// ```
pub type Query(v) {
  Query(sql: String, values: List(v))
}

pub fn query(sql: String) -> Query(v) {
  Query(sql:, values: [])
}

pub fn params(query: Query(v), values: List(v)) -> Query(v) {
  Query(..query, values:)
}

// ---- Values ----

/// Values
/// `Offset` represents a UTC offset. `Timestamptz` is composed
/// of a [`gleam/time/timestamp.Timestamp`][1] and `Offset`. The offset will be
/// applied to the timestamp when being encoded.
///
/// A timestamp with a positive offset represents some time in
/// the future, relative to UTC.
/// A timestamp with a negative offset represents some time in
/// the past, relative to UTC.
///
/// `Offset`s will be subtracted from the `gleam/time/timestamp.Timestamp`
/// so the encoded value is a UTC timestamp.
///
/// [1]: https://hexdocs.pm/gleam_time/gleam/time/timestamp.html
pub type Offset {
  Offset(hours: Int, minutes: Int)
}

/// Returns an Offset with the provided hours and 0 minutes.
pub fn utc_offset(hours: Int) -> Offset {
  Offset(hours:, minutes: 0)
}

/// Applies some number of minutes to the Offset
pub fn minutes(offset: Offset, minutes: Int) -> Offset {
  Offset(..offset, minutes:)
}

pub type Value {
  Uuid(uuid.Uuid)
  Null
  Bool(Bool)
  Int(Int)
  Float(Float)
  Text(String)
  Bytea(BitArray)
  Date(calendar.Date)
  Time(calendar.TimeOfDay)
  Datetime(calendar.Date, calendar.TimeOfDay)
  Timestamp(timestamp.Timestamp)
  Timestamptz(timestamp.Timestamp, Offset)
  Interval(interval.Interval)
  Array(List(Value))
}

pub fn uuid(uuid: uuid.Uuid) -> Value {
  Uuid(uuid)
}

pub const null = Null

pub const true = Bool(True)

pub const false = Bool(False)

pub fn bool(bool: Bool) -> Value {
  Bool(bool)
}

pub fn int(int: Int) -> Value {
  Int(int)
}

pub fn float(float: Float) -> Value {
  Float(float)
}

pub fn text(text: String) -> Value {
  Text(text)
}

pub fn bytea(bytea: BitArray) -> Value {
  Bytea(bytea)
}

pub fn date(date: calendar.Date) -> Value {
  Date(date)
}

pub fn time(time_of_day: calendar.TimeOfDay) -> Value {
  Time(time_of_day)
}

pub fn datetime(date: calendar.Date, time: calendar.TimeOfDay) -> Value {
  Datetime(date, time)
}

pub fn timestamp(timestamp: timestamp.Timestamp) -> Value {
  Timestamp(timestamp)
}

pub fn timestamptz(timestamp: timestamp.Timestamp, offset: Offset) -> Value {
  Timestamptz(timestamp, offset)
}

pub fn interval(interval: interval.Interval) -> Value {
  Interval(interval)
}

pub fn array(elements: List(a), of kind: fn(a) -> Value) -> Value {
  elements
  |> list.map(kind)
  |> Array
}

// ---- Table ----

/// A SQL table reference, optionally aliased.
pub opaque type Table {
  Table(name: String, alias: Option(String))
}

/// Creates a table reference.
///
/// ```gleam
/// sql.table("users")
/// ```
pub fn table(name: String) -> Table {
  Table(name: name, alias: None)
}

/// Sets an alias on a table. Renders as `table AS alias`.
///
/// ```gleam
/// sql.table("users") |> sql.table_as("u")
/// ```
pub fn table_as(table: Table, alias: String) -> Table {
  Table(..table, alias: Some(alias))
}

// ---- Column ----

type Aggregate {
  Count
  Sum
  Avg
  Max
  Min
}

/// A SQL column reference, optionally qualified by a table name, aliased,
/// or wrapped in an aggregate function.
pub opaque type Column {
  Column(
    table: Option(String),
    name: String,
    alias: Option(String),
    func: Option(Aggregate),
  )
  Star
}

/// Creates a plain column reference.
///
/// ```gleam
/// sql.col("email")
/// // Renders as: email
/// ```
pub fn col(name: String) -> Column {
  Column(table: None, name: name, alias: None, func: None)
}

/// Creates a column wrapped in `COUNT(...)`.
pub fn count(name: String) -> Column {
  Column(table: None, name: name, alias: None, func: Some(Count))
}

/// Creates a column wrapped in `SUM(...)`.
pub fn sum(name: String) -> Column {
  Column(table: None, name: name, alias: None, func: Some(Sum))
}

/// Creates a column wrapped in `AVG(...)`.
pub fn avg(name: String) -> Column {
  Column(table: None, name: name, alias: None, func: Some(Avg))
}

/// Creates a column wrapped in `MAX(...)`.
pub fn max(name: String) -> Column {
  Column(table: None, name: name, alias: None, func: Some(Max))
}

/// Creates a column wrapped in `MIN(...)`.
pub fn min(name: String) -> Column {
  Column(table: None, name: name, alias: None, func: Some(Min))
}

/// Qualifies a column with a table name. Renders as `table.column`.
/// No-ops on `star`.
///
/// ```gleam
/// sql.col("email") |> sql.col_for("users")
/// // Renders as: users.email
/// ```
pub fn col_for(column: Column, table: String) -> Column {
  case column {
    Column(..) -> Column(..column, table: Some(table))
    Star -> Star
  }
}

/// Sets an alias on a column. Renders as `column AS alias`.
/// No-ops on `star`.
///
/// ```gleam
/// sql.col("total") |> sql.col_as("order_total")
/// // Renders as: total AS order_total
/// ```
pub fn col_as(column: Column, a: String) -> Column {
  case column {
    Column(..) -> Column(..column, alias: Some(a))
    Star -> Star
  }
}

/// The `*` wildcard column, for use in `SELECT *`.
pub const star = Star

// ---- Adapter ----

/// Database adapter that controls how queries are serialized.
///
/// An adapter defines how placeholders, identifiers, and values are formatted
/// for a specific database backend. Create one with `adapter()` and configure
/// it using the `on_*` builder functions.
///
/// ```gleam
/// let pg =
///   sql.adapter()
///   |> sql.on_placeholder(with: fn(i) { "$" <> int.to_string(i + 1) })
///   |> sql.on_value(with: my_value_to_string)
///   |> sql.on_null(with: fn() { Null })
///   |> sql.on_int(with: Integer)
///   |> sql.on_text(with: Text)
/// ```
pub opaque type Adapter(v) {
  Adapter(
    handle_placeholder: fn(Int) -> String,
    handle_value: fn(v) -> String,
    handle_identifier: fn(String) -> String,
    handle_null: fn() -> v,
    handle_int: fn(Int) -> v,
    handle_text: fn(String) -> v,
  )
}

/// Creates a new adapter with sensible defaults.
///
/// Defaults to `"?"` placeholders and does not quote identifiers.
/// The `on_value`, `on_null`, `on_int`, and `on_text` handlers
/// will panic if left unconfigured.
///
/// Use `adapter()` for a ready-to-use adapter that works with the
/// built-in `Value` type.
pub fn new_adapter() -> Adapter(v) {
  Adapter(
    handle_placeholder: fn(_) { "?" },
    handle_value: fn(_) { panic as "sql.Adapter not configured (on_value)" },
    handle_identifier: function.identity,
    handle_null: fn() { panic as "sql.Adapter not configured (on_null)" },
    handle_int: fn(_) { panic as "sql.Adapter not configured (on_int)" },
    handle_text: fn(_) { panic as "sql.Adapter not configured (on_text)" },
  )
}

/// Sets the placeholder format function.
///
/// ```gleam
/// adapter |> sql.on_placeholder(with: fn(_) { "?" })
///
/// adapter |> sql.on_placeholder(fn(idx) { "$" <> int.to_string(idx + 1) })
/// ```
pub fn on_placeholder(
  adapter: Adapter(v),
  with handle_placeholder: fn(Int) -> String,
) -> Adapter(v) {
  Adapter(..adapter, handle_placeholder:)
}

/// Sets the function used to render a value as a literal SQL string.
///
/// Only needed for `to_string` output — `to_query` uses placeholders instead.
pub fn on_value(
  adapter: Adapter(v),
  with handle_value: fn(v) -> String,
) -> Adapter(v) {
  Adapter(..adapter, handle_value:)
}

/// Sets the identifier quoting function.
///
/// Defaults to identity (no quoting). Override to add database-specific quoting.
///
/// ```gleam
/// // Quote with double quotes
/// adapter |> sql.on_identifier(with: fn(name) { "\"" <> name <> "\"" })
/// ```
pub fn on_identifier(
  adapter: Adapter(v),
  with handle_identifier: fn(String) -> String,
) -> Adapter(v) {
  Adapter(..adapter, handle_identifier:)
}

/// Sets the function that produces the null representation for type `v`.
///
/// Used when a `nullable` kind resolves to `None`.
pub fn on_null(adapter: Adapter(v), with fun: fn() -> v) -> Adapter(v) {
  Adapter(..adapter, handle_null: fun)
}

/// Sets the function that wraps an `Int` into the value type `v`.
///
/// Used internally when rendering `LIMIT`, `OFFSET`, and other integer literals.
pub fn on_int(adapter: Adapter(v), with fun: fn(Int) -> v) -> Adapter(v) {
  Adapter(..adapter, handle_int: fun)
}

/// Sets the function that wraps a `String` into the value type `v`.
///
/// Used internally when rendering `LIKE` patterns and other string literals.
pub fn on_text(adapter: Adapter(v), with fun: fn(String) -> v) -> Adapter(v) {
  Adapter(..adapter, handle_text: fun)
}

// ---- Row ----

/// A type-safe row for INSERT statements.
///
/// Rows are built as continuation-passing linked lists using `field` and `final`,
/// ensuring that every row has the same columns at compile time.
///
/// ```gleam
/// let row = {
///   use <- sql.field(column: "name", value: sql.text("Alice"))
///   sql.final(column: "age", value: sql.int(30))
/// }
/// ```
pub opaque type Row(v) {
  Row(column: String, value: v, next: Option(fn() -> Row(v)))
}

/// Adds a column/value pair to a row, with a continuation for the next field.
///
/// ```gleam
/// use <- sql.field(column: "name", value: sql.text("Alice"))
/// use <- sql.field(column: "email", value: sql.text("alice@example.com"))
/// sql.final(column: "age", value: sql.int(30))
/// ```
pub fn field(
  column column: String,
  value value: v,
  next next: fn() -> Row(v),
) -> Row(v) {
  Row(column: column, value: value, next: Some(next))
}

/// Creates the last column/value pair in a row (the terminal element).
pub fn final(column column: String, value value: v) -> Row(v) {
  Row(column: column, value: value, next: None)
}

fn row_to_columns_and_values(row: Row(v)) -> #(List(String), List(v)) {
  case row.next {
    Some(next) -> {
      let #(columns, values) = next() |> row_to_columns_and_values
      #([row.column, ..columns], [row.value, ..values])
    }
    None -> #([row.column], [row.value])
  }
}

// ---- Operand ----

type Operand(v) {
  Col(Column)
  Val(v)
  NullVal
  SubQuery(QueryBuilder(Select, v))
  AnyQuery(QueryBuilder(Select, v))
  AllQuery(QueryBuilder(Select, v))
}

// ---- Condition ----

/// A WHERE clause condition.
///
/// Conditions are built using constructor functions like `eq`, `gt`, `like`,
/// `is_null`, etc. They can be combined with `and`, `or`, and `not`. Raw SQL
/// conditions are also supported via `raw` and `raw_with_values`.
pub opaque type Condition(v) {
  Equal(left: Operand(v), right: Operand(v))
  NotEqual(left: Operand(v), right: Operand(v))
  GreaterThan(left: Operand(v), right: Operand(v))
  LessThan(left: Operand(v), right: Operand(v))
  GreaterThanOrEqual(left: Operand(v), right: Operand(v))
  LessThanOrEqual(left: Operand(v), right: Operand(v))
  Between(operand: Operand(v), low: Operand(v), high: Operand(v))
  Like(operand: Operand(v), pattern: Operand(v))
  NotLike(operand: Operand(v), pattern: Operand(v))
  In(operand: Operand(v), values: List(Operand(v)))
  IsNull(operand: Operand(v))
  IsNotNull(operand: Operand(v))
  IsTrue(operand: Operand(v))
  IsFalse(operand: Operand(v))
  And(left: Condition(v), right: Condition(v))
  Or(left: Condition(v), right: Condition(v))
  Not(condition: Condition(v))
  Exists(QueryBuilder(Select, v))
  Raw(sql: String, values: List(v))
}

// ---- Join ----

/// Internal representation of a JOIN clause.
type Join(v) {
  InnerJoin(table: Table, on: List(Condition(v)))
  LeftJoin(table: Table, on: List(Condition(v)))
  RightJoin(table: Table, on: List(Condition(v)))
  FullJoin(table: Table, on: List(Condition(v)))
}

/// Adds an `INNER JOIN` clause to a SELECT query.
/// Multiple conditions are combined with AND at render time.
pub fn inner_join(
  query: QueryBuilder(Select, v),
  table table: Table,
  on on: List(Condition(v)),
) -> QueryBuilder(Select, v) {
  append_join(query, InnerJoin(table: table, on: on))
}

/// Adds a `LEFT JOIN` clause to a SELECT query.
/// Multiple conditions are combined with AND at render time.
pub fn left_join(
  query: QueryBuilder(Select, v),
  table table: Table,
  on on: List(Condition(v)),
) -> QueryBuilder(Select, v) {
  append_join(query, LeftJoin(table: table, on: on))
}

/// Adds a `RIGHT JOIN` clause to a SELECT query.
/// Multiple conditions are combined with AND at render time.
pub fn right_join(
  query: QueryBuilder(Select, v),
  table table: Table,
  on on: List(Condition(v)),
) -> QueryBuilder(Select, v) {
  append_join(query, RightJoin(table: table, on: on))
}

/// Adds a `FULL JOIN` clause to a SELECT query.
/// Multiple conditions are combined with AND at render time.
pub fn full_join(
  query: QueryBuilder(Select, v),
  table table: Table,
  on on: List(Condition(v)),
) -> QueryBuilder(Select, v) {
  append_join(query, FullJoin(table: table, on: on))
}

fn append_join(
  query: QueryBuilder(Select, v),
  j: Join(v),
) -> QueryBuilder(Select, v) {
  case query {
    SelectBuilder(..) ->
      SelectBuilder(..query, joins: list.append(query.joins, [j]))
    _ -> query
  }
}

// ---- Order ----

/// Sort direction for ORDER BY clauses.
pub opaque type Order {
  Asc
  Desc
}

/// Ascending sort order.
pub const asc = Asc

/// Descending sort order.
pub const desc = Desc

/// An ORDER BY clause entry pairing a column with a sort direction.
///
/// Created internally by the `order_by` function — not constructed directly.
pub opaque type OrderBy {
  OrderBy(column: Column, direction: Order)
}

// ---- OnConflict ----

/// The action to take when an INSERT conflict occurs.
///
/// - `DoNothing` — ignores the conflicting row (`ON CONFLICT ... DO NOTHING`)
/// - `DoUpdate(sets:)` — updates specified columns (`ON CONFLICT ... DO UPDATE SET ...`).
///   Each tuple is `#(column_name, expression_string)`.
pub type ConflictAction(v) {
  DoNothing
  DoUpdate(sets: List(#(String, String)))
}

/// An ON CONFLICT clause for INSERT statements.
///
/// Created by the `on_conflict` function — not constructed directly.
pub opaque type OnConflict(v) {
  OnConflict(
    target: String,
    action: ConflictAction(v),
    wheres: List(Condition(v)),
  )
}

/// Describes how to convert an input of type `a` into an internal operand
/// for query building.
///
/// Use the built-in kind constants `value`, `subquery`, `column`, `any`, and
/// `all`, or create custom kinds with `nullable` and `list`.
///
/// ```gleam
/// // Compare a column to a plain value
/// sql.eq(sql.col("age"), 21, of: sql.value)
///
/// // Compare with a nullable value
/// sql.eq(sql.col("name"), Some("Alice"), of: sql.nullable(of: sql.value))
/// ```
pub opaque type Kind(a, v) {
  Kind(to_operand: fn(a) -> Operand(v))
}

// ---- Phantom Types ----

/// Phantom type for SELECT queries.
pub type Select

/// Phantom type for INSERT queries.
pub type Insert

/// Phantom type for UPDATE queries.
pub type Update

/// Phantom type for DELETE queries.
pub type Delete

/// Phantom type for the initial FROM stage before selecting a query kind.
pub type From(a)

pub type Subquery

// ---- Union ----

type UnionType {
  Union
  UnionAll
}

// ---- From ----

type FromClause(v) {
  FromTable(Table)
  FromSubQuery(query: QueryBuilder(Select, v), alias: String)
}

// ---- QueryBuilder ----

/// The main query builder type, parameterized by a phantom `kind` type
/// (`Select`, `Insert`, `Update`, `Delete`, or `From`) and a value type `v`.
///
/// The phantom type restricts which modifier functions can be applied,
/// preventing invalid combinations at compile time.
///
/// ```gleam
/// // kind=From initially, becomes Select after sql.select()
/// sql.from(users)
/// |> sql.select([sql.star])
/// |> sql.where(sql.eq(sql.col("active"), True, of: sql.value))
/// |> sql.to_query(adapter)
/// ```
pub opaque type QueryBuilder(kind, v) {
  SelectBuilder(
    columns: List(Column),
    from: FromClause(v),
    wheres: List(Condition(v)),
    joins: List(Join(v)),
    order_by: List(OrderBy),
    limit: Option(Int),
    offset: Option(Int),
    group_by: List(Column),
    distinct: Bool,
    having: List(Condition(v)),
    for_update: Bool,
    ctes: List(Cte(v)),
    recursive: Bool,
  )
  InsertBuilder(
    into: Table,
    columns: List(String),
    values: List(List(v)),
    returning: List(Column),
    on_conflict: Option(OnConflict(v)),
    ctes: List(Cte(v)),
    recursive: Bool,
  )
  UpdateBuilder(
    table: Table,
    sets: List(#(String, Operand(v))),
    wheres: List(Condition(v)),
    returning: List(Column),
    order_by: List(OrderBy),
    limit: Option(Int),
    offset: Option(Int),
    ctes: List(Cte(v)),
    recursive: Bool,
  )
  DeleteBuilder(
    from: Table,
    wheres: List(Condition(v)),
    returning: List(Column),
    ctes: List(Cte(v)),
    recursive: Bool,
  )
  UnionBuilder(
    selects: List(QueryBuilder(Select, v)),
    union_type: UnionType,
    ctes: List(Cte(v)),
    recursive: Bool,
  )
  FromTableBuilder(table: Table)
  FromSubQueryBuilder(query: QueryBuilder(Select, v), alias: String)
}

// ---- CTE ----

/// A Common Table Expression (CTE) for use with `WITH` clauses.
///
/// Created with `cte` and optionally refined with `cte_columns`.
pub opaque type Cte(v) {
  Cte(name: String, columns: List(String), query: QueryBuilder(Select, v))
}

// ---- Condition Constructors ----

/// Creates an equality condition (`column = input`).
///
/// ```gleam
/// sql.eq(sql.col("name"), "Alice", of: sql.value)
/// // Renders as: name = $1
/// ```
pub fn eq(column: Column, input: a, of kind: Kind(a, v)) -> Condition(v) {
  Equal(Col(column), kind.to_operand(input))
}

/// Creates an inequality condition (`column != input`).
pub fn not_eq(column: Column, input: a, of kind: Kind(a, v)) -> Condition(v) {
  NotEqual(Col(column), kind.to_operand(input))
}

/// Creates a greater-than condition (`column > input`).
pub fn gt(column: Column, input: a, of kind: Kind(a, v)) -> Condition(v) {
  GreaterThan(Col(column), kind.to_operand(input))
}

/// Creates a less-than condition (`column < input`).
pub fn lt(column: Column, input: a, of kind: Kind(a, v)) -> Condition(v) {
  LessThan(Col(column), kind.to_operand(input))
}

/// Creates a greater-than-or-equal condition (`column >= input`).
pub fn gt_eq(column: Column, input: a, of kind: Kind(a, v)) -> Condition(v) {
  GreaterThanOrEqual(Col(column), kind.to_operand(input))
}

/// Creates a less-than-or-equal condition (`column <= input`).
pub fn lt_eq(column: Column, input: a, of kind: Kind(a, v)) -> Condition(v) {
  LessThanOrEqual(Col(column), kind.to_operand(input))
}

/// Creates a BETWEEN condition (`column BETWEEN low AND high`).
pub fn between(
  column: Column,
  low: a,
  high: a,
  of kind: Kind(a, v),
) -> Condition(v) {
  Between(Col(column), kind.to_operand(low), kind.to_operand(high))
}

/// Creates a LIKE condition (`column LIKE pattern`).
pub fn like(column: Column, input: a, of kind: Kind(a, v)) -> Condition(v) {
  Like(Col(column), kind.to_operand(input))
}

/// Creates a NOT LIKE condition (`column NOT LIKE pattern`).
pub fn not_like(column: Column, input: a, of kind: Kind(a, v)) -> Condition(v) {
  NotLike(Col(column), kind.to_operand(input))
}

/// Creates an IN condition (`column IN (v1, v2, ...)`).
///
/// ```gleam
/// sql.in(sql.col("status"), ["active", "pending"], of: sql.value)
/// // Renders as: status IN ($1, $2)
/// ```
pub fn in(column: Column, values: List(a), of kind: Kind(a, v)) -> Condition(v) {
  In(Col(column), list.map(values, kind.to_operand))
}

/// Creates an IS NULL condition (`column IS NULL`).
pub fn is_null(column: Column) -> Condition(v) {
  IsNull(Col(column))
}

/// Creates an IS NOT NULL condition (`column IS NOT NULL`).
pub fn is_not_null(column: Column) -> Condition(v) {
  IsNotNull(Col(column))
}

/// Creates an IS TRUE condition (`column IS TRUE`).
pub fn is_true(column: Column) -> Condition(v) {
  IsTrue(Col(column))
}

/// Creates an IS FALSE condition (`column IS FALSE`).
pub fn is_false(column: Column) -> Condition(v) {
  IsFalse(Col(column))
}

/// Combines two conditions with OR (`left OR right`).
///
/// For adding an OR clause to an existing query's WHERE, see `or_where`.
pub fn or(left: Condition(v), right: Condition(v)) -> Condition(v) {
  Or(left, right)
}

/// Combines two conditions with AND (`left AND right`).
pub fn and(left: Condition(v), right: Condition(v)) -> Condition(v) {
  And(left, right)
}

/// Negates a condition (`NOT (condition)`).
pub fn not(condition: Condition(v)) -> Condition(v) {
  Not(condition)
}

/// Creates an EXISTS condition (`EXISTS (subquery)`).
pub fn exists(query: QueryBuilder(Select, v)) -> Condition(v) {
  Exists(query)
}

/// Creates a raw SQL condition without parameterized values.
///
/// ```gleam
/// sql.raw("age > 18")
/// ```
pub fn raw(sql: String) -> Condition(v) {
  Raw(sql: sql, values: [])
}

/// Creates a raw SQL condition with parameterized values.
///
/// Use `?` as placeholders — they are rewritten to the adapter's placeholder
/// format automatically.
///
/// ```gleam
/// sql.raw_with_values("age > ?", [sql.int(18)])
/// ```
pub fn raw_with_values(sql: String, values: List(v)) -> Condition(v) {
  Raw(sql: sql, values: values)
}

// ---- Query Builder Constructors ----

/// Starts building a query from a table. This is the entry point for SELECT
/// and DELETE queries.
///
/// Pipe into `select` or `delete` to choose the query kind.
///
/// ```gleam
/// let users = sql.table("users")
///
/// // SELECT
/// sql.from(users) |> sql.select([sql.star])
///
/// // DELETE
/// sql.from(users) |> sql.delete()
/// ```
pub fn from(table: Table) -> QueryBuilder(From(Table), v) {
  FromTableBuilder(table:)
}

/// Converts a `From` builder into a SELECT query with the given columns.
///
/// ```gleam
/// sql.from(sql.table("users"))
/// |> sql.select([sql.col("name"), sql.col("email")])
/// ```
pub fn select(
  query: QueryBuilder(From(a), v),
  columns: List(Column),
) -> QueryBuilder(Select, v) {
  let from = case query {
    FromTableBuilder(table:) -> FromTable(table)
    FromSubQueryBuilder(query:, alias:) -> FromSubQuery(query:, alias:)
    _ -> panic as "select called on non-From builder"
  }

  SelectBuilder(
    columns: columns,
    from:,
    wheres: [],
    joins: [],
    order_by: [],
    limit: None,
    offset: None,
    group_by: [],
    distinct: False,
    having: [],
    for_update: False,
    ctes: [],
    recursive: False,
  )
}

/// Creates a new INSERT query builder for the given table.
///
/// ```gleam
/// sql.insert(into: sql.table("users"))
/// |> sql.values([row1, row2])
/// |> sql.to_query(adapter)
/// ```
pub fn insert(into tbl: Table) -> QueryBuilder(Insert, v) {
  InsertBuilder(
    into: tbl,
    columns: [],
    values: [],
    returning: [],
    on_conflict: None,
    ctes: [],
    recursive: False,
  )
}

/// Creates a new UPDATE query builder for the given table.
///
/// ```gleam
/// sql.update(table: sql.table("users"))
/// |> sql.set("name", "Bob", of: sql.value)
/// |> sql.where(sql.eq(sql.col("id"), 1, of: sql.value))
/// |> sql.to_query(adapter)
/// ```
pub fn update(table tbl: Table) -> QueryBuilder(Update, v) {
  UpdateBuilder(
    table: tbl,
    sets: [],
    wheres: [],
    returning: [],
    order_by: [],
    limit: None,
    offset: None,
    ctes: [],
    recursive: False,
  )
}

/// Converts a `From` builder into a DELETE query.
///
/// ```gleam
/// sql.from(sql.table("users"))
/// |> sql.delete()
/// |> sql.where(sql.eq(sql.col("id"), 1, of: sql.value))
/// |> sql.to_query(adapter)
/// ```
pub fn delete(query: QueryBuilder(From(Table), v)) -> QueryBuilder(Delete, v) {
  case query {
    FromTableBuilder(table:) -> {
      DeleteBuilder(
        from: table,
        wheres: [],
        returning: [],
        ctes: [],
        recursive: False,
      )
    }
    _ -> panic as "delete called on non-From(Table) builder"
  }
}

// ---- Query Builder Modifiers (generic) ----

/// Creates a `From` builder that selects from a subquery instead of a table.
///
/// ```gleam
/// let sub =
///   sql.from(sql.table("orders"))
///   |> sql.select([sql.col("user_id"), sql.count("id") |> sql.col_as("cnt")])
///   |> sql.group_by([sql.col("user_id")])
///
/// sql.from_subquery(sub, alias: "order_counts")
/// |> sql.select([sql.star])
/// |> sql.to_query(adapter)
/// ```
pub fn from_subquery(
  query: QueryBuilder(Select, v),
  alias a: String,
) -> QueryBuilder(From(Subquery), v) {
  FromSubQueryBuilder(query:, alias: a)
}

/// Adds a WHERE condition to the query. Multiple `where` calls are combined
/// with AND.
///
/// Applies to SELECT, UPDATE, and DELETE queries. No-ops on other builder types.
pub fn where(
  query: QueryBuilder(a, v),
  condition: Condition(v),
) -> QueryBuilder(a, v) {
  case query {
    SelectBuilder(..) ->
      SelectBuilder(..query, wheres: list.append(query.wheres, [condition]))
    UpdateBuilder(..) ->
      UpdateBuilder(..query, wheres: list.append(query.wheres, [condition]))
    DeleteBuilder(..) ->
      DeleteBuilder(..query, wheres: list.append(query.wheres, [condition]))
    _ -> query
  }
}

/// Adds a WHERE condition combined with OR against the existing conditions.
///
/// If there are no existing conditions, behaves like `where`. Otherwise,
/// wraps all existing conditions in an AND group and ORs with the new condition.
pub fn or_where(
  query: QueryBuilder(a, v),
  condition: Condition(v),
) -> QueryBuilder(a, v) {
  case query {
    SelectBuilder(..) ->
      case query.wheres {
        [] -> SelectBuilder(..query, wheres: [condition])
        existing ->
          SelectBuilder(..query, wheres: [
            Or(combine_conditions(existing), condition),
          ])
      }
    UpdateBuilder(..) ->
      case query.wheres {
        [] -> UpdateBuilder(..query, wheres: [condition])
        existing ->
          UpdateBuilder(..query, wheres: [
            Or(combine_conditions(existing), condition),
          ])
      }
    DeleteBuilder(..) ->
      case query.wheres {
        [] -> DeleteBuilder(..query, wheres: [condition])
        existing ->
          DeleteBuilder(..query, wheres: [
            Or(combine_conditions(existing), condition),
          ])
      }
    _ -> query
  }
}

/// Adds a negated WHERE condition (`WHERE NOT condition`).
pub fn where_not(
  query: QueryBuilder(a, v),
  condition: Condition(v),
) -> QueryBuilder(a, v) {
  where(query, Not(condition))
}

/// Adds a `WHERE EXISTS (subquery)` condition to a SELECT query.
pub fn where_exists(
  query: QueryBuilder(Select, v),
  subquery sub: QueryBuilder(Select, v),
) -> QueryBuilder(Select, v) {
  where(query, Exists(sub))
}

/// Adds a RETURNING clause. Applies to INSERT, UPDATE, and DELETE queries.
///
/// ```gleam
/// sql.insert(into: users)
/// |> sql.values([row])
/// |> sql.returning([sql.col("id"), sql.col("name")])
/// ```
pub fn returning(
  query: QueryBuilder(a, v),
  columns: List(Column),
) -> QueryBuilder(a, v) {
  case query {
    InsertBuilder(..) -> InsertBuilder(..query, returning: columns)
    UpdateBuilder(..) -> UpdateBuilder(..query, returning: columns)
    DeleteBuilder(..) -> DeleteBuilder(..query, returning: columns)
    _ -> query
  }
}

// ---- Query Builder Modifiers (Select-only) ----

/// Adds an ORDER BY clause. Can be called multiple times to sort by
/// multiple columns. Applies to SELECT and UPDATE queries.
pub fn order_by(
  query: QueryBuilder(a, v),
  column: Column,
  direction: Order,
) -> QueryBuilder(a, v) {
  case query {
    SelectBuilder(..) ->
      SelectBuilder(
        ..query,
        order_by: list.append(query.order_by, [
          OrderBy(column, direction),
        ]),
      )
    UpdateBuilder(..) ->
      UpdateBuilder(
        ..query,
        order_by: list.append(query.order_by, [
          OrderBy(column, direction),
        ]),
      )
    _ -> query
  }
}

/// Adds a GROUP BY clause to a SELECT query.
pub fn group_by(
  query: QueryBuilder(Select, v),
  columns: List(Column),
) -> QueryBuilder(Select, v) {
  case query {
    SelectBuilder(..) ->
      SelectBuilder(..query, group_by: list.append(query.group_by, columns))
    _ -> query
  }
}

/// Adds a LIMIT clause. Applies to SELECT and UPDATE queries.
pub fn limit(query: QueryBuilder(a, v), n: Int) -> QueryBuilder(a, v) {
  case query {
    SelectBuilder(..) -> SelectBuilder(..query, limit: Some(n))
    UpdateBuilder(..) -> UpdateBuilder(..query, limit: Some(n))
    _ -> query
  }
}

/// Adds an OFFSET clause. Applies to SELECT and UPDATE queries.
pub fn offset(query: QueryBuilder(a, v), n: Int) -> QueryBuilder(a, v) {
  case query {
    SelectBuilder(..) -> SelectBuilder(..query, offset: Some(n))
    UpdateBuilder(..) -> UpdateBuilder(..query, offset: Some(n))
    _ -> query
  }
}

/// Adds `SELECT DISTINCT` to a SELECT query.
pub fn distinct(query: QueryBuilder(Select, v)) -> QueryBuilder(Select, v) {
  case query {
    SelectBuilder(..) -> SelectBuilder(..query, distinct: True)
    _ -> query
  }
}

/// Adds a HAVING clause to a SELECT query. Used with GROUP BY to filter
/// aggregated results.
pub fn having(
  query: QueryBuilder(Select, v),
  condition: Condition(v),
) -> QueryBuilder(Select, v) {
  case query {
    SelectBuilder(..) ->
      SelectBuilder(..query, having: list.append(query.having, [condition]))
    _ -> query
  }
}

/// Adds `FOR UPDATE` to a SELECT query for row-level locking.
pub fn for_update(query: QueryBuilder(Select, v)) -> QueryBuilder(Select, v) {
  case query {
    SelectBuilder(..) -> SelectBuilder(..query, for_update: True)
    _ -> query
  }
}

// ---- Query Builder Modifiers (Insert-only) ----

/// Sets the rows to insert. Columns are extracted from the first row.
/// Replaces any previously set values.
///
/// ```gleam
/// let row1 = {
///   use <- sql.field(column: "name", value: sql.text("Alice"))
///   sql.final(column: "age", value: sql.int(30))
/// }
///
/// sql.insert(into: users) |> sql.values([row1])
/// ```
pub fn values(
  query: QueryBuilder(Insert, v),
  rows: List(Row(v)),
) -> QueryBuilder(Insert, v) {
  case query {
    InsertBuilder(..) -> {
      let extracted = list.map(rows, fn(row) { row_to_columns_and_values(row) })
      let columns = case extracted {
        [#(cols, _), ..] -> cols
        [] -> []
      }
      let value_rows = list.map(extracted, fn(pair) { pair.1 })
      InsertBuilder(..query, columns: columns, values: value_rows)
    }
    _ -> query
  }
}

/// Adds an ON CONFLICT clause to an INSERT query (upsert).
///
/// ```gleam
/// sql.insert(into: users)
/// |> sql.values([row])
/// |> sql.on_conflict(
///   target: "email",
///   action: sql.DoNothing,
///   where: [],
/// )
/// ```
pub fn on_conflict(
  query: QueryBuilder(Insert, v),
  target target: String,
  action action: ConflictAction(v),
  where wheres: List(Condition(v)),
) -> QueryBuilder(Insert, v) {
  case query {
    InsertBuilder(..) ->
      InsertBuilder(
        ..query,
        on_conflict: Some(OnConflict(
          target: target,
          action: action,
          wheres: wheres,
        )),
      )
    _ -> query
  }
}

// ---- Query Builder Modifiers (Update-only) ----

/// Kind that treats the input as a parameterized value.
///
/// This is the most common kind — use it when comparing a column to a literal.
///
/// ```gleam
/// sql.eq(sql.col("name"), "Alice", of: sql.value)
/// ```
pub const value = Kind(to_operand: value_to_operand)

fn value_to_operand(v) -> Operand(v) {
  Val(v)
}

/// Kind that treats the input as a subquery for scalar comparisons.
///
/// ```gleam
/// sql.eq(sql.col("id"), subquery, of: sql.subquery)
/// // Renders as: id = (SELECT ...)
/// ```
pub const subquery = Kind(to_operand: subquery_to_operand)

fn subquery_to_operand(q) -> Operand(v) {
  SubQuery(q)
}

/// Kind that treats the input as a column reference for column-to-column
/// comparisons.
///
/// ```gleam
/// sql.eq(sql.col("id") |> sql.col_for("users"), sql.col("user_id") |> sql.col_for("posts"), of: sql.column)
/// // Renders as: users.id = posts.user_id
/// ```
pub const column = Kind(to_operand: column_to_operand)

fn column_to_operand(c) -> Operand(v) {
  Col(c)
}

/// Kind that wraps a subquery with `ANY(...)`.
///
/// ```gleam
/// sql.eq(sql.col("id"), subquery, of: sql.any)
/// // Renders as: id = ANY(SELECT ...)
/// ```
pub const any = Kind(to_operand: any_to_operand)

fn any_to_operand(q) -> Operand(v) {
  AnyQuery(q)
}

/// Kind that wraps a subquery with `ALL(...)`.
///
/// ```gleam
/// sql.gt(sql.col("score"), subquery, of: sql.all)
/// // Renders as: score > ALL(SELECT ...)
/// ```
pub const all = Kind(to_operand: all_to_operand)

fn all_to_operand(q) -> Operand(v) {
  AllQuery(q)
}

/// Wraps a kind to accept `Option(a)`, mapping `None` to SQL NULL.
///
/// ```gleam
/// sql.eq(sql.col("name"), Some("Alice"), of: sql.nullable(of: sql.value))
/// // Some("Alice") renders as: name = $1
///
/// sql.eq(sql.col("name"), None, of: sql.nullable(of: sql.value))
/// // None renders as: name = NULL
/// ```
pub fn nullable(of kind: Kind(a, v)) -> Kind(Option(a), v) {
  Kind(to_operand: fn(opt) {
    case opt {
      Some(x) -> kind.to_operand(x)
      None -> NullVal
    }
  })
}

/// Creates a kind that maps an arbitrary type to a value using a conversion
/// function. Useful for `in` clauses with custom types.
///
/// ```gleam
/// sql.in(sql.col("id"), [1, 2, 3], of: sql.list(of: sql.int))
/// ```
pub fn list(of map: fn(a) -> v) -> Kind(a, v) {
  Kind(to_operand: fn(x) { Val(map(x)) })
}

/// Sets a column to a value in an UPDATE query. Can be called multiple times.
///
/// ```gleam
/// sql.update(table: users)
/// |> sql.set("name", "Bob", of: sql.value)
/// |> sql.set("age", 25, of: sql.value)
/// ```
pub fn set(
  query: QueryBuilder(Update, v),
  column: String,
  input: a,
  of kind: Kind(a, v),
) -> QueryBuilder(Update, v) {
  case query {
    UpdateBuilder(..) ->
      UpdateBuilder(
        ..query,
        sets: list.append(query.sets, [#(column, kind.to_operand(input))]),
      )
    _ -> query
  }
}

// ---- CTE Builder Functions ----

/// Creates a Common Table Expression (CTE).
///
/// ```gleam
/// let active_users =
///   sql.cte(
///     name: "active_users",
///     query: sql.from(users) |> sql.select([sql.star]) |> sql.where(...)
///   )
/// ```
pub fn cte(name name: String, query query: QueryBuilder(Select, v)) -> Cte(v) {
  Cte(name: name, columns: [], query: query)
}

/// Sets explicit column names on a CTE. Renders as `name(col1, col2) AS (...)`.
pub fn cte_columns(cte c: Cte(v), columns cols: List(String)) -> Cte(v) {
  Cte(..c, columns: cols)
}

/// Attaches CTEs to a query as a `WITH` clause.
///
/// ```gleam
/// query |> sql.with(ctes: [active_users_cte])
/// ```
pub fn with(
  query: QueryBuilder(a, v),
  ctes ctes: List(Cte(v)),
) -> QueryBuilder(a, v) {
  case query {
    SelectBuilder(..) -> SelectBuilder(..query, ctes: ctes)
    InsertBuilder(..) -> InsertBuilder(..query, ctes: ctes)
    UpdateBuilder(..) -> UpdateBuilder(..query, ctes: ctes)
    DeleteBuilder(..) -> DeleteBuilder(..query, ctes: ctes)
    UnionBuilder(..) -> UnionBuilder(..query, ctes: ctes)
    FromTableBuilder(..) -> query
    FromSubQueryBuilder(..) -> query
  }
}

/// Marks the query's WITH clause as `WITH RECURSIVE`.
pub fn recursive(query: QueryBuilder(a, v)) -> QueryBuilder(a, v) {
  case query {
    SelectBuilder(..) -> SelectBuilder(..query, recursive: True)
    InsertBuilder(..) -> InsertBuilder(..query, recursive: True)
    UpdateBuilder(..) -> UpdateBuilder(..query, recursive: True)
    DeleteBuilder(..) -> DeleteBuilder(..query, recursive: True)
    UnionBuilder(..) -> UnionBuilder(..query, recursive: True)
    FromTableBuilder(..) -> query
    FromSubQueryBuilder(..) -> query
  }
}

// ---- Union ----

/// Combines two SELECT queries with `UNION` (removes duplicates).
///
/// Can be chained to union multiple queries.
///
/// ```gleam
/// query_a |> sql.union(query_b) |> sql.union(query_c)
/// // Renders as: SELECT ... UNION SELECT ... UNION SELECT ...
/// ```
pub fn union(
  query: QueryBuilder(Select, v),
  other: QueryBuilder(Select, v),
) -> QueryBuilder(Select, v) {
  case query {
    UnionBuilder(selects:, union_type: Union, ..) ->
      UnionBuilder(..query, selects: list.append(selects, [other]))
    _ ->
      UnionBuilder(
        selects: [query, other],
        union_type: Union,
        ctes: [],
        recursive: False,
      )
  }
}

/// Combines two SELECT queries with `UNION ALL` (preserves duplicates).
pub fn union_all(
  query: QueryBuilder(Select, v),
  other: QueryBuilder(Select, v),
) -> QueryBuilder(Select, v) {
  case query {
    UnionBuilder(selects:, union_type: UnionAll, ..) ->
      UnionBuilder(..query, selects: list.append(selects, [other]))
    _ ->
      UnionBuilder(
        selects: [query, other],
        union_type: UnionAll,
        ctes: [],
        recursive: False,
      )
  }
}

// ---- Output: to_query ----

/// Serializes a query builder into a `Query(v)` with parameterized placeholders.
///
/// Returns a `Query` record with `.sql` containing the SQL string (with
/// placeholders like `$1`, `$2`) and `.values` containing the parameter values
/// in order.
///
/// ```gleam
/// let query =
///   sql.from(users)
///   |> sql.select([sql.star])
///   |> sql.where(sql.eq(sql.col("id"), 1, of: sql.value))
///   |> sql.to_query(adapter)
///
/// query.sql     // "SELECT * FROM users WHERE id = $1"
/// query.values  // [Integer(1)]
/// ```
pub fn to_query(query: QueryBuilder(a, v), adapter: Adapter(v)) -> Query(v) {
  let #(sql, vals) = build_query(query, adapter)
  let sql = replace_placeholders(sql, adapter)
  Query(sql: sql, values: vals)
}

// ---- Output: to_string ----

/// Serializes a query builder into a plain SQL string with values inlined.
///
/// Values are formatted using the adapter's `on_value` handler. This is useful
/// for debugging and logging — use `to_query` for actual database execution.
pub fn to_string(query: QueryBuilder(a, v), adapter: Adapter(v)) -> String {
  let #(sql, vals) = build_query(query, adapter)
  replace_with_values(sql, vals, adapter)
}

// ---- Default Adapter ----

fn value_to_string(value: Value) -> String {
  case value {
    Uuid(val) -> uuid.to_string(val)
    Null -> "NULL"
    Bool(val) -> bool_to_string(val)
    Int(val) -> int.to_string(val)
    Float(val) -> float.to_string(val)
    Text(val) -> text_to_string(val)
    Bytea(val) -> bytea_to_string(val)
    Time(val) -> time_to_string(val)
    Date(val) -> date_to_string(val)
    Datetime(date, time) -> datetime_to_string(date, time)
    Timestamp(val) -> timestamp_to_string(val)
    Timestamptz(ts, offset) -> timestamptz_to_string(ts, offset)
    Interval(val) -> interval.to_iso8601_string(val)
    Array(val) -> array_to_string(val)
  }
}

fn array_to_string(array: List(Value)) -> String {
  let elems = case array {
    [] -> ""
    [val] -> value_to_string(val)
    vals -> {
      vals
      |> list.map(value_to_string)
      |> string.join(", ")
    }
  }

  "[" <> elems <> "]"
}

fn text_to_string(val: String) -> String {
  let val = string.replace(in: val, each: "'", with: "''")

  single_quote(val)
}

fn bool_to_string(val: Bool) -> String {
  case val {
    True -> "TRUE"
    False -> "FALSE"
  }
}

fn bytea_to_string(val: BitArray) -> String {
  let val = "\\x" <> bit_array.base16_encode(val)

  single_quote(val)
}

fn date_to_string(date: calendar.Date) -> String {
  format_date(date) |> single_quote
}

fn datetime_to_string(dt: calendar.Date, tod: calendar.TimeOfDay) -> String {
  let date = format_date(dt)
  let time = format_time(tod)

  { date <> " " <> time }
  |> single_quote
}

fn time_to_string(tod: calendar.TimeOfDay) -> String {
  format_time(tod) |> single_quote
}

fn format_date(date: calendar.Date) -> String {
  let year = int.to_string(date.year)
  let month = calendar.month_to_int(date.month) |> pad_zero
  let day = pad_zero(date.day)

  year <> "-" <> month <> "-" <> day
}

fn format_time(tod: calendar.TimeOfDay) -> String {
  let hours = pad_zero(tod.hours)
  let minutes = pad_zero(tod.minutes)
  let seconds = pad_zero(tod.seconds)
  let milliseconds = tod.nanoseconds / 1_000_000

  let msecs = case milliseconds < 100 {
    True if milliseconds == 0 -> ""
    True if milliseconds < 10 -> ".00" <> int.to_string(milliseconds)
    True -> ".0" <> int.to_string(milliseconds)
    False -> "." <> int.to_string(milliseconds)
  }

  hours <> ":" <> minutes <> ":" <> seconds <> msecs
}

fn timestamp_to_string(ts: timestamp.Timestamp) -> String {
  timestamp.to_rfc3339(ts, calendar.utc_offset)
  |> single_quote
}

fn timestamptz_to_string(
  timestamp: timestamp.Timestamp,
  offset: Offset,
) -> String {
  offset_to_duration(offset)
  |> timestamp.add(timestamp, _)
  |> timestamp_to_string
}

fn offset_to_duration(offset: Offset) -> duration.Duration {
  let sign = case offset.hours < 0 {
    True -> 1
    False -> -1
  }

  int.absolute_value(offset.hours)
  |> int.multiply(60)
  |> int.add(offset.minutes)
  |> int.multiply(sign)
  |> duration.minutes
}

fn single_quote(val: String) -> String {
  "'" <> val <> "'"
}

fn pad_zero(n: Int) -> String {
  case n < 10 {
    True -> "0" <> int.to_string(n)
    False -> int.to_string(n)
  }
}

/// Returns a ready-to-use adapter for the built-in `Value` type.
///
/// handles all `Value` variants for `to_string` output.
pub fn adapter() -> Adapter(Value) {
  Adapter(
    handle_placeholder: fn(_) { "?" },
    handle_identifier: function.identity,
    handle_value: value_to_string,
    handle_null: fn() { Null },
    handle_int: int,
    handle_text: text,
  )
}

// ---- Internal: Sentinel-Based Rendering ----
//
// All build_* functions produce SQL with `:param:` sentinels and collect
// values in order. These two functions do the final replacement pass.

/// Replace `:param:` sentinels with positional placeholders ($1, $2, ...).
fn replace_placeholders(sql: String, formatter: Adapter(v)) -> String {
  let parts = string.split(sql, sqlfmt.placeholder)
  case parts {
    [single] -> single
    [first, ..rest] -> {
      let #(result, _) =
        list.fold(rest, #(first, 0), fn(acc, part) {
          let #(s, i) = acc
          #(s <> formatter.handle_placeholder(i) <> part, i + 1)
        })
      result
    }
    [] -> sql
  }
}

/// Replace `:param:` sentinels with literal formatted values.
fn replace_with_values(
  sql: String,
  values: List(v),
  formatter: Adapter(v),
) -> String {
  let parts = string.split(sql, sqlfmt.placeholder)
  case parts {
    [single] -> single
    [first, ..rest] -> {
      let #(result, _) =
        list.fold(list.zip(rest, values), #(first, Nil), fn(acc, pair) {
          let #(s, _) = acc
          let #(part, val) = pair
          #(s <> formatter.handle_value(val) <> part, Nil)
        })
      result
    }
    [] -> sql
  }
}

// ---- Internal: Clause Append Helpers ----
//
// Each helper takes an accumulator `#(String, List(v))` and appends
// one optional SQL clause, returning the (possibly unchanged) accumulator.
// When the clause data is empty/None/False, the acc passes through unchanged.

fn append_where(
  acc: #(String, List(v)),
  wheres: List(Condition(v)),
  adapter: Adapter(v),
) -> #(String, List(v)) {
  case wheres {
    [] -> acc
    _ -> {
      let #(sql, vals) = acc
      let combined = combine_conditions(wheres)
      let #(ws, wv) = build_condition(combined, adapter)
      #(sqlfmt.where(sql, ws), list.append(vals, wv))
    }
  }
}

fn append_having(
  acc: #(String, List(v)),
  having: List(Condition(v)),
  adapter: Adapter(v),
) -> #(String, List(v)) {
  case having {
    [] -> acc
    _ -> {
      let #(sql, vals) = acc
      let combined = combine_conditions(having)
      let #(hs, hv) = build_condition(combined, adapter)
      #(sqlfmt.having(sql, hs), list.append(vals, hv))
    }
  }
}

fn append_joins(
  acc: #(String, List(v)),
  joins: List(Join(v)),
  adapter: Adapter(v),
) -> #(String, List(v)) {
  list.fold(joins, acc, fn(acc, j) {
    let #(s, v) = acc
    let #(js, jv) = build_join(s, j, adapter)
    #(js, list.append(v, jv))
  })
}

fn append_group_by(
  acc: #(String, List(v)),
  group_by: List(Column),
  fmt: Adapter(v),
) -> #(String, List(v)) {
  case group_by {
    [] -> acc
    cols -> #(sqlfmt.group_by(acc.0, build_columns(cols, fmt)), acc.1)
  }
}

fn append_order_by(
  acc: #(String, List(v)),
  orders: List(OrderBy),
  fmt: Adapter(v),
) -> #(String, List(v)) {
  case orders {
    [] -> acc
    _ -> #(sqlfmt.order_by(acc.0, build_order_by(orders, fmt)), acc.1)
  }
}

fn append_limit(
  acc: #(String, List(v)),
  limit_val: Option(Int),
) -> #(String, List(v)) {
  case limit_val {
    None -> acc
    Some(n) -> #(sqlfmt.limit_int(acc.0, n), acc.1)
  }
}

fn append_offset(
  acc: #(String, List(v)),
  offset_val: Option(Int),
) -> #(String, List(v)) {
  case offset_val {
    None -> acc
    Some(n) -> #(sqlfmt.offset_int(acc.0, n), acc.1)
  }
}

fn append_for_update(
  acc: #(String, List(v)),
  for_update: Bool,
) -> #(String, List(v)) {
  case for_update {
    True -> #(sqlfmt.for_update(acc.0), acc.1)
    False -> acc
  }
}

fn append_returning(
  acc: #(String, List(v)),
  returning: List(Column),
  fmt: Adapter(v),
) -> #(String, List(v)) {
  case returning {
    [] -> acc
    cols -> #(sqlfmt.returning(acc.0, build_columns(cols, fmt)), acc.1)
  }
}

// ---- Internal: Unified Query Building ----
//
// Single-path functions that produce SQL with `:param:` sentinels
// and collect values in order. No idx threading needed.

fn build_query(
  query: QueryBuilder(a, v),
  adapter: Adapter(v),
) -> #(String, List(v)) {
  let #(ctes, recursive) = case query {
    SelectBuilder(ctes:, recursive:, ..) -> #(ctes, recursive)
    InsertBuilder(ctes:, recursive:, ..) -> #(ctes, recursive)
    UpdateBuilder(ctes:, recursive:, ..) -> #(ctes, recursive)
    DeleteBuilder(ctes:, recursive:, ..) -> #(ctes, recursive)
    UnionBuilder(ctes:, recursive:, ..) -> #(ctes, recursive)
    FromTableBuilder(..) -> #([], False)
    FromSubQueryBuilder(..) -> #([], False)
  }

  let #(cte_prefix, cte_vals) = build_ctes(ctes, recursive, adapter)

  let #(body, body_vals) = case query {
    SelectBuilder(
      columns:,
      from:,
      wheres:,
      joins:,
      order_by:,
      limit:,
      offset:,
      group_by:,
      distinct:,
      having:,
      for_update:,
      ..,
    ) ->
      build_select(
        columns,
        from,
        wheres,
        joins,
        order_by,
        limit,
        offset,
        group_by,
        distinct,
        having,
        for_update,
        adapter,
      )
    InsertBuilder(into:, columns:, values:, returning:, on_conflict:, ..) ->
      build_insert(into, columns, values, returning, on_conflict, adapter)
    UpdateBuilder(
      table:,
      sets:,
      wheres:,
      returning:,
      order_by:,
      limit: limit_val,
      offset: offset_val,
      ..,
    ) ->
      build_update(
        table,
        sets,
        wheres,
        returning,
        order_by,
        limit_val,
        offset_val,
        adapter,
      )
    DeleteBuilder(from:, wheres:, returning:, ..) ->
      build_delete(from, wheres, returning, adapter)
    UnionBuilder(selects:, union_type:, ..) ->
      build_combined_select(selects, union_type, adapter)
    FromTableBuilder(..) -> #("", [])
    FromSubQueryBuilder(..) -> #("", [])
  }

  let body_with_suffix = case ctes {
    [] -> body
    _ -> sqlfmt.terminate(body)
  }
  #(cte_prefix <> body_with_suffix, list.append(cte_vals, body_vals))
}

fn build_ctes(
  ctes: List(Cte(v)),
  recursive: Bool,
  adapter: Adapter(v),
) -> #(String, List(v)) {
  case ctes {
    [] -> #("", [])
    _ -> {
      let #(cte_parts, all_vals) =
        list.fold(ctes, #([], []), fn(acc, c) {
          let #(parts, vals) = acc
          let Cte(name:, columns:, query:) = c
          let #(body_sql, body_vals) = build_single_select(query, adapter)
          let col_part = case columns {
            [] -> ""
            cols -> sqlfmt.enclose(sqlfmt.comma_join(cols))
          }
          let part = sqlfmt.cte(name <> col_part, body_sql)
          #(list.append(parts, [part]), list.append(vals, body_vals))
        })
      let ctes_sql = sqlfmt.comma_join(cte_parts)
      let prefix = case recursive {
        True -> sqlfmt.with_recursive(ctes_sql)
        False -> sqlfmt.with_cte(ctes_sql)
      }
      #(prefix <> " ", all_vals)
    }
  }
}

fn build_select(
  columns: List(Column),
  from: FromClause(v),
  wheres: List(Condition(v)),
  joins: List(Join(v)),
  order_by: List(OrderBy),
  limit_val: Option(Int),
  offset_val: Option(Int),
  group_by: List(Column),
  distinct: Bool,
  having: List(Condition(v)),
  for_update: Bool,
  adapter: Adapter(v),
) -> #(String, List(v)) {
  let fmt = adapter
  let cols_sql = build_columns(columns, fmt)
  let select_start = case distinct {
    True -> sqlfmt.select_distinct(cols_sql)
    False -> sqlfmt.select(cols_sql)
  }
  let #(from_sql, from_vals) = case from {
    FromTable(tbl) -> #(build_table(tbl, fmt), [])
    FromSubQuery(query, alias) -> {
      let #(sql, vals) = build_single_select(query, adapter)
      #(
        sqlfmt.alias_as(sqlfmt.enclose(sql), fmt.handle_identifier(alias)),
        vals,
      )
    }
  }

  #(sqlfmt.from(select_start, from_sql), from_vals)
  |> append_joins(joins, adapter)
  |> append_where(wheres, adapter)
  |> append_group_by(group_by, fmt)
  |> append_having(having, adapter)
  |> append_order_by(order_by, fmt)
  |> append_limit(limit_val)
  |> append_offset(offset_val)
  |> append_for_update(for_update)
}

fn build_insert(
  into: Table,
  columns: List(String),
  value_rows: List(List(v)),
  returning: List(Column),
  on_conflict: Option(OnConflict(v)),
  adapter: Adapter(v),
) -> #(String, List(v)) {
  let fmt = adapter
  let cols_sql = columns |> list.map(fmt.handle_identifier) |> sqlfmt.comma_join
  let #(row_strings, all_vals) =
    list.fold(value_rows, #([], []), fn(acc, row) {
      let #(row_strs, vals) = acc
      let row_placeholders = list.map(row, fn(_) { sqlfmt.placeholder })
      #(
        list.append(row_strs, [
          sqlfmt.value_row(sqlfmt.comma_join(row_placeholders)),
        ]),
        list.append(vals, row),
      )
    })
  let values_sql = sqlfmt.comma_join(row_strings)
  let sql =
    into
    |> build_table(fmt)
    |> sqlfmt.insert(columns: cols_sql, values: values_sql)

  // ON CONFLICT
  let #(sql, all_vals) = case on_conflict {
    None -> #(sql, all_vals)
    Some(OnConflict(target:, action:, wheres: conflict_wheres)) -> {
      let sql = sqlfmt.on_conflict(sql, fmt.handle_identifier(target))
      let sql = case action {
        DoNothing -> sqlfmt.do_nothing(sql)
        DoUpdate(sets:) -> {
          let set_strings =
            list.map(sets, fn(pair) {
              let #(col_name, val_expr) = pair
              sqlfmt.eq(fmt.handle_identifier(col_name), val_expr)
            })
          sqlfmt.do_update(sql, sqlfmt.comma_join(set_strings))
        }
      }
      #(sql, all_vals) |> append_where(conflict_wheres, adapter)
    }
  }

  #(sql, all_vals) |> append_returning(returning, fmt)
}

fn build_update(
  tbl: Table,
  sets: List(#(String, Operand(v))),
  wheres: List(Condition(v)),
  returning: List(Column),
  order_by: List(OrderBy),
  limit_val: Option(Int),
  offset_val: Option(Int),
  adapter: Adapter(v),
) -> #(String, List(v)) {
  let fmt = adapter

  let #(set_strings, vals) =
    list.fold(sets, #([], []), fn(acc, pair) {
      let #(ss, vs) = acc
      let #(col_name, set_val) = pair
      case set_val {
        Val(val) -> #(
          list.append(ss, [
            sqlfmt.eq(fmt.handle_identifier(col_name), sqlfmt.placeholder),
          ]),
          list.append(vs, [val]),
        )
        NullVal -> {
          let null_val = adapter.handle_null()
          #(
            list.append(ss, [
              sqlfmt.eq(fmt.handle_identifier(col_name), sqlfmt.placeholder),
            ]),
            list.append(vs, [null_val]),
          )
        }
        SubQuery(query) -> {
          let #(sub_sql, sub_vals) = build_single_select(query, adapter)
          #(
            list.append(ss, [
              sqlfmt.eq(
                fmt.handle_identifier(col_name),
                sqlfmt.enclose(sub_sql),
              ),
            ]),
            list.append(vs, sub_vals),
          )
        }
        _ ->
          panic as "Only Val, NullVal and SubQuery operands are valid in SET clause"
      }
    })

  let sql =
    sqlfmt.update(build_table(tbl, fmt))
    |> sqlfmt.set(sqlfmt.comma_join(set_strings))

  #(sql, vals)
  |> append_where(wheres, adapter)
  |> append_order_by(order_by, fmt)
  |> append_limit(limit_val)
  |> append_offset(offset_val)
  |> append_returning(returning, fmt)
}

fn build_delete(
  from: Table,
  wheres: List(Condition(v)),
  returning: List(Column),
  adapter: Adapter(v),
) -> #(String, List(v)) {
  let fmt = adapter

  #(sqlfmt.delete(from: build_table(from, fmt)), [])
  |> append_where(wheres, adapter)
  |> append_returning(returning, fmt)
}

fn build_operand(operand: Operand(v), adapter: Adapter(v)) -> #(String, List(v)) {
  let fmt = adapter
  case operand {
    Col(column) -> #(build_column(column, fmt), [])
    Val(value) -> #(sqlfmt.placeholder, [value])
    NullVal -> {
      let null_val = adapter.handle_null()
      #(sqlfmt.placeholder, [null_val])
    }
    SubQuery(query) -> {
      let #(sql, vals) = build_single_select(query, adapter)
      #(sqlfmt.subquery(sql), vals)
    }
    AnyQuery(query) -> {
      let #(sql, vals) = build_single_select(query, adapter)
      #(sqlfmt.any(sql), vals)
    }
    AllQuery(query) -> {
      let #(sql, vals) = build_single_select(query, adapter)
      #(sqlfmt.all(sql), vals)
    }
  }
}

fn build_condition(
  condition: Condition(v),
  adapter: Adapter(v),
) -> #(String, List(v)) {
  case condition {
    Equal(left, right) ->
      build_binary_condition(left, sqlfmt.eq, right, adapter)
    NotEqual(left, right) ->
      build_binary_condition(left, sqlfmt.not_eq, right, adapter)
    GreaterThan(left, right) ->
      build_binary_condition(left, sqlfmt.gt, right, adapter)
    LessThan(left, right) ->
      build_binary_condition(left, sqlfmt.lt, right, adapter)
    GreaterThanOrEqual(left, right) ->
      build_binary_condition(left, sqlfmt.gt_eq, right, adapter)
    LessThanOrEqual(left, right) ->
      build_binary_condition(left, sqlfmt.lt_eq, right, adapter)
    Between(operand, low, high) -> {
      let #(os, ov) = build_operand(operand, adapter)
      let #(ls, lv) = build_operand(low, adapter)
      let #(hs, hv) = build_operand(high, adapter)
      #(sqlfmt.between(os, ls, hs), list.flatten([ov, lv, hv]))
    }
    Like(operand, pattern) ->
      build_binary_condition(operand, sqlfmt.like, pattern, adapter)
    NotLike(operand, pattern) ->
      build_binary_condition(operand, sqlfmt.not_like, pattern, adapter)
    In(operand, vals) -> {
      let #(os, ov) = build_operand(operand, adapter)
      let #(val_strings, all_vals) =
        list.fold(vals, #([], []), fn(acc, v) {
          let #(strings, collected) = acc
          let #(vs, vv) = build_operand(v, adapter)
          #(list.append(strings, [vs]), list.append(collected, vv))
        })
      #(
        sqlfmt.in_(os, sqlfmt.comma_join(val_strings)),
        list.append(ov, all_vals),
      )
    }
    IsNull(operand) -> {
      let #(os, ov) = build_operand(operand, adapter)
      #(sqlfmt.is_null(os), ov)
    }
    IsNotNull(operand) -> {
      let #(os, ov) = build_operand(operand, adapter)
      #(sqlfmt.is_not_null(os), ov)
    }
    IsTrue(operand) -> {
      let #(os, ov) = build_operand(operand, adapter)
      #(sqlfmt.is_true(os), ov)
    }
    IsFalse(operand) -> {
      let #(os, ov) = build_operand(operand, adapter)
      #(sqlfmt.is_false(os), ov)
    }
    And(left, right) -> {
      let #(ls, lv) = build_condition(left, adapter)
      let #(rs, rv) = build_condition(right, adapter)
      #(sqlfmt.and_op(ls, rs), list.append(lv, rv))
    }
    Or(left, right) -> {
      let #(ls, lv) = build_condition(left, adapter)
      let #(rs, rv) = build_condition(right, adapter)
      #(sqlfmt.or_op(ls, rs), list.append(lv, rv))
    }
    Not(condition) -> {
      let #(cs, cv) = build_condition(condition, adapter)
      #(sqlfmt.not(cs), cv)
    }
    Exists(query) -> {
      let #(sql, vals) = build_single_select(query, adapter)
      #(sqlfmt.exists(sql), vals)
    }
    Raw(sql:, values:) -> {
      // Rewrite ? placeholders to sentinel markers
      let rewritten = string.replace(sql, "?", sqlfmt.placeholder)
      #(rewritten, values)
    }
  }
}

fn build_binary_condition(
  left: Operand(v),
  op: fn(String, String) -> String,
  right: Operand(v),
  adapter: Adapter(v),
) -> #(String, List(v)) {
  let #(ls, lv) = build_operand(left, adapter)
  let #(rs, rv) = build_operand(right, adapter)
  #(op(ls, rs), list.append(lv, rv))
}

fn build_join(
  sql: String,
  j: Join(v),
  adapter: Adapter(v),
) -> #(String, List(v)) {
  let fmt = adapter
  let #(join_fn, tbl, on_conditions) = case j {
    InnerJoin(table:, on:) -> #(sqlfmt.inner_join, table, on)
    LeftJoin(table:, on:) -> #(sqlfmt.left_join, table, on)
    RightJoin(table:, on:) -> #(sqlfmt.right_join, table, on)
    FullJoin(table:, on:) -> #(sqlfmt.full_join, table, on)
  }
  let combined = combine_conditions(on_conditions)
  let #(on_sql, on_vals) = build_condition(combined, adapter)
  let sql = join_fn(sql, build_table(tbl, fmt)) |> sqlfmt.on(on_sql)
  #(sql, on_vals)
}

fn build_combined_select(
  selects: List(QueryBuilder(Select, v)),
  union_type: UnionType,
  adapter: Adapter(v),
) -> #(String, List(v)) {
  let union_fn = case union_type {
    Union -> sqlfmt.union
    UnionAll -> sqlfmt.union_all
  }

  let #(sql_parts, all_vals) =
    list.fold(selects, #([], []), fn(acc, q) {
      let #(parts, vals) = acc
      let #(sub_sql, sub_vals) = build_single_select(q, adapter)
      #(list.append(parts, [sub_sql]), list.append(vals, sub_vals))
    })

  let sql = case sql_parts {
    [] -> ""
    [first, ..rest] -> list.fold(rest, first, union_fn)
  }
  #(sql, all_vals)
}

fn build_single_select(
  query: QueryBuilder(Select, v),
  adapter: Adapter(v),
) -> #(String, List(v)) {
  case query {
    SelectBuilder(
      columns:,
      from:,
      wheres:,
      joins:,
      order_by:,
      limit:,
      offset:,
      group_by:,
      distinct:,
      having:,
      for_update:,
      ..,
    ) ->
      build_select(
        columns,
        from,
        wheres,
        joins,
        order_by,
        limit,
        offset,
        group_by,
        distinct,
        having,
        for_update,
        adapter,
      )
    UnionBuilder(selects:, union_type:, ..) ->
      build_combined_select(selects, union_type, adapter)
    _ -> #("", [])
  }
}

// ---- Internal: Shared Helpers ----

fn build_column(column: Column, fmt: Adapter(v)) -> String {
  case column {
    Star -> "*"
    Column(table:, name:, alias:, func:) -> {
      // Build the base column reference (possibly table-qualified)
      let col_ref = case name {
        "*" -> {
          case table {
            Some(tbl) -> fmt.handle_identifier(tbl) <> ".*"
            None -> "*"
          }
        }
        _ -> {
          case table {
            Some(tbl) ->
              fmt.handle_identifier(tbl) <> "." <> fmt.handle_identifier(name)
            None -> fmt.handle_identifier(name)
          }
        }
      }
      // Wrap in aggregate function if present
      let col_ref = case func {
        None -> col_ref
        Some(Count) -> sqlfmt.count(col_ref)
        Some(Sum) -> sqlfmt.sum(col_ref)
        Some(Avg) -> sqlfmt.avg(col_ref)
        Some(Max) -> sqlfmt.max(col_ref)
        Some(Min) -> sqlfmt.min(col_ref)
      }
      // Add alias if present
      case alias {
        None -> col_ref
        Some(a) -> sqlfmt.alias_as(col_ref, fmt.handle_identifier(a))
      }
    }
  }
}

fn build_columns(columns: List(Column), fmt: Adapter(v)) -> String {
  columns
  |> list.map(fn(c) { build_column(c, fmt) })
  |> sqlfmt.comma_join
}

fn build_table(tbl: Table, fmt: Adapter(v)) -> String {
  case tbl.alias {
    None -> fmt.handle_identifier(tbl.name)
    Some(a) ->
      sqlfmt.alias_as(fmt.handle_identifier(tbl.name), fmt.handle_identifier(a))
  }
}

fn build_order_by(orders: List(OrderBy), fmt: Adapter(v)) -> String {
  orders
  |> list.map(fn(o) {
    let col_str = build_column(o.column, fmt)
    case o.direction {
      Asc -> sqlfmt.asc(col_str)
      Desc -> sqlfmt.desc(col_str)
    }
  })
  |> sqlfmt.comma_join
}

fn combine_conditions(wheres: List(Condition(v))) -> Condition(v) {
  case wheres {
    [single] -> single
    [first, ..rest] -> list.fold(rest, first, fn(acc, w) { And(acc, w) })
    // This case should never happen when called correctly,
    // but we need to handle it for exhaustiveness.
    [] -> panic as "shouldn't happen"
  }
}
