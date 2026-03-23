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
//// // -> [sql.Bool(True)]
//// ```
////
//// ## Custom adapters
////
//// Use `new_adapter()` with builder functions to configure how queries are
//// rendered for a specific database backend. This controls placeholder style,
//// identifier quoting, and value type mapping.
////
//// ```gleam
//// let mysql_adapter =
////   sql.new_adapter()
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
//// The `Builder(kind, v)` type uses phantom types (`Select`, `Insert`,
//// `Update`, `Delete`, `From`) to restrict which modifier functions can be
//// called. For example, `join` only accepts `Builder(Select, v)`, and
//// `set` only accepts `Builder(Update, v)`. This helps callers avoid
//// building invalid SQL queries. It is possible to call functions that do
//// not modify the provided `Builder`. Passing `Builder(Insert)`
//// to `sql.where` will not modify the query builder. This library is meant
//// to help with building SQL strings, but will not protect callers from
//// creating invalid SQL strings.
////

import based/internal/fmt
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

/// Creates a `Query` from a raw SQL string with no parameters.
pub fn query(sql: String) -> Query(v) {
  Query(sql:, values: [])
}

/// Sets the parameter values on a `Query`.
pub fn params(query: Query(v), values: List(v)) -> Query(v) {
  Query(..query, values:)
}

/// A UTC offset composed of hours and minutes.
///
/// Used with `Timestamptz` to encode a timestamp relative to UTC.
/// Offsets are subtracted from the timestamp during encoding so the
/// result is always a UTC value.
///
/// A positive offset (e.g. `utc_offset(5)`) means local time is
/// ahead of UTC; a negative offset means it is behind.
pub type Offset {
  Offset(hours: Int, minutes: Int)
}

/// Returns an Offset with the provided hours and 0 minutes.
pub fn utc_offset(hours: Int) -> Offset {
  Offset(hours:, minutes: 0)
}

/// Applies some number of minutes to the Offset.
pub fn minutes(offset: Offset, minutes: Int) -> Offset {
  Offset(..offset, minutes:)
}

/// The built-in value type covering common SQL data types.
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

/// Wraps a `Uuid` as a `Value`.
pub fn uuid(uuid: uuid.Uuid) -> Value {
  Uuid(uuid)
}

/// The SQL `NULL` value.
pub const null = Null

/// The SQL boolean `TRUE` value.
pub const true = Bool(True)

/// The SQL boolean `FALSE` value.
pub const false = Bool(False)

/// Wraps a `Bool` as a `Value`.
pub fn bool(bool: Bool) -> Value {
  Bool(bool)
}

/// Wraps an `Int` as a `Value`.
pub fn int(int: Int) -> Value {
  Int(int)
}

/// Wraps a `Float` as a `Value`.
pub fn float(float: Float) -> Value {
  Float(float)
}

/// Wraps a `String` as a `Value`.
pub fn text(text: String) -> Value {
  Text(text)
}

/// Wraps a `BitArray` as a `Value` for binary/bytea columns.
pub fn bytea(bytea: BitArray) -> Value {
  Bytea(bytea)
}

/// Wraps a `calendar.Date` as a `Value`.
pub fn date(date: calendar.Date) -> Value {
  Date(date)
}

/// Wraps a `calendar.TimeOfDay` as a `Value`.
pub fn time(time_of_day: calendar.TimeOfDay) -> Value {
  Time(time_of_day)
}

/// Wraps a date and time as a `Value` for datetime columns.
pub fn datetime(date: calendar.Date, time: calendar.TimeOfDay) -> Value {
  Datetime(date, time)
}

/// Wraps a `timestamp.Timestamp` as a `Value`.
pub fn timestamp(timestamp: timestamp.Timestamp) -> Value {
  Timestamp(timestamp)
}

/// Wraps a `timestamp.Timestamp` and `Offset` as a `Value` for
/// timestamp-with-timezone columns.
pub fn timestamptz(timestamp: timestamp.Timestamp, offset: Offset) -> Value {
  Timestamptz(timestamp, offset)
}

/// Wraps an `interval.Interval` as a `Value`.
pub fn interval(interval: interval.Interval) -> Value {
  Interval(interval)
}

/// Wraps a list of elements as an `Array` value.
///
/// The `of` parameter specifies how to convert each element to a `Value`.
pub fn array(elements: List(a), of kind: fn(a) -> Value) -> Value {
  elements
  |> list.map(kind)
  |> Array
}

/// A SQL table reference, optionally aliased.
pub opaque type Table {
  Table(name: String, alias: Option(String))
}

/// Creates a table reference.
pub fn table(name: String) -> Table {
  Table(name: name, alias: None)
}

/// Sets an alias on a table. Renders as `table AS alias`.
pub fn table_as(table: Table, alias: String) -> Table {
  Table(..table, alias: Some(alias))
}

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
  All
}

/// Creates a plain column reference.
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
pub fn col_for(column: Column, table: String) -> Column {
  case column {
    Column(..) -> Column(..column, table: Some(table))
    All -> All
  }
}

/// Sets an alias on a column. Renders as `column AS alias`.
/// No-ops on wildcard column.
pub fn col_as(column: Column, a: String) -> Column {
  case column {
    Column(..) -> Column(..column, alias: Some(a))
    All -> All
  }
}

/// The `*` wildcard column, for use in `SELECT *`.
pub const star = All

// ---- Adapter ----

/// Database adapter that controls how queries are serialized.
///
/// An adapter defines how placeholders, identifiers, and values are formatted
/// for a specific database backend. Create one with `new_adapter()` and
/// configure it using the `on_*` builder functions.
///
/// ```gleam
/// let my_adapter =
///   sql.new_adapter()
///   |> sql.on_placeholder(with: fn(i) { "$" <> int.to_string(i + 1) })
///   |> sql.on_value(with: my_value_to_string)
///   |> sql.on_null(with: fn() { MyNull })
///   |> sql.on_int(with: fn(i) { MyInt(i) })
///   |> sql.on_text(with: fn(s) { MyText(s) })
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

/// Creates a new adapter with unconfigured value handlers.
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

/// A type-safe row for INSERT statements.
pub opaque type Row(v) {
  Row(column: String, value: v, next: Option(fn() -> Row(v)))
}

/// Adds a column/value pair to a row, with a continuation for the next field.
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

type Operand(v) {
  Col(Column)
  Val(v)
  NullVal
  SubQuery(Builder(Select, v))
  AnyQuery(Builder(Select, v))
  AllQuery(Builder(Select, v))
}

/// A WHERE clause condition.
///
/// Conditions are built using constructor functions like `eq`, `gt`, `like`,
/// `is_null`, etc. They can be combined with `and`, `or`, and `not`. Raw SQL
/// conditions are also supported via `raw`.
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
  Exists(Builder(Select, v))
  Raw(sql: String)
}

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
  builder: Builder(Select, v),
  table table: Table,
  on on: List(Condition(v)),
) -> Builder(Select, v) {
  prepend_join(builder, InnerJoin(table: table, on: on))
}

/// Adds a `LEFT JOIN` clause to a SELECT query.
/// Multiple conditions are combined with AND at render time.
pub fn left_join(
  builder: Builder(Select, v),
  table table: Table,
  on on: List(Condition(v)),
) -> Builder(Select, v) {
  prepend_join(builder, LeftJoin(table: table, on: on))
}

/// Adds a `RIGHT JOIN` clause to a SELECT query.
/// Multiple conditions are combined with AND at render time.
pub fn right_join(
  builder: Builder(Select, v),
  table table: Table,
  on on: List(Condition(v)),
) -> Builder(Select, v) {
  prepend_join(builder, RightJoin(table: table, on: on))
}

/// Adds a `FULL JOIN` clause to a SELECT query.
/// Multiple conditions are combined with AND at render time.
pub fn full_join(
  builder: Builder(Select, v),
  table table: Table,
  on on: List(Condition(v)),
) -> Builder(Select, v) {
  prepend_join(builder, FullJoin(table: table, on: on))
}

fn prepend_join(builder: Builder(Select, v), j: Join(v)) -> Builder(Select, v) {
  case builder {
    SelectBuilder(query:, ctes:, recursive:) -> {
      let joins = list.prepend(query.joins, j)

      SelectQuery(..query, joins:)
      |> SelectBuilder(ctes:, recursive:)
    }
    _ -> builder
  }
}

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

/// The action to take when an INSERT conflict occurs.
///
/// - `DoNothing` ignores the conflicting row (`ON CONFLICT ... DO NOTHING`)
/// - `DoUpdate(sets:)` updates specified columns (`ON CONFLICT ... DO UPDATE SET ...`).
///   Each tuple is `#(column_name, expression_string)`.
pub type ConflictAction(v) {
  DoNothing
  DoUpdate(sets: List(#(String, String)))
}

type OnConflict(v) {
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
pub opaque type Kind(a, v) {
  Kind(to_operand: fn(a) -> Operand(v))
}

/// Phantom type for SELECT queries.
pub type Select

/// Phantom type for INSERT queries.
pub type Insert

/// Phantom type for UPDATE queries.
pub type Update

/// Phantom type for DELETE queries.
pub type Delete

/// Phatom type indicating a sub query
pub type Subquery

type UnionType {
  Union
  UnionAll
}

pub opaque type From(a, v) {
  FromTable(table: Table)
  FromSubQuery(builder: Builder(Select, v), alias: String)
}

type TableOrSubquery(v) {
  SelectFromTable(table: Table)
  SelectFromSubQuery(builder: Builder(Select, v), alias: String)
}

type SelectQuery(v) {
  SelectQuery(
    columns: List(Column),
    from: TableOrSubquery(v),
    wheres: List(List(Condition(v))),
    joins: List(Join(v)),
    order_by: List(OrderBy),
    limit: Option(Int),
    offset: Option(Int),
    group_by: List(Column),
    distinct: Bool,
    having: List(List(Condition(v))),
    for_update: Bool,
  )
}

type InsertQuery(v) {
  InsertQuery(
    into: Table,
    columns: List(String),
    values: List(List(v)),
    returning: List(Column),
    on_conflict: Option(OnConflict(v)),
  )
}

type UpdateQuery(v) {
  UpdateQuery(
    table: Table,
    sets: List(#(String, Operand(v))),
    wheres: List(List(Condition(v))),
    returning: List(Column),
    order_by: List(OrderBy),
    limit: Option(Int),
    offset: Option(Int),
  )
}

type DeleteQuery(v) {
  DeleteQuery(
    from: Table,
    wheres: List(List(Condition(v))),
    returning: List(Column),
  )
}

type UnionQuery(v) {
  UnionQuery(selects: List(Builder(Select, v)), union_type: UnionType)
}

/// The main query builder type, parameterized by a phantom `kind` type
/// (`Select`, `Insert`, `Update`, `Delete`, or `From(a)`) and a value type `v`.
///
/// The phantom type restricts which modifier functions can be applied,
/// preventing some invalid combinations at compile time.
pub opaque type Builder(kind, v) {
  SelectBuilder(query: SelectQuery(v), ctes: List(Cte(v)), recursive: Bool)
  InsertBuilder(query: InsertQuery(v), ctes: List(Cte(v)), recursive: Bool)
  UpdateBuilder(query: UpdateQuery(v), ctes: List(Cte(v)), recursive: Bool)
  DeleteBuilder(query: DeleteQuery(v), ctes: List(Cte(v)), recursive: Bool)
  UnionBuilder(query: UnionQuery(v), ctes: List(Cte(v)), recursive: Bool)
}

/// A Common Table Expression (CTE) for use with `WITH` clauses.
///
/// Created with `cte` and optionally refined with `cte_columns`.
pub opaque type Cte(v) {
  Cte(name: String, columns: List(String), builder: Builder(Select, v))
}

/// Creates an equality condition.
pub fn eq(column: Column, input: a, of kind: Kind(a, v)) -> Condition(v) {
  Equal(Col(column), kind.to_operand(input))
}

/// Creates an inequality condition.
pub fn not_eq(column: Column, input: a, of kind: Kind(a, v)) -> Condition(v) {
  NotEqual(Col(column), kind.to_operand(input))
}

/// Creates a greater-than condition.
pub fn gt(column: Column, input: a, of kind: Kind(a, v)) -> Condition(v) {
  GreaterThan(Col(column), kind.to_operand(input))
}

/// Creates a less-than condition.
pub fn lt(column: Column, input: a, of kind: Kind(a, v)) -> Condition(v) {
  LessThan(Col(column), kind.to_operand(input))
}

/// Creates a greater-than-or-equal condition.
pub fn gt_eq(column: Column, input: a, of kind: Kind(a, v)) -> Condition(v) {
  GreaterThanOrEqual(Col(column), kind.to_operand(input))
}

/// Creates a less-than-or-equal condition.
pub fn lt_eq(column: Column, input: a, of kind: Kind(a, v)) -> Condition(v) {
  LessThanOrEqual(Col(column), kind.to_operand(input))
}

/// Creates a BETWEEN condition.
pub fn between(
  column: Column,
  low: a,
  high: a,
  of kind: Kind(a, v),
) -> Condition(v) {
  Between(Col(column), kind.to_operand(low), kind.to_operand(high))
}

/// Creates a LIKE condition.
pub fn like(column: Column, input: a, of kind: Kind(a, v)) -> Condition(v) {
  Like(Col(column), kind.to_operand(input))
}

/// Creates a NOT LIKE condition.
pub fn not_like(column: Column, input: a, of kind: Kind(a, v)) -> Condition(v) {
  NotLike(Col(column), kind.to_operand(input))
}

/// Creates an IN condition.
pub fn in(column: Column, values: List(a), of kind: Kind(a, v)) -> Condition(v) {
  In(Col(column), list.map(values, kind.to_operand))
}

/// Creates an IS NULL condition.
pub fn is_null(column: Column) -> Condition(v) {
  IsNull(Col(column))
}

/// Creates an IS NOT NULL condition.
pub fn is_not_null(column: Column) -> Condition(v) {
  IsNotNull(Col(column))
}

/// Creates an IS TRUE condition.
pub fn is_true(column: Column) -> Condition(v) {
  IsTrue(Col(column))
}

/// Creates an IS FALSE condition.
pub fn is_false(column: Column) -> Condition(v) {
  IsFalse(Col(column))
}

/// Combines two conditions with OR.
pub fn or(left: Condition(v), right: Condition(v)) -> Condition(v) {
  Or(left, right)
}

/// Combines two conditions with AND.
pub fn and(left: Condition(v), right: Condition(v)) -> Condition(v) {
  And(left, right)
}

/// Negates a condition.
pub fn not(condition: Condition(v)) -> Condition(v) {
  Not(condition)
}

/// Creates an EXISTS condition.
pub fn exists(builder: Builder(Select, v)) -> Condition(v) {
  Exists(builder)
}

/// Creates a raw SQL condition without parameterized values.
pub fn raw(sql: String) -> Condition(v) {
  Raw(sql:)
}

/// Starts building a query from a table. This is the entry point for SELECT
/// and DELETE queries.
///
/// Pass into `select` or `delete` to choose the query kind.
pub fn from(table: Table) -> From(Table, v) {
  FromTable(table:)
}

/// Converts a `From` into a SELECT query with the given columns.
pub fn select(from: From(a, v), columns: List(Column)) -> Builder(Select, v) {
  let from = case from {
    FromTable(table:) -> SelectFromTable(table:)
    FromSubQuery(builder:, alias:) -> SelectFromSubQuery(builder:, alias:)
  }

  SelectQuery(
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
  )
  |> SelectBuilder(ctes: [], recursive: False)
}

/// Creates a new INSERT query builder for the given table.
pub fn insert(into tbl: Table) -> Builder(Insert, v) {
  InsertQuery(
    into: tbl,
    columns: [],
    values: [],
    returning: [],
    on_conflict: None,
  )
  |> InsertBuilder(ctes: [], recursive: False)
}

/// Creates a new UPDATE query builder for the given table.
pub fn update(table tbl: Table) -> Builder(Update, v) {
  UpdateQuery(
    table: tbl,
    sets: [],
    wheres: [],
    returning: [],
    order_by: [],
    limit: None,
    offset: None,
  )
  |> UpdateBuilder(ctes: [], recursive: False)
}

/// Converts a `From` into a DELETE query.
pub fn delete(from: From(Table, v)) -> Builder(Delete, v) {
  case from {
    FromTable(table:) -> {
      DeleteQuery(from: table, wheres: [], returning: [])
      |> DeleteBuilder(ctes: [], recursive: False)
    }
    _ -> panic as "Not possible"
  }
}

/// Creates a `From` that selects from a subquery instead of a table.
pub fn from_subquery(
  builder: Builder(Select, v),
  alias alias: String,
) -> From(Subquery, v) {
  FromSubQuery(builder:, alias:)
}

/// Adds a WHERE condition to the query. Multiple `where` calls are combined
/// with AND.
///
/// Applies to SELECT, UPDATE, and DELETE queries. No-ops on other builder types.
pub fn where(
  builder: Builder(a, v),
  conditions: List(Condition(v)),
) -> Builder(a, v) {
  case builder {
    SelectBuilder(query:, ctes:, recursive:) ->
      SelectQuery(..query, wheres: list.prepend(query.wheres, conditions))
      |> SelectBuilder(ctes:, recursive:)

    UpdateBuilder(query:, ctes:, recursive:) ->
      UpdateQuery(..query, wheres: list.prepend(query.wheres, conditions))
      |> UpdateBuilder(ctes:, recursive:)
    DeleteBuilder(query:, ctes:, recursive:) ->
      DeleteQuery(..query, wheres: list.prepend(query.wheres, conditions))
      |> DeleteBuilder(ctes:, recursive:)
    _ -> builder
  }
}

/// Adds a RETURNING clause. Applies to INSERT, UPDATE, and DELETE queries.
pub fn returning(builder: Builder(a, v), columns: List(Column)) -> Builder(a, v) {
  case builder {
    InsertBuilder(query:, ctes:, recursive:) ->
      InsertQuery(..query, returning: columns)
      |> InsertBuilder(ctes:, recursive:)
    UpdateBuilder(query:, ctes:, recursive:) ->
      UpdateQuery(..query, returning: columns)
      |> UpdateBuilder(ctes:, recursive:)
    DeleteBuilder(query:, ctes:, recursive:) ->
      DeleteQuery(..query, returning: columns)
      |> DeleteBuilder(ctes:, recursive:)
    _ -> builder
  }
}

/// Adds an ORDER BY clause. Can be called multiple times to sort by
/// multiple columns. Applies to SELECT and UPDATE queries.
pub fn order_by(
  builder: Builder(a, v),
  column: Column,
  direction: Order,
) -> Builder(a, v) {
  case builder {
    SelectBuilder(query:, ctes:, recursive:) ->
      SelectQuery(
        ..query,
        order_by: list.prepend(query.order_by, OrderBy(column, direction)),
      )
      |> SelectBuilder(ctes:, recursive:)
    UpdateBuilder(query:, ctes:, recursive:) ->
      UpdateQuery(
        ..query,
        order_by: list.prepend(query.order_by, OrderBy(column, direction)),
      )
      |> UpdateBuilder(ctes:, recursive:)
    _ -> builder
  }
}

/// Adds a GROUP BY clause to a SELECT query.
pub fn group_by(
  builder: Builder(Select, v),
  columns: List(Column),
) -> Builder(Select, v) {
  case builder {
    SelectBuilder(query:, ctes:, recursive:) ->
      SelectQuery(..query, group_by: list.append(query.group_by, columns))
      |> SelectBuilder(ctes:, recursive:)
    _ -> builder
  }
}

/// Adds a LIMIT clause. Applies to SELECT and UPDATE queries.
pub fn limit(builder: Builder(a, v), n: Int) -> Builder(a, v) {
  case builder {
    SelectBuilder(query:, ctes:, recursive:) ->
      SelectQuery(..query, limit: Some(n)) |> SelectBuilder(ctes:, recursive:)
    UpdateBuilder(query:, ctes:, recursive:) ->
      UpdateQuery(..query, limit: Some(n)) |> UpdateBuilder(ctes:, recursive:)
    _ -> builder
  }
}

/// Adds an OFFSET clause. Applies to SELECT and UPDATE queries.
pub fn offset(builder: Builder(a, v), n: Int) -> Builder(a, v) {
  case builder {
    SelectBuilder(query:, ctes:, recursive:) ->
      SelectQuery(..query, offset: Some(n)) |> SelectBuilder(ctes:, recursive:)
    UpdateBuilder(query:, ctes:, recursive:) ->
      UpdateQuery(..query, offset: Some(n)) |> UpdateBuilder(ctes:, recursive:)
    _ -> builder
  }
}

/// Adds `SELECT DISTINCT` to a SELECT query.
pub fn distinct(builder: Builder(Select, v)) -> Builder(Select, v) {
  case builder {
    SelectBuilder(query:, ctes:, recursive:) ->
      SelectQuery(..query, distinct: True) |> SelectBuilder(ctes:, recursive:)
    _ -> builder
  }
}

/// Adds a HAVING clause to a SELECT query. Used with GROUP BY to filter
/// aggregated results. Multiple `having` calls are combined with AND.
pub fn having(
  builder: Builder(Select, v),
  conditions: List(Condition(v)),
) -> Builder(Select, v) {
  case builder {
    SelectBuilder(query:, ctes:, recursive:) ->
      SelectQuery(..query, having: list.prepend(query.having, conditions))
      |> SelectBuilder(ctes:, recursive:)
    _ -> builder
  }
}

/// Adds `FOR UPDATE` to a SELECT query for row-level locking.
pub fn for_update(builder: Builder(Select, v)) -> Builder(Select, v) {
  case builder {
    SelectBuilder(query:, ctes:, recursive:) ->
      SelectQuery(..query, for_update: True) |> SelectBuilder(ctes:, recursive:)
    _ -> builder
  }
}

/// Sets the rows to insert. Columns are extracted from the first row.
/// Replaces any previously set values.
pub fn values(
  builder: Builder(Insert, v),
  rows: List(Row(v)),
) -> Builder(Insert, v) {
  case builder {
    InsertBuilder(query:, ctes:, recursive:) -> {
      let extracted = list.map(rows, fn(row) { row_to_columns_and_values(row) })
      let columns = case extracted {
        [#(cols, _), ..] -> cols
        [] -> []
      }
      let value_rows = list.map(extracted, fn(pair) { pair.1 })
      InsertQuery(..query, columns: columns, values: value_rows)
      |> InsertBuilder(ctes:, recursive:)
    }
    _ -> builder
  }
}

/// Adds an ON CONFLICT clause to an INSERT query (upsert).
pub fn on_conflict(
  builder: Builder(Insert, v),
  target target: String,
  action action: ConflictAction(v),
  where wheres: List(Condition(v)),
) -> Builder(Insert, v) {
  case builder {
    InsertBuilder(query:, ctes:, recursive:) ->
      InsertQuery(
        ..query,
        on_conflict: Some(OnConflict(
          target: target,
          action: action,
          wheres: wheres,
        )),
      )
      |> InsertBuilder(ctes:, recursive:)
    _ -> builder
  }
}

/// Kind that treats the input as a parameterized value.
pub const value = Kind(to_operand: value_to_operand)

fn value_to_operand(v) -> Operand(v) {
  Val(v)
}

/// Kind that treats the input as a subquery for scalar comparisons.
pub const subquery = Kind(to_operand: subquery_to_operand)

fn subquery_to_operand(q) -> Operand(v) {
  SubQuery(q)
}

/// Kind that treats the input as a column reference for column-to-column
/// comparisons.
pub const column = Kind(to_operand: column_to_operand)

fn column_to_operand(c) -> Operand(v) {
  Col(c)
}

/// Kind that wraps a subquery with `ANY(...)`.
pub const any = Kind(to_operand: any_to_operand)

fn any_to_operand(q) -> Operand(v) {
  AnyQuery(q)
}

/// Kind that wraps a subquery with `ALL(...)`.
pub const all = Kind(to_operand: all_to_operand)

fn all_to_operand(q) -> Operand(v) {
  AllQuery(q)
}

/// Wraps a kind to accept `Option(a)`, mapping `None` to SQL NULL.
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
pub fn list(of map: fn(a) -> v) -> Kind(a, v) {
  Kind(to_operand: fn(x) { Val(map(x)) })
}

/// Sets a column to a value in an UPDATE query. Can be called multiple times.
pub fn set(
  builder: Builder(Update, v),
  column: String,
  input: a,
  of kind: Kind(a, v),
) -> Builder(Update, v) {
  case builder {
    UpdateBuilder(query:, ctes:, recursive:) ->
      UpdateQuery(
        ..query,
        sets: list.prepend(query.sets, #(column, kind.to_operand(input))),
      )
      |> UpdateBuilder(ctes:, recursive:)
    _ -> builder
  }
}

/// Creates a Common Table Expression (CTE).
pub fn cte(name name: String, query builder: Builder(Select, v)) -> Cte(v) {
  Cte(name: name, columns: [], builder: builder)
}

/// Sets explicit column names on a CTE. Renders as `name(col1, col2) AS (...)`.
pub fn cte_columns(cte c: Cte(v), columns cols: List(String)) -> Cte(v) {
  Cte(..c, columns: cols)
}

/// Attaches CTEs to a query as a `WITH` clause.
pub fn with(builder: Builder(a, v), ctes ctes: List(Cte(v))) -> Builder(a, v) {
  case builder {
    SelectBuilder(..) -> SelectBuilder(..builder, ctes:)
    InsertBuilder(..) -> InsertBuilder(..builder, ctes:)
    UpdateBuilder(..) -> UpdateBuilder(..builder, ctes: ctes)
    DeleteBuilder(..) -> DeleteBuilder(..builder, ctes: ctes)
    UnionBuilder(..) -> UnionBuilder(..builder, ctes: ctes)
  }
}

/// Marks the query's WITH clause as `WITH RECURSIVE`.
pub fn recursive(builder: Builder(a, v)) -> Builder(a, v) {
  case builder {
    SelectBuilder(..) -> SelectBuilder(..builder, recursive: True)
    InsertBuilder(..) -> InsertBuilder(..builder, recursive: True)
    UpdateBuilder(..) -> UpdateBuilder(..builder, recursive: True)
    DeleteBuilder(..) -> DeleteBuilder(..builder, recursive: True)
    UnionBuilder(..) -> UnionBuilder(..builder, recursive: True)
  }
}

/// Combines two SELECT queries with `UNION`.
///
/// Can be chained to union multiple queries.
pub fn union(
  builder: Builder(Select, v),
  other: Builder(Select, v),
) -> Builder(Select, v) {
  case builder {
    UnionBuilder(
      query: UnionQuery(selects:, union_type: Union),
      ctes:,
      recursive:,
    ) ->
      UnionQuery(selects: list.prepend(selects, other), union_type: Union)
      |> UnionBuilder(ctes:, recursive:)
    _ ->
      UnionQuery(selects: [other, builder], union_type: Union)
      |> UnionBuilder(ctes: [], recursive: False)
  }
}

/// Combines two SELECT queries with `UNION ALL`.
pub fn union_all(
  builder: Builder(Select, v),
  other: Builder(Select, v),
) -> Builder(Select, v) {
  case builder {
    UnionBuilder(
      query: UnionQuery(selects:, union_type: UnionAll),
      ctes:,
      recursive:,
    ) ->
      UnionQuery(selects: list.prepend(selects, other), union_type: UnionAll)
      |> UnionBuilder(ctes:, recursive:)
    _ ->
      UnionQuery(selects: [other, builder], union_type: UnionAll)
      |> UnionBuilder(ctes: [], recursive: False)
  }
}

/// Serializes a query builder into a `Query(v)` with parameterized placeholders.
///
/// Returns a `Query` record with `.sql` containing the SQL string with
/// placeholders and `.values` containing the parameter values in order.
pub fn to_query(builder: Builder(a, v), adapter: Adapter(v)) -> Query(v) {
  let SqlBuilder(sql, values) = build_query(builder, adapter)
  let sql = replace_placeholders(sql, adapter)
  let values = values |> list.reverse |> list.flatten

  Query(sql:, values:)
}

/// Serializes a query builder into a plain SQL string with values inlined.
///
/// Values are formatted using the adapter's `on_value` handler. This is useful
/// for debugging and logging — use `to_query` for actual database execution.
pub fn to_string(builder: Builder(a, v), adapter: Adapter(v)) -> String {
  let SqlBuilder(sql, values) = build_query(builder, adapter)
  let values = values |> list.reverse |> list.flatten

  replace_with_values(sql, values, adapter)
}

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

// All build_* functions produce SQL with `:param:` sentinels and collect
// values in order. These two functions do the final replacement pass.

/// Replace `:param:` sentinels with positional placeholders ($1, $2, ...).
fn replace_placeholders(sql: String, formatter: Adapter(v)) -> String {
  let parts = string.split(sql, fmt.placeholder)
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
  let parts = string.split(sql, fmt.placeholder)
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

type SqlBuilder(v) {
  SqlBuilder(sql: String, values: List(List(v)))
}

fn append_where(
  builder: SqlBuilder(v),
  wheres: List(List(Condition(v))),
  adapter: Adapter(v),
) -> SqlBuilder(v) {
  let wheres =
    wheres
    |> list.reverse
    |> list.flatten

  case wheres {
    [] -> builder
    [where] -> {
      let #(ws, wv) = build_condition(where, adapter)

      builder.sql
      |> fmt.where(ws)
      |> SqlBuilder(list.prepend(builder.values, list.flatten(wv)))
    }
    [first, ..rest] -> {
      let #(ws, wv) =
        rest
        |> list.fold(first, And)
        |> build_condition(adapter)

      builder.sql
      |> fmt.where(ws)
      |> SqlBuilder(list.prepend(builder.values, list.flatten(wv)))
    }
  }
}

fn append_having(
  builder: SqlBuilder(v),
  having: List(List(Condition(v))),
  adapter: Adapter(v),
) -> SqlBuilder(v) {
  let having =
    having
    |> list.reverse
    |> list.flatten

  case having {
    [] -> builder
    [single] -> {
      let #(hs, hv) = build_condition(single, adapter)

      builder.sql
      |> fmt.having(hs)
      |> SqlBuilder(list.prepend(builder.values, list.flatten(hv)))
    }
    [first, ..rest] -> {
      let #(hs, hv) =
        rest
        |> list.fold(first, And)
        |> build_condition(adapter)

      builder.sql
      |> fmt.having(hs)
      |> SqlBuilder(list.prepend(builder.values, list.flatten(hv)))
    }
  }
}

fn append_joins(
  builder: SqlBuilder(v),
  joins: List(Join(v)),
  adapter: Adapter(v),
) -> SqlBuilder(v) {
  joins
  |> list.reverse
  |> list.fold(builder, fn(builder1, join) {
    build_join(builder1, join, adapter)
  })
}

fn append_group_by(
  builder: SqlBuilder(v),
  group_by: List(Column),
  adapter: Adapter(v),
) -> SqlBuilder(v) {
  case group_by {
    [] -> builder
    cols -> {
      builder.sql
      |> fmt.group_by(build_columns(cols, adapter))
      |> SqlBuilder(builder.values)
    }
  }
}

fn append_order_by(
  builder: SqlBuilder(v),
  orders: List(OrderBy),
  adapter: Adapter(v),
) -> SqlBuilder(v) {
  case orders {
    [] -> builder
    _ -> {
      let orders = list.reverse(orders)

      builder.sql
      |> fmt.order_by(build_order_by(orders, adapter))
      |> SqlBuilder(builder.values)
    }
  }
}

fn append_limit(builder: SqlBuilder(v), limit_val: Option(Int)) -> SqlBuilder(v) {
  case limit_val {
    None -> builder
    Some(n) -> {
      builder.sql
      |> fmt.limit(n)
      |> SqlBuilder(builder.values)
    }
  }
}

fn append_offset(
  builder: SqlBuilder(v),
  offset_val: Option(Int),
) -> SqlBuilder(v) {
  case offset_val {
    None -> builder
    Some(n) -> {
      builder.sql
      |> fmt.offset(n)
      |> SqlBuilder(builder.values)
    }
  }
}

fn append_for_update(builder: SqlBuilder(v), for_update: Bool) -> SqlBuilder(v) {
  case for_update {
    True -> {
      builder.sql
      |> fmt.for_update
      |> SqlBuilder(builder.values)
    }
    False -> builder
  }
}

fn append_returning(
  builder: SqlBuilder(v),
  returning: List(Column),
  adapter: Adapter(v),
) -> SqlBuilder(v) {
  case returning {
    [] -> builder
    cols -> {
      builder.sql
      |> fmt.returning(build_columns(cols, adapter))
      |> SqlBuilder(builder.values)
    }
  }
}

fn build_query(builder: Builder(a, v), adapter: Adapter(v)) -> SqlBuilder(v) {
  case builder {
    SelectBuilder(query:, ctes:, recursive:) -> {
      query
      |> build_select(adapter)
      |> apply_ctes(ctes, recursive, adapter)
    }
    InsertBuilder(query:, ctes:, recursive:) -> {
      query
      |> build_insert(adapter)
      |> apply_ctes(ctes, recursive, adapter)
    }
    UpdateBuilder(query:, ctes:, recursive:) -> {
      query
      |> build_update(adapter)
      |> apply_ctes(ctes, recursive, adapter)
    }
    DeleteBuilder(query:, ctes:, recursive:) -> {
      query
      |> build_delete(adapter)
      |> apply_ctes(ctes, recursive, adapter)
    }
    UnionBuilder(query:, ctes:, recursive:) -> {
      query
      |> build_union(adapter)
      |> apply_ctes(ctes, recursive, adapter)
    }
  }
}

fn apply_ctes(
  builder: SqlBuilder(v),
  ctes: List(Cte(v)),
  recursive: Bool,
  adapter: Adapter(v),
) -> SqlBuilder(v) {
  let cte_builder = build_ctes(ctes, recursive, adapter)

  let body_with_suffix = case ctes {
    [] -> builder.sql
    _ -> fmt.terminate(builder.sql)
  }

  SqlBuilder(
    sql: cte_builder.sql <> body_with_suffix,
    values: list.append(builder.values, cte_builder.values),
  )
}

fn build_ctes(
  ctes: List(Cte(v)),
  recursive: Bool,
  adapter: Adapter(v),
) -> SqlBuilder(v) {
  case ctes {
    [] -> SqlBuilder("", [])
    _ -> {
      let #(cte_parts, all_vals) = {
        use #(parts, vals), cte <- list.fold(ctes, #([], []))

        let sql_builder = build_single_select(cte.builder, adapter)
        let col_part = case cte.columns {
          [] -> ""
          cols -> fmt.enclose(string.join(cols, ", "))
        }

        let values = list.prepend(vals, sql_builder.values)

        let parts =
          cte.name
          |> string.append(col_part)
          |> fmt.cte(sql_builder.sql)
          |> list.prepend(parts, _)

        #(parts, values)
      }

      let ctes_sql =
        cte_parts
        |> list.reverse
        |> string.join(", ")

      let prefix = case recursive {
        True -> fmt.with_recursive(ctes_sql)
        False -> fmt.with_cte(ctes_sql)
      }
      SqlBuilder(prefix <> " ", list.flatten(all_vals))
    }
  }
}

fn build_select(query: SelectQuery(v), adapter: Adapter(v)) -> SqlBuilder(v) {
  let select_fn = case query.distinct {
    True -> fmt.select_distinct
    False -> fmt.select
  }

  let select_start =
    query.columns
    |> build_columns(adapter)
    |> select_fn

  let builder = case query.from {
    SelectFromTable(table) -> {
      table
      |> build_table(adapter)
      |> SqlBuilder([])
    }
    SelectFromSubQuery(query, alias) -> {
      let builder = build_single_select(query, adapter)

      builder.sql
      |> fmt.enclose
      |> fmt.alias_as(adapter.handle_identifier(alias))
      |> SqlBuilder(builder.values)
    }
  }

  fmt.from(select_start, builder.sql)
  |> SqlBuilder(builder.values)
  |> append_joins(query.joins, adapter)
  |> append_where(query.wheres, adapter)
  |> append_group_by(query.group_by, adapter)
  |> append_having(query.having, adapter)
  |> append_order_by(query.order_by, adapter)
  |> append_limit(query.limit)
  |> append_offset(query.offset)
  |> append_for_update(query.for_update)
}

fn build_insert(query: InsertQuery(v), adapter: Adapter(v)) -> SqlBuilder(v) {
  let #(placeholders, values) = {
    use #(rows, vals), row <- list.fold(query.values, #([], []))

    let row_placeholders =
      row
      |> list.map(fn(_) { fmt.placeholder })
      |> string.join(", ")
      |> fmt.enclose

    #(list.prepend(rows, row_placeholders), list.prepend(vals, row))
  }

  let columns = list.map(query.columns, adapter.handle_identifier)

  let sql =
    query.into
    |> build_table(adapter)
    |> fmt.insert(columns:, values: placeholders)

  SqlBuilder(sql:, values:)
  |> append_on_conflict(query.on_conflict, adapter)
  |> append_returning(query.returning, adapter)
}

fn append_on_conflict(
  builder: SqlBuilder(v),
  on_conflict: Option(OnConflict(v)),
  adapter: Adapter(v),
) -> SqlBuilder(v) {
  case on_conflict {
    None -> builder
    Some(OnConflict(target:, action:, wheres: conflict_wheres)) -> {
      let sql =
        builder.sql
        |> fmt.on_conflict(adapter.handle_identifier(target))

      case action {
        DoNothing -> fmt.do_nothing(sql)
        DoUpdate(sets:) -> {
          sets
          |> list.map(fn(pair) {
            let #(col_name, val_expr) = pair
            fmt.eq(adapter.handle_identifier(col_name), val_expr)
          })
          |> fmt.do_update(sql, _)
        }
      }
      |> SqlBuilder(builder.values)
      |> append_where([conflict_wheres], adapter)
    }
  }
}

fn build_update(query: UpdateQuery(v), adapter: Adapter(v)) -> SqlBuilder(v) {
  query.table
  |> build_table(adapter)
  |> fmt.update
  |> SqlBuilder([])
  |> append_sets(query.sets, adapter)
  |> append_where(query.wheres, adapter)
  |> append_order_by(query.order_by, adapter)
  |> append_limit(query.limit)
  |> append_offset(query.offset)
  |> append_returning(query.returning, adapter)
}

fn append_sets(
  builder: SqlBuilder(v),
  sets: List(#(String, Operand(v))),
  adapter: Adapter(v),
) -> SqlBuilder(v) {
  let #(sets_sql, values) = {
    use #(sql, values), #(column, operand) <- list.fold(sets, #([], []))

    let #(set, operand_values) = build_operand(operand, adapter)
    let values = list.prepend(values, operand_values)

    let sql =
      column
      |> adapter.handle_identifier
      |> fmt.eq(set)
      |> list.prepend(sql, _)

    #(sql, values)
  }

  let sets_sql = string.join(sets_sql, ", ")
  let values = values |> list.flatten |> list.flatten

  builder.sql
  |> fmt.set(sets_sql)
  |> SqlBuilder(list.prepend(builder.values, values))
}

fn build_delete(query: DeleteQuery(v), adapter: Adapter(v)) -> SqlBuilder(v) {
  query.from
  |> build_table(adapter)
  |> fmt.delete
  |> SqlBuilder([])
  |> append_where(query.wheres, adapter)
  |> append_returning(query.returning, adapter)
}

fn build_operand(
  operand: Operand(v),
  adapter: Adapter(v),
) -> #(String, List(List(v))) {
  case operand {
    Col(column) -> #(build_column(column, adapter), [])
    Val(value) -> #(fmt.placeholder, [[value]])
    NullVal -> #(fmt.placeholder, [[adapter.handle_null()]])
    SubQuery(query) -> {
      let SqlBuilder(sql, vals) = build_single_select(query, adapter)
      #(fmt.subquery(sql), vals)
    }
    AnyQuery(query) -> {
      let SqlBuilder(sql, vals) = build_single_select(query, adapter)
      #(fmt.any(sql), vals)
    }
    AllQuery(query) -> {
      let SqlBuilder(sql, vals) = build_single_select(query, adapter)
      #(fmt.all(sql), vals)
    }
  }
}

fn build_condition(
  condition: Condition(v),
  adapter: Adapter(v),
) -> #(String, List(List(v))) {
  case condition {
    Equal(left, right) -> build_binary_condition(left, fmt.eq, right, adapter)
    NotEqual(left, right) ->
      build_binary_condition(left, fmt.not_eq, right, adapter)
    GreaterThan(left, right) ->
      build_binary_condition(left, fmt.gt, right, adapter)
    LessThan(left, right) ->
      build_binary_condition(left, fmt.lt, right, adapter)
    GreaterThanOrEqual(left, right) ->
      build_binary_condition(left, fmt.gt_eq, right, adapter)
    LessThanOrEqual(left, right) ->
      build_binary_condition(left, fmt.lt_eq, right, adapter)
    Between(operand, low, high) -> {
      let #(os, ov) = build_operand(operand, adapter)
      let #(ls, lv) = build_operand(low, adapter)
      let #(hs, hv) = build_operand(high, adapter)
      #(fmt.between(os, ls, hs), list.flatten([ov, lv, hv]))
    }
    Like(operand, pattern) ->
      build_binary_condition(operand, fmt.like, pattern, adapter)
    NotLike(operand, pattern) ->
      build_binary_condition(operand, fmt.not_like, pattern, adapter)
    In(operand, vals) -> {
      let #(os, ov) = build_operand(operand, adapter)
      let #(val_strings, all_vals) =
        list.fold(vals, #([], []), fn(acc, v) {
          let #(strings, collected) = acc
          let #(vs, vv) = build_operand(v, adapter)
          #(list.prepend(strings, vs), list.append(collected, vv))
        })

      let val_strings =
        val_strings
        |> list.reverse
        |> string.join(", ")

      #(fmt.in_(os, val_strings), list.append(ov, all_vals))
    }
    IsNull(operand) -> {
      let #(os, ov) = build_operand(operand, adapter)
      #(fmt.is_null(os), ov)
    }
    IsNotNull(operand) -> {
      let #(os, ov) = build_operand(operand, adapter)
      #(fmt.is_not_null(os), ov)
    }
    IsTrue(operand) -> {
      let #(os, ov) = build_operand(operand, adapter)
      #(fmt.is_true(os), ov)
    }
    IsFalse(operand) -> {
      let #(os, ov) = build_operand(operand, adapter)
      #(fmt.is_false(os), ov)
    }
    And(left, right) -> {
      let #(ls, lv) = build_condition(left, adapter)
      let #(rs, rv) = build_condition(right, adapter)
      #(fmt.and_op(ls, rs), list.append(lv, rv))
    }
    Or(left, right) -> {
      let #(ls, lv) = build_condition(left, adapter)
      let #(rs, rv) = build_condition(right, adapter)
      #(fmt.or_op(ls, rs), list.append(lv, rv))
    }
    Not(condition) -> {
      let #(cs, cv) = build_condition(condition, adapter)
      #(fmt.not(cs), cv)
    }
    Exists(query) -> {
      let SqlBuilder(sql, vals) = build_single_select(query, adapter)
      #(fmt.exists(sql), vals)
    }
    Raw(sql:) -> #(sql, [])
  }
}

fn build_binary_condition(
  left: Operand(v),
  op: fn(String, String) -> String,
  right: Operand(v),
  adapter: Adapter(v),
) -> #(String, List(List(v))) {
  let #(ls, lv) = build_operand(left, adapter)
  let #(rs, rv) = build_operand(right, adapter)

  #(op(ls, rs), list.prepend(lv, list.flatten(rv)))
}

fn build_join(
  builder: SqlBuilder(v),
  join: Join(v),
  adapter: Adapter(v),
) -> SqlBuilder(v) {
  let #(join_fn, tbl, on_conditions) = case join {
    InnerJoin(table:, on:) -> #(fmt.inner_join, table, on)
    LeftJoin(table:, on:) -> #(fmt.left_join, table, on)
    RightJoin(table:, on:) -> #(fmt.right_join, table, on)
    FullJoin(table:, on:) -> #(fmt.full_join, table, on)
  }

  let #(on_sql, on_vals) = case on_conditions {
    [] -> #("", [])
    [single] -> build_condition(single, adapter)
    [first, ..rest] -> {
      rest
      |> list.fold(first, And)
      |> build_condition(adapter)
    }
  }

  let sql =
    builder.sql
    |> join_fn(build_table(tbl, adapter))
    |> fmt.on(on_sql)

  SqlBuilder(sql, list.prepend(builder.values, list.flatten(on_vals)))
}

fn build_union(query: UnionQuery(v), adapter: Adapter(v)) -> SqlBuilder(v) {
  let union_fn = case query.union_type {
    Union -> fmt.union
    UnionAll -> fmt.union_all
  }

  let #(sql_parts, values) =
    query.selects
    |> list.fold(#([], []), fn(acc, q) {
      let #(parts, vals) = acc
      let builder = build_single_select(q, adapter)

      #(list.prepend(parts, builder.sql), list.prepend(vals, builder.values))
    })

  let values = list.flatten(values) |> list.reverse

  let sql = case sql_parts {
    [] -> ""
    [first, ..rest] -> list.fold(rest, first, union_fn)
  }
  SqlBuilder(sql, values)
}

fn build_single_select(
  builder: Builder(Select, v),
  adapter: Adapter(v),
) -> SqlBuilder(v) {
  case builder {
    SelectBuilder(query:, ..) -> build_select(query, adapter)
    UnionBuilder(query:, ..) -> build_union(query, adapter)
    _ -> SqlBuilder("", [])
  }
}

fn build_column(column: Column, adapter: Adapter(v)) -> String {
  case column {
    All -> "*"
    Column(table:, name:, alias:, func:) -> {
      let col_ref = case name {
        "*" -> {
          case table {
            Some(tbl) -> adapter.handle_identifier(tbl) <> ".*"
            None -> "*"
          }
        }
        _ -> {
          case table {
            Some(tbl) ->
              adapter.handle_identifier(tbl)
              <> "."
              <> adapter.handle_identifier(name)
            None -> adapter.handle_identifier(name)
          }
        }
      }

      let col_ref = case func {
        None -> col_ref
        Some(Count) -> fmt.count(col_ref)
        Some(Sum) -> fmt.sum(col_ref)
        Some(Avg) -> fmt.avg(col_ref)
        Some(Max) -> fmt.max(col_ref)
        Some(Min) -> fmt.min(col_ref)
      }

      case alias {
        None -> col_ref
        Some(a) -> fmt.alias_as(col_ref, adapter.handle_identifier(a))
      }
    }
  }
}

fn build_columns(columns: List(Column), adapter: Adapter(v)) -> String {
  columns
  |> list.map(fn(c) { build_column(c, adapter) })
  |> string.join(", ")
}

fn build_table(tbl: Table, adapter: Adapter(v)) -> String {
  case tbl.alias {
    None -> adapter.handle_identifier(tbl.name)
    Some(a) ->
      fmt.alias_as(
        adapter.handle_identifier(tbl.name),
        adapter.handle_identifier(a),
      )
  }
}

fn build_order_by(orders: List(OrderBy), adapter: Adapter(v)) -> String {
  orders
  |> list.map(fn(o) {
    let col_str = build_column(o.column, adapter)
    case o.direction {
      Asc -> fmt.asc(col_str)
      Desc -> fmt.desc(col_str)
    }
  })
  |> string.join(", ")
}
