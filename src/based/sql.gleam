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
////   |> sql.where([sql.col("active") |> sql.eq(sql.true, of: sql.value)])
////   |> sql.order_by(sql.col("name"), sql.asc)
////   |> sql.limit(10)
////   |> sql.to_query(adapter)
////
//// query.sql
//// // -> "SELECT name, email FROM users WHERE active = ? ORDER BY name ASC LIMIT 10"
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
////   |> sql.on_null(fn() { mysql.null })
////   |> sql.on_int(fn(i) { mysql.int(i) })
////   |> sql.on_text(fn(s) { mysql.text(s) })
////   |> sql.on_placeholder(fn(_) { "?" })
////   |> sql.on_value(myslq_value_to_string)
////   |> sql.on_identifier(fn(name) { "`" <> name <> "`" })
//// ```
////
//// ## Phantom types
////
//// The `Builder(kind, v)` type uses phantom types to restrict which modifier
//// functions can be called. For example, `join` only accepts `Builder(Select, v)`,
//// and `set` only accepts `Builder(Update, v)`. This helps callers avoid
//// building invalid SQL queries. It is possible to call functions that do
//// not modify the provided `Builder`. Passing `Builder(Insert, v)`
//// to `sql.where` will not modify the query builder.
////

import based/internal/fmt
import gleam/dict
import gleam/function
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string

/// Contains a parameterized SQL string and an ordered list of values. Can be
/// used directly or built from a `sql.Builder`.
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
    table: Option(Table),
    name: String,
    alias: Option(String),
    func: Option(Aggregate),
  )
  All(table: Option(Table))
}

/// Creates a plain column reference.
pub fn column(name: String) -> Column {
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
pub fn column_for(column: Column, table: Table) -> Column {
  case column {
    Column(..) -> Column(..column, table: Some(table))
    All(..) -> All(table: Some(table))
  }
}

/// Sets an alias on a column. Renders as `column AS alias`.
/// No-ops on `star`.
pub fn column_as(column: Column, alias alias: String) -> Column {
  case column {
    Column(..) -> Column(..column, alias: Some(alias))
    All(..) -> column
  }
}

/// The `*` wildcard column, for use in `SELECT *`.
pub const star = All(table: None)

/// Database adapter that controls how queries are serialized.
///
/// An adapter defines how placeholders, identifiers, and values are formatted
/// for a specific database backend. Create one with `new_adapter()` and
/// configure it using the `on_*` builder functions.
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
pub fn adapter() -> Adapter(v) {
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
pub fn on_null(adapter: Adapter(v), with handle_null: fn() -> v) -> Adapter(v) {
  Adapter(..adapter, handle_null:)
}

/// Sets the function that wraps an `Int` into the value type `v`.
///
/// Used internally when rendering `LIMIT`, `OFFSET`, and other integer literals.
pub fn on_int(adapter: Adapter(v), with handle_int: fn(Int) -> v) -> Adapter(v) {
  Adapter(..adapter, handle_int:)
}

/// Sets the function that wraps a `String` into the value type `v`.
///
/// Used internally when rendering `LIKE` patterns and other string literals.
pub fn on_text(
  adapter: Adapter(v),
  with handle_text: fn(String) -> v,
) -> Adapter(v) {
  Adapter(..adapter, handle_text:)
}

/// A reusable specification for INSERT rows. Defines the column names and
/// how to map inputs too values.
///
/// Built using `rows` and piping through `val`:
///
/// ```gleam
/// let users =
///   sql.rows([alice, bob])
///   |> sql.value("name", fn(u) { sql.text(u.name) })
///   |> sql.value("age", fn(u) { sql.int(u.age) })
///
/// sql.insert(into: sql.table("users"))
/// |> sql.values(users)
/// ```
pub opaque type Rows(a, v) {
  Rows(columns: List(String), extractors: List(fn(a) -> v), values: List(a))
}

/// Adds a column to the rows being inserted, with a function to extract the SQL value.
pub fn value(
  rows: Rows(a, v),
  column: String,
  extract: fn(a) -> v,
) -> Rows(a, v) {
  Rows(
    columns: [column, ..rows.columns],
    extractors: [extract, ..rows.extractors],
    values: rows.values,
  )
}

/// Creates a new `Rows` with the given values. Pipe through `val` to add
/// columns and extractors.
pub fn rows(values: List(a)) -> Rows(a, v) {
  Rows(columns: [], extractors: [], values:)
}

type Operand(v) {
  ColumnRef(Column)
  ValueRef(v)
  NullRef
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

type Join(v) {
  InnerJoin(table: Table, on: List(Condition(v)))
  LeftJoin(table: Table, on: List(Condition(v)))
  RightJoin(table: Table, on: List(Condition(v)))
  FullJoin(table: Table, on: List(Condition(v)))
}

/// Adds an `INNER JOIN` clause to a SELECT query.
pub fn inner_join(
  builder: Builder(Select, v),
  table table: Table,
  on on: List(Condition(v)),
) -> Builder(Select, v) {
  prepend_join(builder, InnerJoin(table: table, on: on))
}

/// Adds a `LEFT JOIN` clause to a SELECT query.
pub fn left_join(
  builder: Builder(Select, v),
  table table: Table,
  on on: List(Condition(v)),
) -> Builder(Select, v) {
  prepend_join(builder, LeftJoin(table: table, on: on))
}

/// Adds a `RIGHT JOIN` clause to a SELECT query.
pub fn right_join(
  builder: Builder(Select, v),
  table table: Table,
  on on: List(Condition(v)),
) -> Builder(Select, v) {
  prepend_join(builder, RightJoin(table: table, on: on))
}

/// Adds a `FULL JOIN` clause to a SELECT query.
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
  Asc(column: Column)
  Desc(column: Column)
}

/// Returns an ascending `Order` for the column.
pub fn asc(column: Column) -> Order {
  Asc(column:)
}

/// Returns a descending `Order` for the column.
pub fn desc(column: Column) -> Order {
  Desc(column:)
}

/// The action to take when an INSERT conflict occurs.
///
/// - `DoNothing` ignores the conflicting row (`ON CONFLICT ... DO NOTHING`)
/// - `DoUpdate(sets:)` updates specified columns (`ON CONFLICT ... DO UPDATE SET ...`).
///   Each tuple is `#(column_name, raw_sql_expression)`. Column names are quoted
///   by the adapter; expression strings are emitted verbatim (e.g. `"excluded.quantity"`).
pub type ConflictAction {
  DoNothing
  DoUpdate(sets: List(#(String, String)))
}

type OnConflict(v) {
  OnConflict(target: String, action: ConflictAction, wheres: List(Condition(v)))
}

/// Describes how to convert an input of type `a` into an internal operand
/// for query building.
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

/// Phantom type for subqueries.
pub type Subquery

/// Phantom type for UNION queries.
pub type Union

/// Phantom type for UNION ALL queries.
pub type UnionAll

type UnionType {
  Union
  UnionAll
}

/// An intermediate builder representing a table source.
/// Created by `from()` or `from_subquery()`, then passed to `select()` or
/// `delete()` to produce a full query builder.
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
    order_by: List(Order),
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
    sets: List(Set(v)),
    wheres: List(List(Condition(v))),
    returning: List(Column),
    order_by: List(Order),
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
/// and a value type `v`.
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
  Equal(ColumnRef(column), kind.to_operand(input))
}

/// Creates an inequality condition.
pub fn not_eq(column: Column, input: a, of kind: Kind(a, v)) -> Condition(v) {
  NotEqual(ColumnRef(column), kind.to_operand(input))
}

/// Creates a greater-than condition.
pub fn gt(column: Column, input: a, of kind: Kind(a, v)) -> Condition(v) {
  GreaterThan(ColumnRef(column), kind.to_operand(input))
}

/// Creates a less-than condition.
pub fn lt(column: Column, input: a, of kind: Kind(a, v)) -> Condition(v) {
  LessThan(ColumnRef(column), kind.to_operand(input))
}

/// Creates a greater-than-or-equal condition.
pub fn gt_eq(column: Column, input: a, of kind: Kind(a, v)) -> Condition(v) {
  GreaterThanOrEqual(ColumnRef(column), kind.to_operand(input))
}

/// Creates a less-than-or-equal condition.
pub fn lt_eq(column: Column, input: a, of kind: Kind(a, v)) -> Condition(v) {
  LessThanOrEqual(ColumnRef(column), kind.to_operand(input))
}

/// Creates a BETWEEN condition.
pub fn between(
  column: Column,
  low: a,
  high: a,
  of kind: Kind(a, v),
) -> Condition(v) {
  Between(ColumnRef(column), kind.to_operand(low), kind.to_operand(high))
}

/// Creates a LIKE condition.
pub fn like(column: Column, input: a, of kind: Kind(a, v)) -> Condition(v) {
  Like(ColumnRef(column), kind.to_operand(input))
}

/// Creates a NOT LIKE condition.
pub fn not_like(column: Column, input: a, of kind: Kind(a, v)) -> Condition(v) {
  NotLike(ColumnRef(column), kind.to_operand(input))
}

/// Creates an IN condition.
pub fn in(column: Column, values: List(a), of kind: Kind(a, v)) -> Condition(v) {
  In(ColumnRef(column), list.map(values, kind.to_operand))
}

/// Creates an IS NULL condition.
pub fn is_null(column: Column) -> Condition(v) {
  IsNull(ColumnRef(column))
}

/// Creates an IS NOT NULL condition.
pub fn is_not_null(column: Column) -> Condition(v) {
  IsNotNull(ColumnRef(column))
}

/// Creates an IS TRUE condition.
pub fn is_true(column: Column) -> Condition(v) {
  IsTrue(ColumnRef(column))
}

/// Creates an IS FALSE condition.
pub fn is_false(column: Column) -> Condition(v) {
  IsFalse(ColumnRef(column))
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

/// Creates a raw SQL condition.
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
pub fn update(tbl: Table, sets: List(Set(v))) -> Builder(Update, v) {
  UpdateQuery(
    table: tbl,
    sets:,
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
    _ -> panic as "unreachable: delete requires FromTable"
  }
}

/// Creates a `From` that selects from a subquery instead of a table.
pub fn from_subquery(
  builder: Builder(Select, v),
  alias alias: String,
) -> From(Subquery, v) {
  FromSubQuery(builder:, alias:)
}

/// Adds a WHERE condition to the query.
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
pub fn order_by(builder: Builder(a, v), order_by: List(Order)) -> Builder(a, v) {
  case builder {
    SelectBuilder(query:, ctes:, recursive:) ->
      SelectQuery(..query, order_by:)
      |> SelectBuilder(ctes:, recursive:)
    UpdateBuilder(query:, ctes:, recursive:) ->
      UpdateQuery(..query, order_by:)
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
    _ -> panic as "unreachable: group_by called on non-Select builder"
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

/// Adds DISTINCT to a SELECT query.
pub fn distinct(builder: Builder(Select, v)) -> Builder(Select, v) {
  case builder {
    SelectBuilder(query:, ctes:, recursive:) ->
      SelectQuery(..query, distinct: True) |> SelectBuilder(ctes:, recursive:)
    _ -> panic as "unreachable: distinct called on non-Select builder"
  }
}

/// Adds a HAVING clause to a SELECT query.
pub fn having(
  builder: Builder(Select, v),
  conditions: List(Condition(v)),
) -> Builder(Select, v) {
  case builder {
    SelectBuilder(query:, ctes:, recursive:) ->
      SelectQuery(..query, having: list.prepend(query.having, conditions))
      |> SelectBuilder(ctes:, recursive:)
    _ -> panic as "unreachable: having called on non-Select builder"
  }
}

/// Adds `FOR UPDATE` to a SELECT query for row-level locking.
pub fn for_update(builder: Builder(Select, v)) -> Builder(Select, v) {
  case builder {
    SelectBuilder(query:, ctes:, recursive:) ->
      SelectQuery(..query, for_update: True) |> SelectBuilder(ctes:, recursive:)
    _ -> panic as "unreachable: for_update called on non-Select builder"
  }
}

/// Sets the rows to be inserted.
pub fn values(
  builder: Builder(Insert, v),
  rows: Rows(a, v),
) -> Builder(Insert, v) {
  let columns = list.reverse(rows.columns)
  let extractors = list.reverse(rows.extractors)
  let value_rows =
    rows.values
    |> list.map(fn(item) {
      extractors
      |> list.map(fn(extract) { extract(item) })
    })

  case builder {
    InsertBuilder(query:, ctes:, recursive:) ->
      InsertQuery(..query, columns: columns, values: value_rows)
      |> InsertBuilder(ctes:, recursive:)
    _ -> panic as "unreachable: values called on non-Insert builder"
  }
}

/// Adds an ON CONFLICT clause to an INSERT query (upsert).
pub fn on_conflict(
  builder: Builder(Insert, v),
  target: String,
  action: ConflictAction,
  wheres: List(Condition(v)),
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
    _ -> panic as "unreachable: on_conflict called on non-Insert builder"
  }
}

/// Kind that treats the input as a parameterized value.
pub const val = Kind(to_operand: ValueRef)

/// Kind that treats the input as a subquery for scalar comparisons.
pub const subquery = Kind(to_operand: SubQuery)

/// Kind that treats the input as a column reference for column-to-column
/// comparisons.
pub const col = Kind(to_operand: ColumnRef)

/// Kind that wraps a subquery with `ANY(...)`.
pub const any = Kind(to_operand: AnyQuery)

/// Kind that wraps a subquery with `ALL(...)`.
pub const all = Kind(to_operand: AllQuery)

/// Creates a kind that maps an arbitrary type to a value using a conversion
/// function. Useful for `in` clauses with custom types.
pub fn list(of map: fn(a) -> v) -> Kind(a, v) {
  Kind(to_operand: fn(x) { ValueRef(map(x)) })
}

pub opaque type Set(v) {
  Set(column: String, operand: Operand(v))
}

/// Sets a column to a value in an UPDATE query. Can be called multiple times.
pub fn set(column: String, input: a, of kind: Kind(a, v)) -> Set(v) {
  Set(column:, operand: kind.to_operand(input))
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

/// Combines SELECT queries with `UNION`.
pub fn union(selects: List(Builder(Select, v))) -> Builder(Union, v) {
  UnionBuilder(
    query: UnionQuery(selects:, union_type: Union),
    ctes: [],
    recursive: False,
  )
}

/// Combines SELECT queries with `UNION ALL`.
pub fn union_all(selects: List(Builder(Select, v))) -> Builder(UnionAll, v) {
  UnionBuilder(
    query: UnionQuery(selects:, union_type: UnionAll),
    ctes: [],
    recursive: False,
  )
}

/// Serializes a query builder into a `Query(v)` with parameterized placeholders.
///
/// Returns a `Query` record with `.sql` containing the SQL string with
/// placeholders and `.values` containing the parameter values in order.
pub fn to_query(builder: Builder(a, v), adapter: Adapter(v)) -> Query(v) {
  let SqlBuilder(sql, values) = build_query(builder, adapter)
  let sql = placeholders(sql, adapter.handle_placeholder)
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

  let values_by_idx =
    values
    |> list.index_map(fn(val, idx) { #(idx + 1, val) })
    |> dict.from_list

  let with = fn(idx) {
    values_by_idx
    |> dict.get(idx)
    |> result.map(adapter.handle_value)
    |> result.unwrap("")
  }

  placeholders(sql, with)
}

fn placeholders(for st: String, with mapper: fn(Int) -> String) -> String {
  string.split(st, on: fmt.placeholder)
  |> list.index_fold(from: [], with: fn(acc, st1, idx) {
    case idx {
      0 -> [st1, ..acc]
      idx -> {
        let ph = mapper(idx)

        [st1, ph, ..acc]
      }
    }
  })
  |> list.reverse
  |> string.join(with: "")
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
  orders: List(Order),
  adapter: Adapter(v),
) -> SqlBuilder(v) {
  case orders {
    [] -> builder
    _ -> {
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

  SqlBuilder(
    sql: cte_builder.sql <> fmt.terminate(builder.sql),
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
  sets: List(Set(v)),
  adapter: Adapter(v),
) -> SqlBuilder(v) {
  let #(sets_sql, values) = {
    use #(sql, values), set <- list.fold(sets, #([], []))

    let #(set_string, operand_values) = build_operand(set.operand, adapter)
    let values = list.prepend(values, operand_values)

    let sql =
      set.column
      |> adapter.handle_identifier
      |> fmt.eq(set_string)
      |> list.prepend(sql, _)

    #(sql, values)
  }

  let sets_sql = sets_sql |> list.reverse |> string.join(", ")
  let values = values |> list.reverse |> list.flatten |> list.flatten

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
    ColumnRef(column) -> #(build_column(column, adapter), [])
    ValueRef(value) -> #(fmt.placeholder, [[value]])
    NullRef -> #(fmt.placeholder, [[adapter.handle_null()]])
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
    _ ->
      panic as "unreachable: build_single_select called with non-Select builder"
  }
}

fn build_column(column: Column, adapter: Adapter(v)) -> String {
  case column {
    All(table:) -> {
      case table {
        Some(tbl) -> build_table_ref(tbl, adapter) <> ".*"
        None -> "*"
      }
    }
    Column(table:, name:, alias:, func:) -> {
      let col_ref = case name {
        "*" -> {
          case table {
            Some(tbl) -> build_table_ref(tbl, adapter) <> ".*"
            None -> "*"
          }
        }
        _ -> {
          case table {
            Some(tbl) ->
              build_table_ref(tbl, adapter)
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

fn build_table_ref(tbl: Table, adapter: Adapter(v)) -> String {
  case tbl.alias {
    None -> adapter.handle_identifier(tbl.name)
    Some(a) -> adapter.handle_identifier(a)
  }
}

fn build_order_by(orders: List(Order), adapter: Adapter(v)) -> String {
  orders
  |> list.map(fn(o) {
    case o {
      Asc(column:) -> column |> build_column(adapter) |> fmt.asc
      Desc(column:) -> column |> build_column(adapter) |> fmt.desc
    }
  })
  |> string.join(", ")
}
