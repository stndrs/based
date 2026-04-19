# Changelog

## [4.0.0]

Complete rewrite of the library. The v3 actor-based API has been replaced with a
composable, database-agnostic SQL query builder and type system.

### Added

- **Database layer** (`based`): Structured error types, parameterized queries,
  connection/driver abstraction, transaction support, batch queries, and row
  decoding via `gleam/decode`.
- **SQL query builder** (`based/sql`): Composable, type-safe builders for
  `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `UNION`/`UNION ALL`, and
  `WITH`/`WITH RECURSIVE`. Configurable adapter system for placeholder style,
  identifier quoting, and value type mapping via `on_placeholder`,
  `on_identifier`, `on_value`, `on_text`, `on_int`, and `on_null`. Each builder
  produces either a parameterized `Query` or a formatted SQL string. Includes a
  type-safe condition API supporting `eq`, `gt`, `lt`, `between`, `like`, `in`,
  `is_null`, `or`, `not`, `exists`, and `raw`. Column-vs-column and
  column-vs-value comparisons are distinguished at the type level.
- **Joins**: `INNER`, `LEFT`, `RIGHT`, and `FULL` joins with condition-based `ON`
  clauses.
- **Subqueries**: `WHERE EXISTS`, `IN (subquery)`, `ANY`, and `ALL` support.

### Removed

- `based` module (v3 actor-based API with `register`, `execute`, `query`,
  connection pooling via OTP actors).
- `based/testing` module.
- `gleam_erlang` and `gleam_otp` dependencies.

### Changed

- Minimum Gleam version is now `>= 1.11.0`.
- Dependencies: `gleam_stdlib >= 0.44.0`, `gleam_time >= 1.6.0`.
