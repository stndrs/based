# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com),
and this project adheres to [Semantic Versioning](https://semver.org).

## [4.0.0] - 2026-03-08

Complete rewrite of the library. The v3 actor-based API has been replaced with a
composable, database-agnostic SQL query builder and type system.

### Added

- **Database layer** (`based/db`): Value types covering common SQL data types
  (text, int, float, bool, bytea, date, time, datetime, timestamp, timestamptz,
  interval, uuid, array, null), structured error types, parameterized queries,
  connection/driver abstraction, transaction support, batch queries, and row
  decoding via `gleam/decode`.
- **Repository configuration** (`based/repo`): Configurable SQL formatting
  (placeholders, identifiers, value serialization) with a ready-made `default()`
  preset. Adapter packages use `on_placeholder`, `on_identifier`, `on_value`,
  `on_text`, `on_int`, and `on_null` to plug in database-specific behaviour.
- **SQL query builders**: Composable, type-safe builders for
  `SELECT` (`based/sql/select`), `INSERT` (`based/sql/insert`),
  `UPDATE` (`based/sql/update`), `DELETE` (`based/sql/delete`),
  `UNION`/`UNION ALL` (`based/sql/union`), and
  `WITH`/`WITH RECURSIVE` (`based/sql/with`).
  Each builder produces either a parameterized `db.Query` or a formatted SQL
  string.
- **Conditions and comparisons** (`based/sql`, `based/sql/column`,
  `based/sql/condition`): Type-safe condition API supporting `eq`, `gt`, `lt`,
  `between`, `like`, `in`, `is`, `is_null`, `or`, `not`, `exists`, and `raw`.
  Column-vs-column and column-vs-value comparisons are distinguished at the type
  level via `sql.col` and `sql.val` kinds.
- **Joins**: `INNER`, `LEFT`, `RIGHT`, and `FULL` joins with condition-based `ON`
  clauses.
- **Subqueries**: `WHERE EXISTS`, `IN (subquery)`, `ANY`, and `ALL` support.
- **UUID** (`based/uuid`): Generation of v4 (random) and v7 (time-ordered) UUIDs,
  parsing, formatting, nil UUID, and version detection.
- **Interval** (`based/interval`): ISO 8601 duration type with months, days,
  seconds, and microseconds. Includes addition, string formatting, and a decoder.

### Removed

- `based` module (v3 actor-based API with `register`, `execute`, `query`,
  connection pooling via OTP actors).
- `based/testing` module.
- `gleam_erlang` and `gleam_otp` dependencies.

### Changed

- Minimum Gleam version is now `>= 1.11.0`.
- Dependencies: `gleam_stdlib >= 0.44.0`, `gleam_time >= 1.6.0`,
  `gleam_crypto >= 1.5.1`.
