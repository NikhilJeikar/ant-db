# PostgreSQL SQL Compatibility Matrix

This document tracks how closely **ant-db** matches PostgreSQL SQL and protocol behavior.
It is intended for development planning and client compatibility checks.

**Legend**

| Status | Meaning |
|--------|---------|
| ✅ Supported | Works for typical use cases |
| ⚠️ Partial | Parsed or partially implemented; important gaps remain |
| ❌ Missing | Not implemented or explicitly rejected |

**Last reviewed:** 2026-06-08 (against `src/backend/core/pg_wire.rs`, `search.rs`)

---

## General behavior

| Feature | Status | Notes |
|---------|--------|-------|
| PostgreSQL wire protocol | ⚠️ Partial | Simple-query protocol only |
| Extended query (prepared statements, bind params) | ❌ Missing | `PlaceholderExtendedQueryHandler` — `$1`, portals, etc. do not work |
| `COPY FROM` / `COPY TO` | ❌ Missing | `NoopCopyHandler` |
| Multi-statement queries | ❌ Missing | Only the **first** parsed statement is executed |
| Schema / catalog (`public.foo`) | ❌ Missing | Flat table namespace; names normalized to lowercase |
| `information_schema` / `pg_catalog` | ❌ Missing | No system catalogs |
| Result type metadata | ⚠️ Partial | All columns reported as `VARCHAR`; values sent as `Debug` strings |
| Authentication / roles / `GRANT` | ❌ Missing | No role-based access control |

---

## DDL

| Feature | Status | Notes |
|---------|--------|-------|
| `CREATE TABLE` | ✅ Supported | Column definitions with types and constraints |
| `CREATE TABLE IF NOT EXISTS` | ✅ Supported | |
| `CREATE TABLE AS SELECT` (`CTAS`) | ❌ Missing | Explicitly rejected |
| `DROP TABLE` | ✅ Supported | One table per statement |
| `DROP TABLE IF EXISTS` | ✅ Supported | |
| `ALTER TABLE … DROP COLUMN` | ✅ Supported | `IF EXISTS` supported |
| `ALTER TABLE … DROP COLUMN CASCADE` | ❌ Missing | Explicitly rejected |
| `ALTER TABLE ADD COLUMN` | ❌ Missing | |
| `ALTER TABLE RENAME` | ❌ Missing | |
| `ALTER TABLE ALTER COLUMN` | ❌ Missing | |
| `CREATE INDEX` | ❌ Missing | |
| `DROP INDEX` | ❌ Missing | |
| `CREATE VIEW` | ❌ Missing | |
| `CREATE MATERIALIZED VIEW` | ❌ Missing | |
| `CREATE SCHEMA` / `DROP SCHEMA` | ❌ Missing | |
| `CREATE DATABASE` / `DROP DATABASE` | ❌ Missing | |
| `CREATE TYPE` / `ENUM` / `DOMAIN` | ❌ Missing | |
| `CREATE SEQUENCE` | ❌ Missing | |
| `CREATE FUNCTION` / `PROCEDURE` | ❌ Missing | |
| `CREATE TRIGGER` | ❌ Missing | |
| `CREATE EXTENSION` | ❌ Missing | |
| `TRUNCATE` | ❌ Missing | |
| `SHOW TABLES` | ✅ Supported | `LIKE` / `ILIKE` filters; `WHERE` filter rejected |

### `CREATE TABLE` — column types

| PostgreSQL type (family) | Status | Maps to |
|--------------------------|--------|---------|
| `BOOLEAN` | ✅ Supported | `Boolean` |
| `SMALLINT` / `INT2` | ✅ Supported | `IntegerI16` |
| `INTEGER` / `INT` / `INT4` | ✅ Supported | `IntegerI32` |
| `BIGINT` / `INT8` | ✅ Supported | `IntegerI64` |
| Unsigned integer variants | ✅ Supported | Corresponding unsigned types |
| `REAL` / `FLOAT4` / `DOUBLE PRECISION` / `FLOAT8` | ✅ Supported | `FloatF64` |
| `NUMERIC` / `DECIMAL` | ⚠️ Partial | Stored as `FloatF64` (precision not preserved) |
| `TEXT` / `VARCHAR` / `CHAR` / `CLOB` | ✅ Supported | `String` |
| `UUID` | ⚠️ Partial | Stored as `String` |
| `BYTEA` / `BLOB` / `BINARY` | ✅ Supported | `Bytes` |
| `DATE` / `TIME` / `TIMESTAMP` / `TIMESTAMPTZ` | ❌ Missing | |
| `JSON` / `JSONB` | ❌ Missing | |
| `ARRAY` | ❌ Missing | |
| `INET` / `CIDR` / `MACADDR` | ❌ Missing | |
| `SERIAL` / `BIGSERIAL` | ❌ Missing | `AUTO_INCREMENT` dialect token accepted instead |
| Geometric, range, enum, composite types | ❌ Missing | |

### `CREATE TABLE` — constraints

| Constraint | Status | Notes |
|------------|--------|-------|
| `NOT NULL` | ⚠️ Partial | Parsed at DDL; **not enforced on INSERT** |
| `UNIQUE` | ⚠️ Partial | Parsed at DDL; **not enforced on INSERT** |
| `PRIMARY KEY` | ⚠️ Partial | Parsed at DDL; **not enforced on INSERT** |
| `AUTO_INCREMENT` (dialect token) | ⚠️ Partial | Accepted in DDL; auto-value generation not implemented |
| `DEFAULT` | ❌ Missing | Rejected at parse time |
| `FOREIGN KEY` | ❌ Missing | Rejected at parse time |
| `CHECK` | ❌ Missing | Rejected at parse time |
| `GENERATED` columns | ❌ Missing | Rejected at parse time |
| Table-level `PRIMARY KEY` / `UNIQUE` | ⚠️ Partial | Parsed; same enforcement gap as column constraints |

---

## DML

| Feature | Status | Notes |
|---------|--------|-------|
| `INSERT INTO … VALUES` | ✅ Supported | Multiple rows; optional column list |
| `INSERT INTO … SELECT` | ❌ Missing | Explicitly rejected |
| `INSERT … ON CONFLICT` | ❌ Missing | |
| `INSERT … RETURNING` | ❌ Missing | |
| `UPDATE` | ✅ Supported | Single table; literal assignments |
| `UPDATE … RETURNING` | ❌ Missing | |
| `DELETE` | ✅ Supported | Single table; `WHERE` only |
| `DELETE … RETURNING` | ❌ Missing | |
| `DELETE ORDER BY` | ❌ Missing | Explicitly rejected |
| `DELETE LIMIT` | ❌ Missing | Explicitly rejected |
| `DELETE USING` | ❌ Missing | Explicitly rejected |
| Multi-table `UPDATE` / `DELETE` | ❌ Missing | |

### `INSERT` / `UPDATE` value expressions

| Expression | Status | Notes |
|------------|--------|-------|
| Numeric literals | ✅ Supported | Including unary minus |
| String literals | ✅ Supported | |
| `TRUE` / `FALSE` | ✅ Supported | |
| `NULL` | ✅ Supported | |
| Column references in `UPDATE SET` | ❌ Missing | Literals only |
| Functions / subqueries in values | ❌ Missing | |

---

## `SELECT`

| Feature | Status | Notes |
|---------|--------|-------|
| Plain `SELECT` from one table | ✅ Supported | |
| `SELECT *` | ✅ Supported | |
| `SELECT col1, col2` | ✅ Supported | Column identifiers only |
| `SELECT` expressions (`col + 1`, functions) | ❌ Missing | |
| Column / table aliases | ❌ Missing | |
| `DISTINCT` | ❌ Missing | |
| `WHERE` | ✅ Supported | See predicate table below |
| `ORDER BY` | ✅ Supported | Column identifiers only; `ASC` / `DESC` |
| `LIMIT` | ✅ Supported | Numeric literal only |
| `OFFSET` | ✅ Supported | Numeric literal only |
| `JOIN` (all kinds) | ❌ Missing | |
| Subqueries | ❌ Missing | |
| `UNION` / `INTERSECT` / `EXCEPT` | ❌ Missing | |
| `GROUP BY` | ❌ Missing | |
| `HAVING` | ❌ Missing | |
| Aggregate functions (`COUNT`, `SUM`, …) | ❌ Missing | |
| Window functions | ❌ Missing | |
| CTEs (`WITH …`) | ❌ Missing | |
| `SELECT FOR UPDATE` / `FOR SHARE` | ❌ Missing | |

### `WHERE` predicates

| Predicate | Status | Notes |
|-----------|--------|-------|
| `=` / `<>` / `!=` | ✅ Supported | |
| `<` / `>` / `<=` / `>=` | ✅ Supported | |
| `AND` / `OR` / `NOT` | ✅ Supported | |
| `IS NULL` / `IS NOT NULL` | ✅ Supported | |
| `LIKE` / `ILIKE` | ❌ Missing | Only available for `SHOW TABLES` filtering |
| `IN (…)` | ❌ Missing | |
| `BETWEEN` | ❌ Missing | |
| `EXISTS` | ❌ Missing | |
| Function calls | ❌ Missing | |

---

## Transactions

| Feature | Status | Notes |
|---------|--------|-------|
| `BEGIN` / `START TRANSACTION` | ✅ Supported | Per connection |
| `COMMIT` | ✅ Supported | |
| `ROLLBACK` | ✅ Supported | |
| `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` | ❌ Missing | |
| Isolation level (`READ COMMITTED`, `SERIALIZABLE`, …) | ❌ Missing | |
| Auto-commit (implicit transactions) | ✅ Supported | Non-`BEGIN` queries auto-commit |
| MVCC row versioning | ⚠️ Partial | Snapshot visibility at row level; not full Postgres isolation |
| Row-level locks (`FOR UPDATE`) | ❌ Missing | |
| Advisory locks | ❌ Missing | |

---

## Administration & introspection

| Feature | Status | Notes |
|---------|--------|-------|
| `EXPLAIN` | ❌ Missing | |
| `VACUUM` (SQL command) | ❌ Missing | Internal compaction exists but no SQL interface |
| `ANALYZE` | ❌ Missing | |
| `LISTEN` / `NOTIFY` | ❌ Missing | |
| `SET` / `SHOW` (session variables) | ❌ Missing | Except `SHOW TABLES` |
| Replication (`CREATE PUBLICATION`, etc.) | ❌ Missing | |
| Tablespaces / partitioning | ❌ Missing | |

---

## Storage & durability

| Feature | Status | Notes |
|---------|--------|-------|
| In-memory / page-based table storage | ✅ Supported | |
| WAL / crash recovery (SQL-visible) | ❌ Missing | Config fields exist; not wired to SQL |
| Full-text search (`tsvector`, GIN) | ❌ Missing | |
| Secondary indexes (SQL-level) | ❌ Missing | |

---

## Example queries

### Works today

```sql
CREATE TABLE users (
    id   BIGINT PRIMARY KEY,
    name TEXT NOT NULL
);

INSERT INTO users VALUES (1, 'alice'), (2, 'bob');

SELECT name FROM users WHERE id = 1 ORDER BY name DESC LIMIT 10 OFFSET 0;

UPDATE users SET name = 'alice2' WHERE id = 1;

DELETE FROM users WHERE id = 2;

BEGIN;
INSERT INTO users VALUES (3, 'carol');
COMMIT;

SHOW TABLES LIKE 'user%';

ALTER TABLE users DROP COLUMN IF EXISTS nickname;

DROP TABLE IF EXISTS users;
```

### Does not work today

```sql
-- Queries
SELECT a.name, b.score FROM a JOIN b ON a.id = b.user_id;
SELECT dept, COUNT(*) FROM employees GROUP BY dept;
SELECT * FROM t WHERE name LIKE 'foo%';
WITH cte AS (SELECT 1) SELECT * FROM cte;

-- DML
INSERT INTO t SELECT * FROM other;
INSERT INTO t VALUES (1) RETURNING id;
COPY t FROM STDIN;

-- DDL
CREATE INDEX idx ON t(col);
CREATE TABLE t2 AS SELECT * FROM t;
ALTER TABLE t ADD COLUMN age INT;

-- Protocol
PREPARE s AS SELECT $1::int;
EXECUTE s(1);
```

---

## Implementation references

| Area | Primary source |
|------|----------------|
| Statement dispatch | `src/backend/core/pg_wire.rs` |
| `SELECT` parsing & `WHERE` | `src/backend/core/search.rs` |
| Storage engine | `src/backend/core/table.rs` |
| MVCC / transactions | `src/backend/core/row.rs`, `transaction.rs` |
| Constraint definitions | `src/backend/core/column.rs` |

### Known enforcement gaps

These are parsed or stored but not fully applied at runtime:

1. **Constraints** — `schema_validation` in `column.rs` is never called during `INSERT`.
2. **Type checking** — `validate_data_type` in `table.rs` is unused.
3. **Auto-increment** — accepted in DDL; values are not generated automatically.

---

## Suggested priority order (for contributors)

1. Extended query protocol (prepared statements) — unlocks most clients/ORMs
2. Constraint enforcement on `INSERT` / `UPDATE`
3. `LIKE` / `IN` / `BETWEEN` in `WHERE`
4. `JOIN` and `GROUP BY` / aggregates
5. `RETURNING` clause
6. `CREATE INDEX` and index-backed lookups
7. Proper PostgreSQL type OIDs in result metadata
