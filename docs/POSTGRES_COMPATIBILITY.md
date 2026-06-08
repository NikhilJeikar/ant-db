# PostgreSQL SQL Compatibility Matrix

This document tracks how closely **ant-db** matches PostgreSQL SQL and protocol behavior.
It is intended for development planning and client compatibility checks.

**Legend**

| Status | Meaning |
|--------|---------|
| ✅ Supported | Works for typical use cases |
| ⚠️ Partial | Parsed or partially implemented; important gaps remain |
| ❌ Missing | Not implemented or explicitly rejected |

**Last reviewed:** 2026-06-08 (against `pg_wire.rs`, `search.rs`, `query.rs`, `table.rs`, `column.rs`, `database.rs`, `page_store.rs`)

---

## General behavior

| Feature | Status | Notes |
|---------|--------|-------|
| PostgreSQL wire protocol | ⚠️ Partial | Simple-query protocol only |
| Extended query (prepared statements, bind params) | ❌ Missing | `PlaceholderExtendedQueryHandler` — `$1`, portals, etc. do not work |
| `COPY FROM` / `COPY TO` | ⚠️ Partial | `COPY table FROM STDIN` / `COPY table TO STDOUT` (text format, tab delimiter); file/program targets and `COPY (query)` unsupported |
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
| `ALTER TABLE … DROP COLUMN` | ✅ Supported | `IF EXISTS` supported; missing columns return an error (no panic) |
| `ALTER TABLE … DROP COLUMN CASCADE` | ❌ Missing | Explicitly rejected |
| `ALTER TABLE ADD COLUMN` | ❌ Missing | |
| `ALTER TABLE RENAME` | ❌ Missing | |
| `ALTER TABLE ALTER COLUMN` | ❌ Missing | |
| `CREATE INDEX` | ⚠️ Partial | Single-column hash index on a table column; `UNIQUE` / `PRIMARY KEY` columns are indexed automatically |
| `DROP INDEX` | ⚠️ Partial | Drops user-created indexes by name; cannot drop constraint-backed indexes |
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
| `NOT NULL` | ✅ Supported | Enforced on `INSERT` and `UPDATE` |
| `UNIQUE` | ✅ Supported | Enforced via in-memory hash index on `INSERT` / `UPDATE`; checks are visibility-aware (tombstoned rows ignored) |
| `PRIMARY KEY` | ✅ Supported | Same as `UNIQUE`; index entries deferred until prune after commit |
| `AUTO_INCREMENT` (dialect token) | ⚠️ Partial | Accepted in DDL; auto-value generation not implemented |
| `DEFAULT` | ❌ Missing | Rejected at parse time |
| `FOREIGN KEY` | ⚠️ Partial | Column-level `REFERENCES` and table-level single-column `FOREIGN KEY` in `CREATE TABLE`; referenced table must already exist; composite keys unsupported; enforced on `INSERT` / `UPDATE` |
| `CHECK` | ⚠️ Partial | Column-level `CHECK` accepted in DDL; expression is **not** evaluated — non-`NULL` writes fail with an explicit unsupported error |
| `GENERATED` columns | ❌ Missing | Rejected at parse time |
| Table-level `PRIMARY KEY` / `UNIQUE` | ✅ Supported | Parsed and applied per column; same enforcement as column constraints |

---

## DML

| Feature | Status | Notes |
|---------|--------|-------|
| `INSERT INTO … VALUES` | ✅ Supported | Multiple rows; optional column list; `NOT NULL` / `UNIQUE` / `PRIMARY KEY` enforced; FK checked when constraint metadata exists |
| `INSERT INTO … SELECT` | ❌ Missing | Explicitly rejected |
| `INSERT … ON CONFLICT` | ❌ Missing | |
| `INSERT … RETURNING` | ❌ Missing | |
| `UPDATE` | ✅ Supported | Single table; literal assignments; constraints (including FK) enforced on updated values |
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
| `SELECT col1, col2` | ✅ Supported | Simple identifiers; qualified `alias.col` in joins |
| `SELECT` expressions (`col + 1`, functions) | ❌ Missing | |
| Column aliases (`AS`) | ⚠️ Partial | Supported in join projections |
| Table aliases | ⚠️ Partial | Supported in `FROM` / `JOIN` |
| `DISTINCT` | ❌ Missing | |
| `WHERE` | ✅ Supported | See predicate table below; equality on indexed columns can use an internal index fast path; qualified predicates in joins |
| `ORDER BY` | ✅ Supported | Simple / qualified column identifiers; `ASC` / `DESC` |
| `LIMIT` | ✅ Supported | Numeric literal only |
| `OFFSET` | ✅ Supported | Numeric literal only |
| `INNER JOIN` / `JOIN` | ✅ Supported | `ON` or `USING`; equality `ON` can use right-side hash index |
| `LEFT JOIN` / `LEFT OUTER JOIN` | ✅ Supported | Unmatched left rows preserved with `NULL` right columns |
| `CROSS JOIN` | ✅ Supported | Cartesian product |
| `RIGHT` / `FULL OUTER` / `NATURAL JOIN` | ❌ Missing | `RIGHT JOIN` rejected with a hint to swap table order |
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
| `=` / `<>` / `!=` | ✅ Supported | Single-column `col = literal` may use an internal hash index when the column is `UNIQUE` / `PRIMARY KEY` |
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
| MVCC row versioning | ⚠️ Partial | Snapshot visibility at row level; not full Postgres isolation. Snapshot reload restores committed-transaction watermark from `next_transaction_id` |
| MVCC index maintenance | ⚠️ Partial | Index inserts are tracked per transaction and rolled back; deletes/updates defer index removal until prune (like Postgres tombstones). Unique checks use visibility-aware lookups |
| Row-level locks (`FOR UPDATE`) | ❌ Missing | |
| Advisory locks | ❌ Missing | |

---

## Administration & introspection

| Feature | Status | Notes |
|---------|--------|-------|
| `EXPLAIN` | ❌ Missing | |
| `VACUUM` (SQL command) | ❌ Missing | Internal prune + compaction runs via periodic `auto_vacuum`; no SQL interface |
| `ANALYZE` | ❌ Missing | |
| `LISTEN` / `NOTIFY` | ❌ Missing | |
| `SET` / `SHOW` (session variables) | ❌ Missing | Except `SHOW TABLES` |
| Replication (`CREATE PUBLICATION`, etc.) | ❌ Missing | |
| Tablespaces / partitioning | ❌ Missing | |

---

## Storage & durability

| Feature | Status | Notes |
|---------|--------|-------|
| In-memory / page-based table storage | ✅ Supported | Append-only pages with configurable `page_size_bytes` |
| On-disk page files | ✅ Supported | Per-table file `{database}-{table}` under `table_data_path`; ANTPG v1 format (`page_store.rs`) |
| Page eviction (memory limit) | ✅ Supported | LRU eviction to disk when `table_memory_limit_bytes` is exceeded; pages loaded on demand |
| Database snapshot (startup / shutdown) | ✅ Supported | Metadata snapshot at `snapshot_path`; row data flushed to page files first |
| Snapshot format | ⚠️ Partial | Metadata only (schema, counters, column indexes); **not** a full in-memory clone. Row locations rebuilt from page files on load |
| Periodic snapshot + compaction | ✅ Supported | When `auto_vacuum_interval_secs > 0`, background task runs compaction and saves snapshot |
| Full-text search (`tsvector`, GIN) | ❌ Missing | |
| Secondary indexes (SQL-level) | ⚠️ Partial | `CREATE INDEX` / `DROP INDEX` for single-column hash indexes; no composite or expression indexes |
| WAL / crash recovery (SQL-visible) | ❌ Missing | Config fields exist; not wired to SQL |

---

## Example queries

### Works today

```sql
CREATE TABLE users (
    id   BIGINT PRIMARY KEY,
    name TEXT NOT NULL
);

INSERT INTO users VALUES (1, 'alice'), (2, 'bob');

-- Duplicate primary key is rejected
-- INSERT INTO users VALUES (1, 'duplicate');

SELECT name FROM users WHERE id = 1 ORDER BY name DESC LIMIT 10 OFFSET 0;

UPDATE users SET name = 'alice2' WHERE id = 1;

DELETE FROM users WHERE id = 2;

BEGIN;
INSERT INTO users VALUES (3, 'carol');
COMMIT;

SHOW TABLES LIKE 'user%';

ALTER TABLE users DROP COLUMN IF EXISTS nickname;

CREATE TABLE orders (
    user_id BIGINT REFERENCES users (id),
    amount  BIGINT
);

INSERT INTO orders VALUES (1, 99);

-- Invalid FK is rejected; NULL is allowed when the column is nullable
-- INSERT INTO orders VALUES (999, 1);

SELECT u.name, o.amount
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE o.amount > 10
ORDER BY u.name;

DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS orders;

CREATE TABLE bulk_users (id BIGINT, name TEXT);
-- psql: COPY bulk_users FROM STDIN; then paste tab-separated rows, end with \.
COPY bulk_users TO STDOUT;
DROP TABLE bulk_users;
```

### Does not work today

```sql
-- Queries
SELECT dept, COUNT(*) FROM employees GROUP BY dept;
SELECT * FROM t WHERE name LIKE 'foo%';
WITH cte AS (SELECT 1) SELECT * FROM cte;

-- DML
INSERT INTO t SELECT * FROM other;
INSERT INTO t VALUES (1) RETURNING id;
-- COPY t FROM STDIN;  -- now works for table COPY FROM/TO STDIN/STDOUT (text)

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
| Query pipeline (parse → bind → optimize → execute) | `src/backend/core/plan/` |
| `COPY` text encoding/parsing | `src/backend/core/copy.rs` |
| `SELECT` parsing & `WHERE` | `src/backend/core/search.rs` |
| `JOIN` execution | `src/backend/core/query.rs` (`execute_physical_select`) |
| Index vs seq scan planning | `src/backend/core/plan/optimize.rs` |
| Storage engine | `src/backend/core/table.rs` |
| On-disk page files | `src/backend/core/page_store.rs` |
| Snapshots & database lifecycle | `src/backend/core/database.rs` (`Database::search`), `src/backend/handler.rs`, `src/main.rs` |
| MVCC / transactions | `src/backend/core/row.rs`, `transaction.rs` |
| Constraints & internal indexes | `src/backend/core/column.rs` |
| Configuration | `src/backend/config.rs` (`table_data_path`, `snapshot_path`, `page_size_bytes`, `table_memory_limit_bytes`, `auto_vacuum_interval_secs`) |

### Known enforcement gaps

These are parsed or stored but not fully applied at runtime:

1. **Auto-increment** — accepted in DDL; values are not generated automatically.
2. **Composite keys** — table-level `PRIMARY KEY (a, b)` is applied as separate per-column indexes, not a single composite constraint.
3. **Foreign key scope** — single-column references only; no `ON DELETE` / `ON UPDATE` actions; referenced table must exist before `CREATE TABLE` on the child.
4. **CHECK constraints** — column-level `CHECK` is stored but the expression is not evaluated; non-`NULL` values are rejected. Table-level `CHECK` is rejected at parse time.

### Internal optimizations (not SQL features)

Recent engine changes that improve performance but do not change client-visible SQL support:

- **`SELECT` hot path** — filters and `ORDER BY` evaluate borrowed row data; only projected columns are cloned into results.
- **Snapshot save** — serializes metadata through locked guards without deep-cloning tables/pages; row data lives in page files.
- **Equality filter fast path** — `WHERE indexed_col = literal` can scan a hash-index candidate set instead of full table scan; tombstoned row IDs are filtered before evaluation.
- **Join index lookup** — equality `ON` clauses such as `u.id = o.user_id` probe the right table's hash index when available.
- **Transaction-aware indexes** — uncommitted index inserts roll back with the transaction; committed deletes leave index entries until auto-vacuum prune reclaims them (unique checks remain correct via MVCC visibility).

---

## Suggested priority order (for contributors)

1. Extended query protocol (prepared statements) — unlocks most clients/ORMs
2. `LIKE` / `IN` / `BETWEEN` in `WHERE`
3. `GROUP BY` / aggregates
4. `RETURNING` clause
5. `FOREIGN KEY` / `CHECK` in `CREATE TABLE` DDL
6. `RIGHT` / `FULL OUTER` / `NATURAL JOIN`
7. Proper PostgreSQL type OIDs in result metadata
8. Auto-increment value generation
