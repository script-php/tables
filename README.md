# Go Tables

A Go package for managing MySQL and MariaDB structure from JSON schemas. Each
application owns and embeds its schema. The package compares it with the database
when its hash changes and keeps a success marker beside the application binary.

Requires Go 1.22 or later. Supported database families are MySQL 5.7 or later and
MariaDB 10.6 or later. The package uses only the standard library; your
application supplies a compatible driver and database connection.

## Use from another Go project

The module path is `github.com/script-php/tables`. Once the repository is pushed
to GitHub, install the library in your consuming project:

```sh
go get github.com/script-php/tables
go get github.com/go-sql-driver/mysql
```

Create your application's schema folder:

```text
my-app/
  go.mod
  main.go
  database/
    users.json
    session.json
```

Embed it in **your application**, then pass it to the library:

```go
package main

import (
    "context"
    "database/sql"
    "embed"
    "log"
    "os"
    "time"

    "github.com/script-php/tables"
    _ "github.com/go-sql-driver/mysql"
)

//go:embed database
var schemaFiles embed.FS

func main() {
    db, err := sql.Open("mysql", os.Getenv("MYSQL_DSN"))
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
    defer cancel()

    manager := tables.NewTables(db, schemaFiles)
    if _, err := manager.Apply(ctx); err != nil {
        log.Fatal(err)
    }
    // Start serving application requests here.
}
```

The library itself uses only the standard library. Your application supplies the
MySQL/MariaDB driver and owns the database connection. It must select an
**existing database**, use UTF-8 (`charset=utf8mb4`), and have the required DDL
permissions. This manager creates tables, not the database or database users.

**Rebuild your application after adding, editing or removing JSON files.** The
schema files become part of your binary; the source folder is not needed at
runtime. The package bundles no application schemas.

## One file per table

The default layout is `database/users.json`, `database/session.json`, and so on.
Each file contains **one table object**, without the outer `"tables"` array:

```json
{
  "name": "users",
  "columns": [
    {"name": "id", "type": "BIGINT UNSIGNED", "auto_increment": true},
    {"name": "email", "type": "VARCHAR(255)"}
  ],
  "primary_key": ["id"]
}
```

`name` is optional in this format: if omitted, `users.json` defines the `users`
table. An explicit `name` takes precedence, which also lets you keep a stable
file name when using `rename_from`. Duplicate table names are rejected.

The loader reads `.json` files directly inside `database/`, combines the table
definitions and sorts them by table name. Subdirectories and non-JSON files are
ignored. The folder must contain at least one JSON table definition. All tables
share the existing `tables.hash`; changing any definition triggers a comparison,
and only required MySQL changes are applied. Foreign keys can reference tables
in other files, regardless of file ordering.

**Removing a JSON file does not delete its MySQL table.** To delete a table,
keep a file containing an explicit deletion until the migration has run:

```json
{"name": "session", "drop": true}
```

### Keep using a single database.json

The original format is still supported. Embed the single file in your application
instead of the folder:

```go
//go:embed database.json
var schemaFiles embed.FS
```

Pass it to the same `tables.NewTables(db, schemaFiles)` constructor. Use the full
`{"tables": [...]}` document shown below. Embed **one source**: either `database/`
or `database.json`. No package source changes are needed to switch formats.

For an already-available JSON document, `manager.Schema = data` overrides the
filesystem with a complete `{"tables": [...]}` schema. Omitting both the source
and this override returns an error; the library never applies a built-in schema.

## Hash behavior

By default, `Apply` uses `tables.hash` in the executable's directory, regardless
of the current working directory. The hash combines normalized schema JSON with
the selected database name and server hostname/port. Formatting-only JSON changes
do not trigger a migration.

- Matching hash: skip database schema inspection and DDL.
- Missing or changed hash: inspect the current structure and apply differences.
- Success: atomically replace the hash file.
- Before the first DDL statement: remove any old success hash so a partial
  migration cannot leave a misleading marker, even if you later run an older binary.
- Failure: return the error and the statements already applied; do not write a
  new success hash.

A normal startup still opens a database connection, detects MySQL or MariaDB,
identifies the target and takes an advisory lock. The lock serializes cooperating
manager instances while checking the marker and applying changes. All work uses
the same connection. Changing database family or major/minor version invalidates
the marker and forces a fresh comparison.

Override the marker location if the binary directory is read-only or several
applications share it:

```go
manager := tables.NewTables(db, schemaFiles)
manager.HashFile = "/var/lib/myapp/tables.hash"
```

The directory must already exist. Give separate applications separate marker
files. After manually changing/restoring/recreating the database, remove the
marker to force another comparison. A matching hash deliberately does not detect
external schema drift.

## JSON schema

The following is the full-schema format for `database.json`; in folder mode,
put each object from its `tables` array into a separate file:

```json
{
  "tables": [
    {
      "name": "users",
      "engine": "InnoDB",
      "charset": "utf8mb4",
      "collation": "utf8mb4_unicode_ci",
      "comment": "Application users",
      "columns": [
        {"name": "id", "type": "BIGINT UNSIGNED", "auto_increment": true},
        {"name": "email", "type": "VARCHAR(255)"},
        {"name": "nickname", "type": "VARCHAR(100)", "nullable": true},
        {"name": "active", "type": "BOOLEAN", "default": true},
        {"name": "price", "type": "DECIMAL(10,2)", "default": 0},
        {"name": "status", "type": "ENUM", "enum": ["active", "inactive"]},
        {"name": "metadata", "type": "JSON", "nullable": true},
        {"name": "created_at", "type": "TIMESTAMP", "default_expression": "CURRENT_TIMESTAMP"},
        {"name": "updated_at", "type": "DATETIME(6)", "default_expression": "CURRENT_TIMESTAMP(6)", "on_update": "CURRENT_TIMESTAMP(6)"}
      ],
      "primary_key": ["id"],
      "indexes": [
        {"name": "uq_users_email", "columns": ["email"], "unique": true}
      ]
    },
    {
      "name": "orders",
      "columns": [
        {"name": "id", "type": "BIGINT UNSIGNED", "auto_increment": true},
        {"name": "user_id", "type": "BIGINT UNSIGNED"}
      ],
      "primary_key": ["id"],
      "foreign_keys": [
        {
          "name": "fk_orders_users",
          "columns": ["user_id"],
          "references": "users",
          "reference_columns": ["id"],
          "on_delete": "CASCADE",
          "on_update": "RESTRICT"
        }
      ]
    }
  ]
}
```

### Supported properties

- Columns: common MySQL/MariaDB scalar types, `ENUM`, nullability, auto-increment, scalar
  defaults, timestamp defaults/updates and comments. Columns are **NOT NULL** by
  default. Use `nullable: true` when needed.
- `default`: a JSON string, number, boolean, or `null`. `null` requires a nullable
  column. A declared column's omitted default means no explicit default. Timestamp
  SQL expressions belong in `default_expression`, not `default`.
- `default_expression` and `on_update`: `CURRENT_TIMESTAMP`, optionally with
  parentheses or precision 0–6. Arbitrary SQL expressions are rejected.
- Indexes: ordinary BTREE, unique/composite, `"kind": "FULLTEXT"`, or
  `"kind": "SPATIAL"`. The database validates type-specific restrictions.
- `primary_key`: one or more column names.
- Foreign keys: composite keys and `RESTRICT`, `CASCADE`, or `SET NULL` actions.
- Table options: engine, default charset/collation and comment. Changing the table
  default charset does not convert existing columns.

Unknown properties and unsafe identifiers are rejected before DDL. Identifiers
use ASCII letters, digits and underscores, start with a letter or underscore,
and have at most 64 characters. There is no SQL-string interpolation of values
from outside the schema.

## Explicit deletion

**Omitting a table, column, index or foreign key preserves it.** A declared column
is a complete definition: include its type, nullability, default and comment as
needed. Changing a named index or foreign key replaces that definition.

Delete selected objects explicitly:

```json
{
  "tables": [
    {
      "name": "users",
      "drop_columns": ["legacy_name"],
      "drop_indexes": ["old_index"],
      "drop_foreign_keys": ["old_foreign_key"]
    },
    {"name": "obsolete_table", "drop": true}
  ]
}
```

Use `"drop_indexes": ["PRIMARY"]` to remove a primary key. Remove the corresponding
auto-increment property in the same update when needed. A deletion cannot also
appear as a desired definition. Repeating an already-completed deletion is safe.
Dropping columns/tables removes their stored data; the database may also remove indexes
on a dropped column.

## Renames

Table:

```json
{"name": "members", "rename_from": "users"}
```

Column, inside the table's `columns` array:

```json
{"name": "display_name", "rename_from": "name", "type": "VARCHAR(100)"}
```

Include the rest of the desired structure as usual. Rename declarations can stay
in the schema: once the destination exists and the source is gone, later runs
use the destination. If both exist, the migration fails rather than guessing.
For a new installation, supply the columns needed to create the destination table.

## Preview and errors

```go
queries, err := manager.Plan(ctx)
```

`Plan` always reads database metadata. It executes no DDL and writes no hash. It is a
snapshot, so `Apply` builds a fresh plan while holding its lock. Returned statements
assume `NO_BACKSLASH_ESCAPES`; `Apply` sets that mode on its dedicated connection
and restores the previous mode before returning.

MySQL and MariaDB DDL can commit individual statements. A later failure cannot roll back
all earlier changes. `Result.Queries` lists completed statements. Fix the cause
and rerun; planning starts from the actual database structure.

Declared foreign keys are temporarily removed and restored when either managed
table changes. Include dependent foreign keys in the schema when changing their
columns or indexes. Unmanaged foreign keys, incompatible types, existing data,
and other database constraints can prevent an alteration; errors are returned.
Foreign-key checks are never disabled.

Generated columns, expression/prefix indexes, arbitrary default expressions,
column positioning, partitioning and cross-database foreign keys are not managed
by this version. Existing unmentioned objects are left alone.

### MariaDB behavior

MariaDB represents `JSON` as `LONGTEXT` with a `JSON_VALID` check constraint. The
package detects this representation and treats it as the same schema as
`{"type":"JSON"}`, preventing a repeated `MODIFY COLUMN` on every comparison.
JSON storage and comparison semantics still follow MariaDB, rather than MySQL's
binary JSON implementation.

The package rejects MariaDB versions older than 10.6 and MySQL versions older
than 5.7 with a clear error. Live integration tests cover MySQL 8.0.45; the same
integration test can be run against either supported server family.

Reference: [MySQL CREATE TABLE syntax](https://dev.mysql.com/doc/refman/8.4/en/create-table.html).

## Tests

```sh
go test -race ./...
go vet ./...
```

Live tests use unique table names and clean up only those tables. Supply a MySQL
or MariaDB DSN:

```sh
export TABLES_DATABASE_DSN='user:password@tcp(127.0.0.1:3306)/test_database?charset=utf8mb4'
go test -count=1 -tags=integration -v ./...
```

The MySQL driver in `go.mod` is used only by the integration tests. Importing
this package does not register or choose a driver for your application.
