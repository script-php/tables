package tables

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
)

func emptySnapshot() *snapshot {
	return &snapshot{columns: map[string]Column{}, indexes: map[string]Index{}, foreign: map[string]ForeignKey{}, engine: "InnoDB", collation: "utf8mb4_unicode_ci"}
}
func allSQL(t *testing.T, table Table, old *snapshot) string {
	t.Helper()
	a, b, c, err := planTable(table, old)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Join(append(append(a, b...), c...), ";\n")
}
func TestApplicationSchemaSource(t *testing.T) {
	s, hash, err := NewTables(nil, fstest.MapFS{"database/users.json": &fstest.MapFile{Data: []byte(`{"name":"users","columns":[{"name":"id","type":"INT"}]}`)}}).decode()
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Tables) == 0 || len(hash) != 64 {
		t.Fatal("missing embedded schema")
	}
}
func TestSchemaHashIgnoresFormatting(t *testing.T) {
	a := &Tables{Schema: []byte(`{"tables":[]}`)}
	b := &Tables{Schema: []byte("{\n  \"tables\": []\n}")}
	_, x, e := a.decode()
	if e != nil {
		t.Fatal(e)
	}
	_, y, e := b.decode()
	if e != nil || x != y {
		t.Fatal("formatting changed hash", e)
	}
}
func TestSchemaRejectsInvalidDefinitions(t *testing.T) {
	for _, schema := range []string{
		`{"tables":[],"typo":true}`,
		`{"tables":[{"name":"bad; DROP TABLE users"}]}`,
		`{"tables":[{"name":"users","drop":true,"columns":[{"name":"id","type":"INT"}]}]}`,
		`{"tables":[{"name":"users","columns":[{"name":"id","type":"INT); DROP TABLE users; --"}]}]}`,
		`{"tables":[{"name":"users","columns":[{"name":"id","type":"INT"}],"drop_columns":["id"]}]}`,
		`{"tables":[{"name":"users","columns":[{"name":"id","type":"INT","default":null}]}]}`,
		`{"tables":[{"name":"users","columns":[{"name":"id","type":"INT","default":{},"nullable":true}]}]}`,
		`{"tables":[{"name":"users","indexes":[{"name":"i","columns":["id"],"kind":"FULLTEXT","unique":true}]}]}`,
		`{"tables":[{"name":"users","charset":"latin1","collation":"utf8mb4_unicode_ci"}]}`,
		`{"tables":[]} {"tables":[]}`,
	} {
		t.Run(schema, func(t *testing.T) {
			if _, _, err := (&Tables{Schema: []byte(schema)}).decode(); err == nil {
				t.Fatal("accepted invalid schema")
			}
		})
	}
}
func TestOmissionNeverDeletes(t *testing.T) {
	old := emptySnapshot()
	old.columns["id"] = Column{Name: "id", Type: "int(11)"}
	old.columns["extra"] = Column{Name: "extra", Type: "text", Nullable: true}
	old.indexes["legacy"] = Index{Name: "legacy", Columns: []string{"id"}, Kind: "BTREE"}
	old.foreign["legacy_fk"] = ForeignKey{Name: "legacy_fk"}
	if sql := allSQL(t, Table{Name: "users", Columns: []Column{{Name: "id", Type: "INT"}}}, old); sql != "" {
		t.Fatalf("unmentioned objects changed: %s", sql)
	}
}
func TestExplicitDropsAndRetry(t *testing.T) {
	old := emptySnapshot()
	old.columns["old"] = Column{Name: "old", Type: "INT"}
	old.indexes["old_index"] = Index{}
	old.foreign["old_fk"] = ForeignKey{}
	table := Table{Name: "users", DropColumns: []string{"old"}, DropIndexes: []string{"old_index"}, DropForeignKeys: []string{"old_fk"}}
	query := allSQL(t, table, old)
	for _, want := range []string{"DROP FOREIGN KEY `old_fk`", "DROP INDEX `old_index`", "DROP COLUMN `old`"} {
		if !strings.Contains(query, want) {
			t.Fatalf("missing %s: %s", want, query)
		}
	}
	if got := allSQL(t, table, emptySnapshot()); got != "" {
		t.Fatalf("retry repeated drops: %s", got)
	}
	if got := allSQL(t, Table{Name: "users", Drop: true}, old); !strings.HasSuffix(got, "DROP TABLE `users`") {
		t.Fatal(got)
	}
	if got := allSQL(t, Table{Name: "users", Drop: true}, nil); got != "" {
		t.Fatal(got)
	}
}
func TestRenameColumnPreservesIntent(t *testing.T) {
	old := emptySnapshot()
	old.columns["old_name"] = Column{Name: "old_name", Type: "VARCHAR(100)"}
	table := Table{Name: "users", Columns: []Column{{Name: "name", RenameFrom: "old_name", Type: "VARCHAR(100)"}}}
	if got := allSQL(t, table, old); !strings.Contains(got, "CHANGE COLUMN `old_name` `name`") {
		t.Fatal(got)
	}
	delete(old.columns, "old_name")
	old.columns["name"] = Column{Name: "name", Type: "VARCHAR(100)"}
	if got := allSQL(t, table, old); got != "" {
		t.Fatal("rename not retryable:", got)
	}
	old.columns["old_name"] = Column{}
	if _, _, _, err := planTable(table, old); err == nil {
		t.Fatal("ambiguous rename accepted")
	}
}
func TestPrimaryKeyReplacementIsOneAlter(t *testing.T) {
	old := emptySnapshot()
	old.columns["id"] = Column{Name: "id", Type: "BIGINT", AutoIncrement: true}
	old.indexes["PRIMARY"] = Index{Name: "PRIMARY", Columns: []string{"id"}, Unique: true, Kind: "BTREE"}
	table := Table{Name: "users", Columns: []Column{{Name: "id", Type: "BIGINT"}, {Name: "new_id", Type: "BIGINT", AutoIncrement: true}}, PrimaryKey: []string{"new_id"}}
	got := allSQL(t, table, old)
	if strings.Count(got, "ALTER TABLE") != 1 || !strings.Contains(got, "DROP PRIMARY KEY") || !strings.Contains(got, "ADD PRIMARY KEY (`new_id`)") {
		t.Fatal(got)
	}
}
func TestChangedForeignKeyIsReplaced(t *testing.T) {
	old := emptySnapshot()
	old.columns["user_id"] = Column{Name: "user_id", Type: "BIGINT"}
	f := ForeignKey{Name: "fk_user", Columns: []string{"user_id"}, References: "users", ReferenceColumns: []string{"id"}, OnDelete: "RESTRICT", OnUpdate: "RESTRICT"}
	old.foreign[f.Name] = f
	f.OnDelete = "CASCADE"
	got := allSQL(t, Table{Name: "orders", ForeignKeys: []ForeignKey{f}}, old)
	if !strings.Contains(got, "DROP FOREIGN KEY `fk_user`") || !strings.Contains(got, "ON DELETE CASCADE") {
		t.Fatal(got)
	}
}
func TestColumnComparisonPreservesDefaultCaseAndPrecision(t *testing.T) {
	a := Column{Name: "status", Type: "varchar(20)", Default: json.RawMessage(`"Active"`)}
	b := a
	b.Default = json.RawMessage(`"active"`)
	if sameColumn(a, b) {
		t.Fatal("case-sensitive default change lost")
	}
	a = Column{Name: "id", Type: "bigint", Default: json.RawMessage(`"9007199254740992"`)}
	b = a
	b.Default = json.RawMessage(`9007199254740993`)
	if sameColumn(a, b) {
		t.Fatal("large integer rounded")
	}
	a = Column{Name: "price", Type: "decimal(10,2)", Default: json.RawMessage(`"0.00"`)}
	b = a
	b.Default = json.RawMessage(`0`)
	if !sameColumn(a, b) {
		t.Fatal("equivalent numeric default differs")
	}
	a = Column{Name: "created", Type: "timestamp", DefaultExpression: "current_timestamp()"}
	b = a
	b.DefaultExpression = "CURRENT_TIMESTAMP"
	if !sameColumn(a, b) {
		t.Fatal("equivalent timestamp expression differs")
	}
}
func TestSQLLiteralEscaping(t *testing.T) {
	c := Column{Name: "value", Type: "VARCHAR(100)", Default: json.RawMessage(`"a'\\b"`), Comment: "it's fine"}
	got, err := columnSQL(c)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, `DEFAULT 'a''\b'`) || !strings.Contains(got, "COMMENT 'it''s fine'") {
		t.Fatal(got)
	}
}
func TestDefaultHashLocation(t *testing.T) {
	got, err := NewTables(nil, nil).hashPath()
	if err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if got != filepath.Join(filepath.Dir(exe), "tables.hash") {
		t.Fatal(got)
	}
}

// Scripted SQL driver exercises the actual database/sql execution path, marker
// writes and failures without adding a driver dependency to this reusable file.
type step struct {
	prefix string
	values [][]driver.Value
	err    error
	exec   bool
}
type script struct {
	t     *testing.T
	steps []step
}
type connector struct{ s *script }

func (c connector) Connect(context.Context) (driver.Conn, error) { return &fakeConn{s: c.s}, nil }
func (c connector) Driver() driver.Driver                        { return fakeDriver{} }

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) { return nil, errors.New("use connector") }

type fakeConn struct{ s *script }

func (c *fakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unexpected prepare") }
func (c *fakeConn) Close() error                        { return nil }
func (c *fakeConn) Begin() (driver.Tx, error) {
	return nil, errors.New("DDL must not pretend to be transactional")
}
func (c *fakeConn) next(query string, exec bool) step {
	c.s.t.Helper()
	if len(c.s.steps) == 0 {
		c.s.t.Fatalf("unexpected SQL: %s", query)
	}
	s := c.s.steps[0]
	c.s.steps = c.s.steps[1:]
	if !strings.HasPrefix(query, s.prefix) || exec != s.exec {
		c.s.t.Fatalf("SQL mismatch: want %q exec=%v; got %q exec=%v", s.prefix, s.exec, query, exec)
	}
	return s
}
func (c *fakeConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	s := c.next(q, false)
	if s.err != nil {
		return nil, s.err
	}
	return &fakeRows{values: s.values}, nil
}
func (c *fakeConn) ExecContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Result, error) {
	s := c.next(q, true)
	return driver.RowsAffected(0), s.err
}

type fakeRows struct{ values [][]driver.Value }

func (r *fakeRows) Columns() []string {
	if len(r.values) == 0 {
		return []string{"empty"}
	}
	out := make([]string, len(r.values[0]))
	for i := range out {
		out[i] = fmt.Sprint(i)
	}
	return out
}
func (r *fakeRows) Close() error { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if len(r.values) == 0 {
		return io.EOF
	}
	copy(dest, r.values[0])
	r.values = r.values[1:]
	return nil
}
func openScript(t *testing.T, steps []step) (*sql.DB, *script) {
	t.Helper()
	s := &script{t: t, steps: steps}
	db := sql.OpenDB(connector{s})
	t.Cleanup(func() {
		db.Close()
		if len(s.steps) > 0 {
			t.Errorf("%d expected SQL steps were not executed; next: %s", len(s.steps), s.steps[0].prefix)
		}
	})
	return db, s
}
func connectSteps() []step {
	return []step{{prefix: "SELECT DATABASE()", values: [][]driver.Value{{"app", "localhost", "3306", "8.0.45"}}}, {prefix: "SELECT GET_LOCK", values: [][]driver.Value{{int64(1)}}}}
}
func releaseStep() step {
	return step{prefix: "SELECT RELEASE_LOCK", values: [][]driver.Value{{int64(1)}}}
}
func createSteps(ddlErr error) []step {
	s := connectSteps()
	return append(s, step{prefix: "SELECT TABLE_TYPE"}, step{prefix: "SELECT @@SESSION.sql_mode", values: [][]driver.Value{{"STRICT_TRANS_TABLES,NO_BACKSLASH_ESCAPES"}}}, step{prefix: "CREATE TABLE `users`", exec: true, err: ddlErr}, releaseStep())
}

const simpleSchema = `{"tables":[{"name":"users","columns":[{"name":"id","type":"INT"}],"primary_key":["id"]}]}`

func TestApplyWritesHashThenSkipsMetadataAndDDL(t *testing.T) {
	db, s := openScript(t, createSteps(nil))
	path := filepath.Join(t.TempDir(), "tables.hash")
	m := &Tables{DB: db, HashFile: path, Schema: []byte(simpleSchema)}
	result, err := m.Apply(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Skipped || len(result.Queries) != 1 {
		t.Fatal(result)
	}
	stored, err := os.ReadFile(path)
	if err != nil || len(stored) != 65 {
		t.Fatal("hash not saved", err)
	}
	s.steps = append(connectSteps(), releaseStep())
	result, err = m.Apply(context.Background())
	if err != nil || !result.Skipped || len(result.Queries) != 0 {
		t.Fatal(result, err)
	}
	// Changing the embedded schema invalidates the marker.
	m.Schema = []byte(strings.Replace(simpleSchema, "INT", "BIGINT", 1))
	s.steps = createSteps(nil)
	result, err = m.Apply(context.Background())
	if err != nil || result.Skipped {
		t.Fatal(result, err)
	}
}
func TestFailedDDLInvalidatesSuccessHash(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprint(existing), func(t *testing.T) {
			db, _ := openScript(t, createSteps(errors.New("DDL rejected")))
			path := filepath.Join(t.TempDir(), "tables.hash")
			if existing {
				if err := os.WriteFile(path, []byte("old hash"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			m := &Tables{DB: db, HashFile: path, Schema: []byte(simpleSchema)}
			_, err := m.Apply(context.Background())
			if err == nil {
				t.Fatal("expected DDL failure")
			}
			_, readErr := os.ReadFile(path)
			if !errors.Is(readErr, os.ErrNotExist) {
				t.Fatal("success hash remains after partial migration failure")
			}
			files, _ := filepath.Glob(filepath.Join(filepath.Dir(path), ".tables-hash-*"))
			if len(files) != 0 {
				t.Fatal("temporary marker leaked")
			}
		})
	}
}
func TestPlanDoesNotExecuteOrWriteMarker(t *testing.T) {
	db, _ := openScript(t, []step{connectSteps()[0], {prefix: "SELECT TABLE_TYPE"}})
	path := filepath.Join(t.TempDir(), "tables.hash")
	m := &Tables{DB: db, HashFile: path, Schema: []byte(simpleSchema)}
	queries, err := m.Plan(context.Background())
	if err != nil || len(queries) != 1 {
		t.Fatal(queries, err)
	}
	if _, err = os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("Plan wrote marker")
	}
}
func TestApplyRestoresSQLModeOnFailure(t *testing.T) {
	steps := connectSteps()
	steps = append(steps, step{prefix: "SELECT TABLE_TYPE"}, step{prefix: "SELECT @@SESSION.sql_mode", values: [][]driver.Value{{"STRICT_TRANS_TABLES"}}}, step{prefix: "SET SESSION sql_mode", exec: true}, step{prefix: "CREATE TABLE", exec: true, err: errors.New("rejected")}, step{prefix: "SET SESSION sql_mode", exec: true}, releaseStep())
	db, _ := openScript(t, steps)
	m := &Tables{DB: db, HashFile: filepath.Join(t.TempDir(), "tables.hash"), Schema: []byte(simpleSchema)}
	if _, err := m.Apply(context.Background()); err == nil {
		t.Fatal("expected failure")
	}
}

func TestEmbeddedDirectoryLoading(t *testing.T) {
	source := fstest.MapFS{
		"database/users.json":   &fstest.MapFile{Data: []byte(`{"columns":[{"name":"id","type":"BIGINT"}]}`)},
		"database/session.json": &fstest.MapFile{Data: []byte(`{"name":"sessions","columns":[{"name":"id","type":"VARCHAR(128)"}]}`)},
		"database/README.md":    &fstest.MapFile{Data: []byte("ignored")},
	}
	schema, err := loadEmbeddedSchema(source)
	if err != nil {
		t.Fatal(err)
	}
	if len(schema.Tables) != 2 || schema.Tables[0].Name != "sessions" || schema.Tables[1].Name != "users" {
		t.Fatalf("unexpected schema: %+v", schema)
	}
	if err = validate(&schema); err != nil {
		t.Fatal(err)
	}
	// Moving a file while retaining its explicit table name preserves the schema.
	original, _ := json.Marshal(schema)
	source["database/z.json"] = source["database/session.json"]
	delete(source, "database/session.json")
	renamed, err := loadEmbeddedSchema(source)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(renamed)
	if string(original) != string(data) {
		t.Fatal("file rename changed normalized schema")
	}
}
func TestEmbeddedSingleFileCompatibility(t *testing.T) {
	schema, err := loadEmbeddedSchema(fstest.MapFS{"database.json": &fstest.MapFile{Data: []byte(simpleSchema)}})
	if err != nil || len(schema.Tables) != 1 || schema.Tables[0].Name != "users" {
		t.Fatal(schema, err)
	}
	// The storage format must not alter the schema hash for the same definition.
	directory, err := loadEmbeddedSchema(fstest.MapFS{"database/users.json": &fstest.MapFile{Data: []byte(`{"name":"users","columns":[{"name":"id","type":"INT"}],"primary_key":["id"]}`)}})
	if err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(schema)
	b, _ := json.Marshal(directory)
	_, ha, err := (&Tables{Schema: a}).decode()
	if err != nil {
		t.Fatal(err)
	}
	_, hb, err := (&Tables{Schema: b}).decode()
	if err != nil || ha != hb {
		t.Fatal("storage format changed hash", err)
	}
}
func TestEmbeddedDirectoryErrors(t *testing.T) {
	cases := []struct {
		name   string
		source fstest.MapFS
		want   string
	}{
		{"missing", fstest.MapFS{}, "no embedded"},
		{"empty", fstest.MapFS{"database/README.md": &fstest.MapFile{}}, "at least one"},
		{"ambiguous", fstest.MapFS{"database.json": &fstest.MapFile{}, "database/users.json": &fstest.MapFile{}}, "not both"},
		{"unknown property", fstest.MapFS{"database/users.json": &fstest.MapFile{Data: []byte(`{"typo":true}`)}}, "database/users.json"},
		{"null", fstest.MapFS{"database/users.json": &fstest.MapFile{Data: []byte(`null`)}}, "database/users.json"},
		{"multiple documents", fstest.MapFS{"database/users.json": &fstest.MapFile{Data: []byte(`{} {}`)}}, "database/users.json"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadEmbeddedSchema(tc.source)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatal(err)
			}
		})
	}
	schema, err := loadEmbeddedSchema(fstest.MapFS{
		"database/a.json": &fstest.MapFile{Data: []byte(`{"name":"users"}`)},
		"database/b.json": &fstest.MapFile{Data: []byte(`{"name":"users"}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = validate(&schema); err == nil || !strings.Contains(err.Error(), "duplicate table") {
		t.Fatal("duplicate table was not rejected", err)
	}
}
func TestEmbeddedDirectoryRemovalPreservesDatabaseTable(t *testing.T) {
	source := fstest.MapFS{
		"database/users.json":   &fstest.MapFile{Data: []byte(`{"name":"users","columns":[{"name":"id","type":"INT"}]}`)},
		"database/session.json": &fstest.MapFile{Data: []byte(`{"name":"session","drop":true}`)},
	}
	schema, err := loadEmbeddedSchema(source)
	if err != nil {
		t.Fatal(err)
	}
	if got := allSQL(t, schema.Tables[0], emptySnapshot()); got != "DROP TABLE `session`" {
		t.Fatal(got)
	}
	delete(source, "database/session.json")
	schema, err = loadEmbeddedSchema(source)
	if err != nil {
		t.Fatal(err)
	}
	// buildPlan only sees users, so it neither inspects nor drops session.
	db, _ := openScript(t, []step{connectSteps()[0], {prefix: "SELECT TABLE_TYPE"}})
	data, _ := json.Marshal(schema)
	queries, err := (&Tables{DB: db, Schema: data}).Plan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(queries) != 1 || !strings.HasPrefix(queries[0], "CREATE TABLE `users`") {
		t.Fatal(queries)
	}
}

func TestLibraryRequiresApplicationSchema(t *testing.T) {
	if _, _, err := NewTables(nil, nil).decode(); err == nil || !strings.Contains(err.Error(), "schema source is required") {
		t.Fatal("missing application schema was accepted", err)
	}
	for _, name := range []string{"first_app", "second_app"} {
		source := fstest.MapFS{"database/" + name + ".json": &fstest.MapFile{Data: []byte(`{"columns":[{"name":"id","type":"INT"}]}`)}}
		db, _ := openScript(t, []step{connectSteps()[0], {prefix: "SELECT TABLE_TYPE"}})
		queries, err := NewTables(db, source).Plan(context.Background())
		if err != nil || len(queries) != 1 || !strings.HasPrefix(queries[0], "CREATE TABLE `"+name+"`") {
			t.Fatal("application schema was not isolated", queries, err)
		}
	}
}

func TestDetectSupportedServers(t *testing.T) {
	tests := []struct {
		version string
		marker  string
	}{
		{"5.7.44", "mysql-5.7"},
		{"8.0.45", "mysql-8.0"},
		{"8.4.7-commercial", "mysql-8.4"},
		{"10.6.24-MariaDB", "mariadb-10.6"},
		{"5.5.5-10.11.14-MariaDB", "mariadb-10.11"},
		{"11.4.10-MariaDB-ubu2404", "mariadb-11.4"},
	}
	for _, test := range tests {
		t.Run(test.version, func(t *testing.T) {
			server, err := detectServer(test.version)
			if err != nil {
				t.Fatal(err)
			}
			if server.marker() != test.marker {
				t.Fatalf("got %s, want %s", server.marker(), test.marker)
			}
		})
	}
	for _, version := range []string{"5.6.51", "10.5.28-MariaDB", "unknown"} {
		if _, err := detectServer(version); err == nil {
			t.Fatalf("accepted unsupported server %s", version)
		}
	}
}

func TestDecodeMariaDBDefaults(t *testing.T) {
	tests := map[string]string{
		`'Guest''s name'`:  "Guest's name",
		`'path\\to\\file'`: `path\to\file`,
		`'line\nfeed'`:     "line\nfeed",
	}
	for encoded, want := range tests {
		got, literal := decodeMariaDBLiteral(encoded)
		if !literal || got != want {
			t.Fatalf("decode %s: got %q, want %q", encoded, got, want)
		}
	}
	if got, literal := decodeMariaDBLiteral("current_timestamp()"); literal || got != "current_timestamp()" {
		t.Fatalf("expression treated as literal: %q", got)
	}
}

func TestMariaDBJSONAliasIsIdempotent(t *testing.T) {
	steps := []step{
		{prefix: "SELECT DATABASE()", values: [][]driver.Value{{"app", "localhost", "3306", "10.11.14-MariaDB"}}},
		{prefix: "SELECT TABLE_TYPE", values: [][]driver.Value{{"BASE TABLE", "InnoDB", "utf8mb4_unicode_ci", ""}}},
		{prefix: "SELECT CHECK_CLAUSE", values: [][]driver.Value{{"json_valid(`payload`)"}}},
		{prefix: "SELECT COLUMN_NAME", values: [][]driver.Value{{"payload", "longtext", "YES", nil, "", "", nil}}},
		{prefix: "SELECT INDEX_NAME"},
		{prefix: "SELECT k.CONSTRAINT_NAME"},
	}
	db, _ := openScript(t, steps)
	m := &Tables{DB: db, Schema: []byte(`{"tables":[{"name":"documents","columns":[{"name":"payload","type":"JSON","nullable":true}]}]}`)}
	queries, err := m.Plan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(queries) != 0 {
		t.Fatalf("MariaDB JSON alias caused repeated migration: %v", queries)
	}
}
