// Package tables applies application-owned JSON schemas to MySQL and MariaDB.
// The application supplies an opened *sql.DB and a filesystem containing its
// schema, typically an embed.FS compiled into the application binary.
package tables

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// Tables manages schema only. It never reads or writes application records.
// Construct it once during startup, before serving requests.
type Tables struct {
	DB *sql.DB
	// Source contains database/*.json table files or a single database.json.
	// Pass the application's embed.FS to keep schema files inside its binary.
	Source fs.FS
	// Schema optionally overrides Source with a complete {"tables": [...]} JSON
	// document. There is no bundled or automatically loaded default schema.
	Schema []byte
	// HashFile defaults to tables.hash beside the executable, not the working directory.
	HashFile string
}

// NewTables uses the caller's database connection and schema filesystem.
// It does not open or close the database connection.
func NewTables(db *sql.DB, source fs.FS) *Tables { return &Tables{DB: db, Source: source} }

type Schema struct {
	Tables []Table `json:"tables"`
}
type Table struct {
	Name            string       `json:"name"`
	RenameFrom      string       `json:"rename_from,omitempty"`
	Drop            bool         `json:"drop,omitempty"`
	Columns         []Column     `json:"columns,omitempty"`
	PrimaryKey      []string     `json:"primary_key,omitempty"`
	Indexes         []Index      `json:"indexes,omitempty"`
	ForeignKeys     []ForeignKey `json:"foreign_keys,omitempty"`
	DropColumns     []string     `json:"drop_columns,omitempty"`
	DropIndexes     []string     `json:"drop_indexes,omitempty"`
	DropForeignKeys []string     `json:"drop_foreign_keys,omitempty"`
	Engine          string       `json:"engine,omitempty"`
	Charset         string       `json:"charset,omitempty"`
	Collation       string       `json:"collation,omitempty"`
	Comment         *string      `json:"comment,omitempty"`
}
type Column struct {
	Name          string   `json:"name"`
	RenameFrom    string   `json:"rename_from,omitempty"`
	Type          string   `json:"type"`
	Enum          []string `json:"enum,omitempty"`
	Nullable      bool     `json:"nullable,omitempty"`
	AutoIncrement bool     `json:"auto_increment,omitempty"`
	// Default is a JSON scalar. Omit it for no explicit default; null means DEFAULT NULL.
	Default           json.RawMessage `json:"default,omitempty"`
	DefaultExpression string          `json:"default_expression,omitempty"`
	OnUpdate          string          `json:"on_update,omitempty"`
	Comment           string          `json:"comment,omitempty"`
}
type Index struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Unique  bool     `json:"unique,omitempty"`
	// Kind can be BTREE (default), FULLTEXT, or SPATIAL.
	Kind string `json:"kind,omitempty"`
}
type ForeignKey struct {
	Name             string   `json:"name"`
	Columns          []string `json:"columns"`
	References       string   `json:"references"`
	ReferenceColumns []string `json:"reference_columns"`
	OnDelete         string   `json:"on_delete,omitempty"`
	OnUpdate         string   `json:"on_update,omitempty"`
}
type Result struct {
	Skipped bool
	// Queries contains statements successfully executed, including on a partial failure.
	Queries []string
}
type snapshot struct {
	columns                    map[string]Column
	indexes                    map[string]Index
	foreign                    map[string]ForeignKey
	engine, collation, comment string
}

type serverFlavor uint8

const (
	flavorMySQL serverFlavor = iota
	flavorMariaDB
)

type serverInfo struct {
	flavor serverFlavor
	major  int
	minor  int
	raw    string
}

var identifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
var dataType = regexp.MustCompile(`(?i)^(TINYINT|SMALLINT|MEDIUMINT|INT|INTEGER|BIGINT|DECIMAL|NUMERIC|FLOAT|DOUBLE|REAL|BIT|CHAR|VARCHAR|BINARY|VARBINARY|TINYTEXT|TEXT|MEDIUMTEXT|LONGTEXT|TINYBLOB|BLOB|MEDIUMBLOB|LONGBLOB|DATE|DATETIME|TIMESTAMP|TIME|YEAR|JSON|BOOLEAN|BOOL|GEOMETRY|POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)(\([0-9]+(,[0-9]+)?\))?( UNSIGNED)?$`)
var timestampExpr = regexp.MustCompile(`^CURRENT_TIMESTAMP(\([0-6]?\))?$`)
var integerWidth = regexp.MustCompile(`^(tinyint|smallint|mediumint|int|bigint)\([0-9]+\)`)
var versionNumber = regexp.MustCompile(`([0-9]+)\.([0-9]+)`)

func detectServer(version string) (serverInfo, error) {
	info := serverInfo{raw: version}
	matches := versionNumber.FindAllStringSubmatch(version, -1)
	if len(matches) == 0 {
		return info, fmt.Errorf("cannot parse database server version %q", version)
	}
	match := matches[0]
	if strings.Contains(strings.ToLower(version), "mariadb") {
		info.flavor = flavorMariaDB
		// Some MariaDB servers include a 5.5.5 compatibility prefix. The real
		// MariaDB version is the last version number before the vendor suffix.
		match = matches[len(matches)-1]
	}
	var err error
	if info.major, err = strconv.Atoi(match[1]); err != nil {
		return info, fmt.Errorf("parse database major version: %w", err)
	}
	if info.minor, err = strconv.Atoi(match[2]); err != nil {
		return info, fmt.Errorf("parse database minor version: %w", err)
	}
	if info.flavor == flavorMariaDB {
		if info.major < 10 || info.major == 10 && info.minor < 6 {
			return info, fmt.Errorf("unsupported MariaDB version %s: version 10.6 or later is required", version)
		}
		return info, nil
	}
	if info.major < 5 || info.major == 5 && info.minor < 7 {
		return info, fmt.Errorf("unsupported MySQL version %s: version 5.7 or later is required", version)
	}
	return info, nil
}

func (s serverInfo) marker() string {
	name := "mysql"
	if s.flavor == flavorMariaDB {
		name = "mariadb"
	}
	return fmt.Sprintf("%s-%d.%d", name, s.major, s.minor)
}

func ident(s string) error {
	if len(s) > 64 || !identifier.MatchString(s) {
		return fmt.Errorf("invalid identifier %q (use ASCII letters, digits and underscores; maximum 64 characters)", s)
	}
	return nil
}
func q(s string) string { return "`" + s + "`" }

// Statements use SQL-standard quote doubling. Apply enables
// NO_BACKSLASH_ESCAPES on its dedicated connection and restores it afterwards.
func literal(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
func names(xs []string) string {
	out := make([]string, len(xs))
	for i, s := range xs {
		out[i] = q(s)
	}
	return strings.Join(out, ", ")
}
func normalizedType(s string) string {
	if strings.HasPrefix(strings.ToLower(s), "enum(") {
		return "enum" + s[4:]
	}
	s = strings.ToLower(strings.Join(strings.Fields(s), " "))
	s = strings.ReplaceAll(s, ", ", ",")
	if s == "boolean" || s == "bool" {
		s = "tinyint"
	}
	if s == "integer" || strings.HasPrefix(s, "integer(") || strings.HasPrefix(s, "integer ") {
		s = "int" + s[len("integer"):]
	}
	if strings.HasPrefix(s, "numeric") {
		s = "decimal" + s[len("numeric"):]
	}
	return integerWidth.ReplaceAllString(s, "$1")
}
func expression(s string) (string, error) {
	s = strings.ToUpper(strings.TrimSpace(s))
	if !timestampExpr.MatchString(s) {
		return "", fmt.Errorf("unsupported timestamp expression %q", s)
	}
	return s, nil
}
func defaultValue(c Column) (value string, present bool, err error) {
	if c.DefaultExpression != "" {
		v, e := expression(c.DefaultExpression)
		return v, true, e
	}
	if len(c.Default) == 0 || string(c.Default) == "null" {
		return "", false, nil
	}
	var v any
	d := json.NewDecoder(strings.NewReader(string(c.Default)))
	d.UseNumber()
	if e := d.Decode(&v); e != nil {
		return "", false, e
	}
	switch x := v.(type) {
	case string:
		return x, true, nil
	case json.Number:
		return x.String(), true, nil
	case bool:
		if x {
			return "1", true, nil
		}
		return "0", true, nil
	}
	return "", false, errors.New("default must be a JSON string, number, boolean, or null")
}
func columnSQL(c Column) (string, error) {
	typ := strings.ToUpper(strings.Join(strings.Fields(c.Type), " "))
	if len(c.Enum) > 0 {
		values := make([]string, len(c.Enum))
		for i, v := range c.Enum {
			values[i] = literal(v)
		}
		typ = "ENUM(" + strings.Join(values, ",") + ")"
	} else if !dataType.MatchString(typ) {
		return "", fmt.Errorf("unsupported type %q for column %s", c.Type, c.Name)
	}
	s := q(c.Name) + " " + typ
	if c.Nullable {
		s += " NULL"
	} else {
		s += " NOT NULL"
	}
	if c.AutoIncrement {
		s += " AUTO_INCREMENT"
	}
	if c.DefaultExpression != "" {
		v, e := expression(c.DefaultExpression)
		if e != nil {
			return "", e
		}
		s += " DEFAULT " + v
	} else if len(c.Default) > 0 {
		if string(c.Default) == "null" {
			if !c.Nullable {
				return "", fmt.Errorf("non-null column %s has a null default", c.Name)
			}
			s += " DEFAULT NULL"
		} else {
			v, _, e := defaultValue(c)
			if e != nil {
				return "", e
			}
			s += " DEFAULT " + literal(v)
		}
	}
	if c.OnUpdate != "" {
		v, e := expression(c.OnUpdate)
		if e != nil {
			return "", e
		}
		s += " ON UPDATE " + v
	}
	if c.Comment != "" {
		s += " COMMENT " + literal(c.Comment)
	}
	return s, nil
}
func indexSQL(i Index) string {
	if i.Name == "PRIMARY" {
		return "PRIMARY KEY (" + names(i.Columns) + ")"
	}
	prefix := ""
	if i.Unique {
		prefix = "UNIQUE "
	}
	if i.Kind == "FULLTEXT" || i.Kind == "SPATIAL" {
		prefix = i.Kind + " "
	}
	return prefix + "INDEX " + q(i.Name) + " (" + names(i.Columns) + ")"
}
func action(s string) string {
	if s == "" || s == "NO ACTION" {
		return "RESTRICT"
	}
	return strings.ToUpper(s)
}
func foreignSQL(f ForeignKey) string {
	return "CONSTRAINT " + q(f.Name) + " FOREIGN KEY (" + names(f.Columns) + ") REFERENCES " + q(f.References) + " (" + names(f.ReferenceColumns) + ") ON DELETE " + action(f.OnDelete) + " ON UPDATE " + action(f.OnUpdate)
}

// decodeJSON rejects unknown properties and multiple JSON documents in a file.
func decodeJSON(data []byte, target any) error {
	d := json.NewDecoder(strings.NewReader(string(data)))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return errors.New("expected exactly one JSON object")
	}
	if strings.TrimSpace(string(data)) == "null" {
		return errors.New("expected a JSON object")
	}
	return nil
}

// loadEmbeddedSchema reads table definitions or a full schema from the supplied
// filesystem. An embed.FS keeps this independent of files on disk at runtime.
func loadEmbeddedSchema(source fs.FS) (Schema, error) {
	var schema Schema
	if source == nil {
		return schema, errors.New("schema source is required: pass an embed.FS or set Schema")
	}
	dir, dirErr := fs.Stat(source, "database")
	file, fileErr := fs.Stat(source, "database.json")
	if dirErr != nil && !errors.Is(dirErr, fs.ErrNotExist) {
		return schema, dirErr
	}
	if fileErr != nil && !errors.Is(fileErr, fs.ErrNotExist) {
		return schema, fileErr
	}
	if dirErr == nil && fileErr == nil {
		return schema, errors.New("embed either database/ or database.json, not both")
	}
	if fileErr == nil {
		if file.IsDir() {
			return schema, errors.New("database.json must be a file")
		}
		data, err := fs.ReadFile(source, "database.json")
		if err != nil {
			return schema, err
		}
		if err = decodeJSON(data, &schema); err != nil {
			return schema, fmt.Errorf("database.json: %w", err)
		}
		return schema, nil
	}
	if dirErr != nil {
		return schema, errors.New("no embedded database/ folder or database.json file")
	}
	if !dir.IsDir() {
		return schema, errors.New("database must be a directory")
	}
	entries, err := fs.ReadDir(source, "database")
	if err != nil {
		return schema, err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := "database/" + entry.Name()
		data, err := fs.ReadFile(source, path)
		if err != nil {
			return schema, fmt.Errorf("%s: %w", path, err)
		}
		var table Table
		if err = decodeJSON(data, &table); err != nil {
			return schema, fmt.Errorf("%s: %w", path, err)
		}
		if table.Name == "" {
			table.Name = strings.TrimSuffix(entry.Name(), ".json")
		}
		schema.Tables = append(schema.Tables, table)
	}
	if len(schema.Tables) == 0 {
		return schema, errors.New("database/ must contain at least one .json table definition")
	}
	// File names and directory enumeration order do not affect the combined schema.
	sort.Slice(schema.Tables, func(i, j int) bool { return schema.Tables[i].Name < schema.Tables[j].Name })
	return schema, nil
}

func (m *Tables) decode() (Schema, string, error) {
	var s Schema
	var err error
	if m.Schema == nil {
		s, err = loadEmbeddedSchema(m.Source)
	} else {
		err = decodeJSON(m.Schema, &s)
	}
	if err != nil {
		return s, "", fmt.Errorf("decode schema: %w", err)
	}
	if err = validate(&s); err != nil {
		return s, "", err
	}
	canonical, _ := json.Marshal(s)
	hash := sha256.Sum256(canonical)
	return s, hex.EncodeToString(hash[:]), nil
}

func validate(s *Schema) error {
	seen := map[string]bool{}
	checkList := func(list []string) error {
		used := map[string]bool{}
		for _, n := range list {
			if e := ident(n); e != nil {
				return e
			}
			if used[n] {
				return fmt.Errorf("duplicate name %s", n)
			}
			used[n] = true
		}
		return nil
	}
	for ti := range s.Tables {
		t := &s.Tables[ti]
		if e := ident(t.Name); e != nil {
			return e
		}
		if seen[t.Name] {
			return fmt.Errorf("duplicate table %s", t.Name)
		}
		seen[t.Name] = true
		if t.RenameFrom != "" {
			if e := ident(t.RenameFrom); e != nil {
				return e
			}
			if t.RenameFrom == t.Name {
				return errors.New("rename_from must differ from name")
			}
		}
		if t.Drop {
			if len(t.Columns)+len(t.Indexes)+len(t.ForeignKeys)+len(t.PrimaryKey)+len(t.DropColumns)+len(t.DropIndexes)+len(t.DropForeignKeys) > 0 || t.RenameFrom != "" || t.Engine != "" || t.Charset != "" || t.Collation != "" || t.Comment != nil {
				return fmt.Errorf("dropped table %s must contain only name and drop", t.Name)
			}
			continue
		}
		for _, list := range [][]string{t.PrimaryKey, t.DropColumns, t.DropIndexes, t.DropForeignKeys} {
			if e := checkList(list); e != nil {
				return e
			}
		}
		cols := map[string]bool{}
		sources := map[string]bool{}
		for _, c := range t.Columns {
			if e := ident(c.Name); e != nil {
				return e
			}
			if cols[c.Name] {
				return fmt.Errorf("duplicate column %s.%s", t.Name, c.Name)
			}
			cols[c.Name] = true
			if c.RenameFrom != "" {
				if e := ident(c.RenameFrom); e != nil {
					return e
				}
				if c.RenameFrom == c.Name || sources[c.RenameFrom] {
					return fmt.Errorf("ambiguous column rename %s", c.Name)
				}
				sources[c.RenameFrom] = true
			}
			if len(c.Default) > 0 && c.DefaultExpression != "" {
				return fmt.Errorf("column %s has two defaults", c.Name)
			}
			if c.AutoIncrement && (c.Nullable || len(c.Default) > 0 || c.DefaultExpression != "") {
				return fmt.Errorf("auto_increment column %s cannot be nullable or have a default", c.Name)
			}
			if len(c.Enum) > 0 && !strings.EqualFold(c.Type, "enum") {
				return fmt.Errorf("enum values require type ENUM for %s", c.Name)
			}
			if _, e := columnSQL(c); e != nil {
				return e
			}
		}
		for source := range sources {
			if cols[source] {
				return fmt.Errorf("rename source %s is also a declared column", source)
			}
		}
		for _, n := range t.DropColumns {
			if cols[n] || sources[n] {
				return fmt.Errorf("column %s is both declared/renamed and dropped", n)
			}
		}
		idx := map[string]bool{}
		if len(t.PrimaryKey) > 0 {
			idx["PRIMARY"] = true
		}
		for ii := range t.Indexes {
			i := &t.Indexes[ii]
			if e := ident(i.Name); e != nil {
				return e
			}
			if i.Name == "PRIMARY" {
				return errors.New("use primary_key to define PRIMARY")
			}
			if idx[i.Name] {
				return fmt.Errorf("duplicate index %s", i.Name)
			}
			idx[i.Name] = true
			i.Kind = strings.ToUpper(i.Kind)
			if i.Kind == "" {
				i.Kind = "BTREE"
			}
			if i.Kind != "BTREE" && i.Kind != "FULLTEXT" && i.Kind != "SPATIAL" {
				return fmt.Errorf("invalid index kind %s", i.Kind)
			}
			if i.Unique && i.Kind != "BTREE" {
				return errors.New("unique requires a BTREE index")
			}
			if len(i.Columns) == 0 {
				return errors.New("index requires columns")
			}
			if e := checkList(i.Columns); e != nil {
				return e
			}
		}
		for _, n := range t.DropIndexes {
			if idx[n] {
				return fmt.Errorf("index %s is both defined and dropped", n)
			}
		}
		fk := map[string]bool{}
		for fi := range t.ForeignKeys {
			f := &t.ForeignKeys[fi]
			if e := ident(f.Name); e != nil {
				return e
			}
			if fk[f.Name] {
				return fmt.Errorf("duplicate foreign key %s", f.Name)
			}
			fk[f.Name] = true
			if e := ident(f.References); e != nil {
				return e
			}
			if len(f.Columns) == 0 || len(f.Columns) != len(f.ReferenceColumns) {
				return fmt.Errorf("foreign key %s requires matching column lists", f.Name)
			}
			if e := checkList(f.Columns); e != nil {
				return e
			}
			if e := checkList(f.ReferenceColumns); e != nil {
				return e
			}
			f.OnDelete = action(strings.ToUpper(f.OnDelete))
			f.OnUpdate = action(strings.ToUpper(f.OnUpdate))
			for _, a := range []string{f.OnDelete, f.OnUpdate} {
				if a != "RESTRICT" && a != "CASCADE" && a != "SET NULL" {
					return fmt.Errorf("invalid foreign key action %s", a)
				}
			}
		}
		for _, n := range t.DropForeignKeys {
			if fk[n] {
				return fmt.Errorf("foreign key %s is both defined and dropped", n)
			}
		}
		if t.Charset != "" && t.Collation != "" && !strings.HasPrefix(t.Collation, t.Charset+"_") {
			return fmt.Errorf("table %s charset and collation disagree", t.Name)
		}
		for _, n := range []string{t.Engine, t.Charset, t.Collation} {
			if n != "" {
				if e := ident(n); e != nil {
					return e
				}
			}
		}
	}
	for _, t := range s.Tables {
		if t.RenameFrom != "" && seen[t.RenameFrom] {
			return fmt.Errorf("rename source table %s also appears in schema", t.RenameFrom)
		}
	}
	return nil
}

func mariaDBJSONColumns(ctx context.Context, conn *sql.Conn, db, table string) (map[string]bool, error) {
	rows, err := conn.QueryContext(ctx, `SELECT CHECK_CLAUSE FROM information_schema.CHECK_CONSTRAINTS WHERE CONSTRAINT_SCHEMA=? AND TABLE_NAME=?`, db, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := map[string]bool{}
	for rows.Next() {
		var clause sql.NullString
		if err := rows.Scan(&clause); err != nil {
			return nil, err
		}
		if !clause.Valid {
			continue
		}
		compact := strings.Map(func(r rune) rune {
			if unicode.IsSpace(r) || r == '`' || r == '"' {
				return -1
			}
			return unicode.ToLower(r)
		}, clause.String)
		if strings.HasPrefix(compact, "json_valid(") && strings.HasSuffix(compact, ")") {
			column := strings.TrimSuffix(strings.TrimPrefix(compact, "json_valid("), ")")
			if identifier.MatchString(column) {
				columns[column] = true
			}
		}
	}
	return columns, rows.Err()
}

func decodeMariaDBLiteral(value string) (string, bool) {
	value = strings.TrimSpace(value)
	if len(value) < 2 || value[0] != '\'' || value[len(value)-1] != '\'' {
		return value, false
	}
	value = value[1 : len(value)-1]
	var decoded strings.Builder
	for i := 0; i < len(value); i++ {
		if value[i] == '\'' && i+1 < len(value) && value[i+1] == '\'' {
			decoded.WriteByte('\'')
			i++
			continue
		}
		if value[i] == '\\' && i+1 < len(value) {
			i++
			switch value[i] {
			case '0':
				decoded.WriteByte(0)
			case 'b':
				decoded.WriteByte('\b')
			case 'n':
				decoded.WriteByte('\n')
			case 'r':
				decoded.WriteByte('\r')
			case 't':
				decoded.WriteByte('\t')
			case 'Z':
				decoded.WriteByte(26)
			default:
				decoded.WriteByte(value[i])
			}
			continue
		}
		decoded.WriteByte(value[i])
	}
	return decoded.String(), true
}

func trimExpressionParentheses(value string) string {
	value = strings.TrimSpace(value)
	for len(value) >= 2 && value[0] == '(' && value[len(value)-1] == ')' {
		value = strings.TrimSpace(value[1 : len(value)-1])
	}
	return value
}

// inspect reads metadata only, never table contents.
func inspect(ctx context.Context, conn *sql.Conn, db, name string, server serverInfo) (*snapshot, error) {
	s := &snapshot{columns: map[string]Column{}, indexes: map[string]Index{}, foreign: map[string]ForeignKey{}}
	var kind string
	err := conn.QueryRowContext(ctx, `SELECT TABLE_TYPE, COALESCE(ENGINE,''), COALESCE(TABLE_COLLATION,''), TABLE_COMMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`, db, name).Scan(&kind, &s.engine, &s.collation, &s.comment)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if kind != "BASE TABLE" {
		return nil, fmt.Errorf("%s is not a base table", name)
	}
	jsonColumns := map[string]bool{}
	if server.flavor == flavorMariaDB {
		jsonColumns, err = mariaDBJSONColumns(ctx, conn, db, name)
		if err != nil {
			return nil, fmt.Errorf("inspect MariaDB JSON constraints for %s: %w", name, err)
		}
	}
	rows, err := conn.QueryContext(ctx, `SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA, COLUMN_COMMENT, GENERATION_EXPRESSION FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION`, db, name)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var c Column
		var nullable, extra string
		var def, generated sql.NullString
		if err = rows.Scan(&c.Name, &c.Type, &nullable, &def, &extra, &c.Comment, &generated); err != nil {
			rows.Close()
			return nil, err
		}
		c.Nullable = nullable == "YES"
		c.AutoIncrement = strings.Contains(strings.ToLower(extra), "auto_increment")
		if def.Valid {
			value := def.String
			literalDefault := false
			if server.flavor == flavorMariaDB {
				value, literalDefault = decodeMariaDBLiteral(value)
				if !literalDefault {
					value = trimExpressionParentheses(value)
				}
			}
			if v, e := expression(value); e == nil && !literalDefault && (strings.HasPrefix(strings.ToLower(c.Type), "timestamp") || strings.HasPrefix(strings.ToLower(c.Type), "datetime")) {
				c.DefaultExpression = v
			} else {
				c.Default, _ = json.Marshal(value)
			}
		}
		lower := strings.ToLower(extra)
		if pos := strings.Index(lower, "on update "); pos >= 0 {
			c.OnUpdate = strings.ToUpper(strings.Fields(lower[pos+10:])[0])
		}
		// Never overwrite generated columns through an ordinary column declaration.
		if generated.Valid && generated.String != "" {
			c.Type = "GENERATED:" + c.Type
		} else if server.flavor == flavorMariaDB && normalizedType(c.Type) == "longtext" && jsonColumns[strings.ToLower(c.Name)] {
			// MariaDB implements JSON as LONGTEXT plus JSON_VALID(). Treat that
			// representation as JSON so an unchanged schema remains idempotent.
			c.Type = "JSON"
		}
		s.columns[c.Name] = c
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	rows, err = conn.QueryContext(ctx, `SELECT INDEX_NAME, NON_UNIQUE, INDEX_TYPE, COLUMN_NAME, SUB_PART FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY INDEX_NAME, SEQ_IN_INDEX`, db, name)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var n, k string
		var nonunique int
		var col sql.NullString
		var part sql.NullInt64
		if err = rows.Scan(&n, &nonunique, &k, &col, &part); err != nil {
			rows.Close()
			return nil, err
		}
		i := s.indexes[n]
		i.Name = n
		i.Unique = nonunique == 0
		i.Kind = strings.ToUpper(k)
		v := col.String
		if !col.Valid || part.Valid {
			v = "UNSUPPORTED:" + v
		}
		i.Columns = append(i.Columns, v)
		s.indexes[n] = i
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	rows, err = conn.QueryContext(ctx, `SELECT k.CONSTRAINT_NAME, k.COLUMN_NAME, k.REFERENCED_TABLE_NAME, k.REFERENCED_COLUMN_NAME, r.DELETE_RULE, r.UPDATE_RULE, k.REFERENCED_TABLE_SCHEMA FROM information_schema.KEY_COLUMN_USAGE k JOIN information_schema.REFERENTIAL_CONSTRAINTS r ON r.CONSTRAINT_SCHEMA=k.CONSTRAINT_SCHEMA AND r.TABLE_NAME=k.TABLE_NAME AND r.CONSTRAINT_NAME=k.CONSTRAINT_NAME WHERE k.TABLE_SCHEMA=? AND k.TABLE_NAME=? AND k.REFERENCED_TABLE_NAME IS NOT NULL ORDER BY k.CONSTRAINT_NAME,k.ORDINAL_POSITION`, db, name)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var n, c, ref, rc, del, upd, refdb string
		if err = rows.Scan(&n, &c, &ref, &rc, &del, &upd, &refdb); err != nil {
			rows.Close()
			return nil, err
		}
		f := s.foreign[n]
		f.Name = n
		f.Columns = append(f.Columns, c)
		f.References = ref
		if refdb != db {
			f.References = refdb + "." + ref
		}
		f.ReferenceColumns = append(f.ReferenceColumns, rc)
		f.OnDelete = action(del)
		f.OnUpdate = action(upd)
		s.foreign[n] = f
	}
	err = rows.Err()
	rows.Close()
	return s, err
}
func sameIndex(a, b Index) bool {
	return a.Unique == b.Unique && a.Kind == b.Kind && strings.Join(a.Columns, "\x00") == strings.Join(b.Columns, "\x00")
}
func sameForeign(a, b ForeignKey) bool {
	aa, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return string(aa) == string(bb)
}
func normExpr(s string) string {
	s = strings.ToUpper(s)
	s = strings.ReplaceAll(s, "()", "")
	return strings.ReplaceAll(s, "(0)", "")
}
func sameColumn(a, b Column) bool {
	typ := b.Type
	if len(b.Enum) > 0 {
		parts := make([]string, len(b.Enum))
		for i, v := range b.Enum {
			parts[i] = "'" + strings.ReplaceAll(strings.ReplaceAll(v, "\\", "\\\\"), "'", "''") + "'"
		}
		typ = "enum(" + strings.Join(parts, ",") + ")"
	}
	av, ap, _ := defaultValue(a)
	bv, bp, _ := defaultValue(b)
	// MySQL returns numeric defaults with normalized scale (for example 0.00).
	if ap && bp && len(b.Default) > 0 && string(b.Default)[0] != '"' {
		if x, ok := new(big.Rat).SetString(av); ok {
			if y, ok := new(big.Rat).SetString(bv); ok && x.Cmp(y) == 0 {
				av = bv
			}
		}
	}
	if a.DefaultExpression != "" && b.DefaultExpression != "" {
		av = normExpr(av)
		bv = normExpr(bv)
	}
	return normalizedType(a.Type) == normalizedType(typ) && a.Nullable == b.Nullable && a.AutoIncrement == b.AutoIncrement && ap == bp && av == bv && normExpr(a.OnUpdate) == normExpr(b.OnUpdate) && a.Comment == b.Comment
}
func dropIndex(table, name string) string {
	if name == "PRIMARY" {
		return "ALTER TABLE " + q(table) + " DROP PRIMARY KEY"
	}
	return "ALTER TABLE " + q(table) + " DROP INDEX " + q(name)
}

// planTable preserves every unmentioned column, index, and foreign key.
func planTable(t Table, old *snapshot) (before, body, after []string, err error) {
	if t.Drop {
		if old != nil {
			keys := make([]string, 0, len(old.foreign))
			for n := range old.foreign {
				keys = append(keys, n)
			}
			sort.Strings(keys)
			for _, n := range keys {
				before = append(before, "ALTER TABLE "+q(t.Name)+" DROP FOREIGN KEY "+q(n))
			}
			body = append(body, "DROP TABLE "+q(t.Name))
		}
		return
	}
	desiredIndexes := append([]Index(nil), t.Indexes...)
	if len(t.PrimaryKey) > 0 {
		desiredIndexes = append(desiredIndexes, Index{Name: "PRIMARY", Columns: t.PrimaryKey, Unique: true, Kind: "BTREE"})
	}
	available := map[string]bool{}
	if old != nil {
		for n := range old.columns {
			available[n] = true
		}
	}
	for _, n := range t.DropColumns {
		delete(available, n)
	}
	for _, c := range t.Columns {
		delete(available, c.RenameFrom)
		available[c.Name] = true
	}
	for _, i := range desiredIndexes {
		for _, n := range i.Columns {
			if !available[n] {
				err = fmt.Errorf("index %s refers to missing column %s.%s", i.Name, t.Name, n)
				return
			}
		}
	}
	for _, f := range t.ForeignKeys {
		for _, n := range f.Columns {
			if !available[n] {
				err = fmt.Errorf("foreign key %s refers to missing column %s.%s", f.Name, t.Name, n)
				return
			}
		}
	}
	if old == nil {
		if len(t.Columns) == 0 {
			err = fmt.Errorf("new table %s requires columns", t.Name)
			return
		}
		defs := []string{}
		for _, c := range t.Columns {
			d, _ := columnSQL(c)
			defs = append(defs, d)
		}
		for _, i := range desiredIndexes {
			defs = append(defs, indexSQL(i))
		}
		stmt := "CREATE TABLE " + q(t.Name) + " (" + strings.Join(defs, ", ") + ")"
		if t.Engine != "" {
			stmt += " ENGINE=" + q(t.Engine)
		}
		if t.Charset != "" {
			stmt += " DEFAULT CHARACTER SET=" + q(t.Charset)
		}
		if t.Collation != "" {
			stmt += " COLLATE=" + q(t.Collation)
		}
		if t.Comment != nil {
			stmt += " COMMENT=" + literal(*t.Comment)
		}
		body = append(body, stmt)
		for _, f := range t.ForeignKeys {
			after = append(after, "ALTER TABLE "+q(t.Name)+" ADD "+foreignSQL(f))
		}
		return
	}
	for _, n := range t.DropForeignKeys {
		if _, ok := old.foreign[n]; ok {
			before = append(before, "ALTER TABLE "+q(t.Name)+" DROP FOREIGN KEY "+q(n))
		}
	}
	for _, f := range t.ForeignKeys {
		existing, ok := old.foreign[f.Name]
		if ok && sameForeign(existing, f) {
			continue
		}
		if ok {
			before = append(before, "ALTER TABLE "+q(t.Name)+" DROP FOREIGN KEY "+q(f.Name))
		}
		after = append(after, "ALTER TABLE "+q(t.Name)+" ADD "+foreignSQL(f))
	}
	// Keep all per-table alterations in one statement so a replacement primary key
	// and AUTO_INCREMENT changes are valid together.
	alterations := []string{}
	for _, n := range t.DropIndexes {
		if _, ok := old.indexes[n]; ok {
			alterations = append(alterations, strings.TrimPrefix(dropIndex(t.Name, n), "ALTER TABLE "+q(t.Name)+" "))
		}
	}
	for _, i := range desiredIndexes {
		existing, ok := old.indexes[i.Name]
		if ok && sameIndex(existing, i) {
			continue
		}
		if ok {
			alterations = append(alterations, strings.TrimPrefix(dropIndex(t.Name, i.Name), "ALTER TABLE "+q(t.Name)+" "))
		}
		alterations = append(alterations, "ADD "+indexSQL(i))
	}
	for _, n := range t.DropColumns {
		if _, ok := old.columns[n]; ok {
			alterations = append(alterations, "DROP COLUMN "+q(n))
		}
	}
	for _, c := range t.Columns {
		existing, ok := old.columns[c.Name]
		source, sourceExists := old.columns[c.RenameFrom]
		if c.RenameFrom != "" && !ok && !sourceExists {
			err = fmt.Errorf("cannot rename %s.%s: neither source nor destination exists", t.Name, c.RenameFrom)
			return
		}
		if c.RenameFrom != "" && sourceExists {
			if ok {
				err = fmt.Errorf("cannot rename %s.%s: destination %s exists", t.Name, c.RenameFrom, c.Name)
				return
			}
			if strings.HasPrefix(source.Type, "GENERATED:") {
				err = errors.New("generated column renames are unsupported")
				return
			}
			d, _ := columnSQL(c)
			alterations = append(alterations, "CHANGE COLUMN "+q(c.RenameFrom)+" "+d)
			continue
		}
		if ok && strings.HasPrefix(existing.Type, "GENERATED:") {
			err = fmt.Errorf("cannot manage generated column %s.%s", t.Name, c.Name)
			return
		}
		d, _ := columnSQL(c)
		if !ok {
			alterations = append(alterations, "ADD COLUMN "+d)
		} else if !sameColumn(existing, c) {
			alterations = append(alterations, "MODIFY COLUMN "+d)
		}
	}
	if t.Engine != "" && !strings.EqualFold(t.Engine, old.engine) {
		alterations = append(alterations, "ENGINE="+q(t.Engine))
	}
	if t.Collation != "" && t.Collation != old.collation {
		alterations = append(alterations, "DEFAULT CHARACTER SET="+q(strings.Split(t.Collation, "_")[0])+" COLLATE="+q(t.Collation))
	} else if t.Charset != "" && !strings.HasPrefix(old.collation, t.Charset+"_") {
		alterations = append(alterations, "DEFAULT CHARACTER SET="+q(t.Charset))
	}
	if t.Comment != nil && *t.Comment != old.comment {
		alterations = append(alterations, "COMMENT="+literal(*t.Comment))
	}
	if len(alterations) > 0 {
		body = append(body, "ALTER TABLE "+q(t.Name)+" "+strings.Join(alterations, ", "))
	}
	return
}

func buildPlan(ctx context.Context, conn *sql.Conn, db string, s Schema, server serverInfo) ([]string, error) {
	var renames, before, body, after []string
	snapshots := map[string]*snapshot{}
	changed := map[string]bool{}
	for _, t := range s.Tables {
		old, err := inspect(ctx, conn, db, t.Name, server)
		if err != nil {
			return nil, err
		}
		if t.RenameFrom != "" {
			source, err := inspect(ctx, conn, db, t.RenameFrom, server)
			if err != nil {
				return nil, err
			}
			if source != nil {
				if old != nil {
					return nil, fmt.Errorf("both rename source %s and destination %s exist", t.RenameFrom, t.Name)
				}
				old = source
				renames = append(renames, "RENAME TABLE "+q(t.RenameFrom)+" TO "+q(t.Name))
				changed[t.Name] = true
			}
		}
		snapshots[t.Name] = old
		b, m, a, err := planTable(t, old)
		if err != nil {
			return nil, fmt.Errorf("table %s: %w", t.Name, err)
		}
		if len(m) > 0 {
			changed[t.Name] = true
		}
		before = append(before, b...)
		body = append(body, m...)
		after = append(after, a...)
	}
	// Temporarily remove declared foreign keys around changes on either side.
	// Re-additions run after every table has reached its desired structure.
	for _, t := range s.Tables {
		old := snapshots[t.Name]
		if old == nil || t.Drop {
			continue
		}
		for _, f := range t.ForeignKeys {
			existing, ok := old.foreign[f.Name]
			if ok && sameForeign(existing, f) && (changed[t.Name] || changed[f.References]) {
				before = append(before, "ALTER TABLE "+q(t.Name)+" DROP FOREIGN KEY "+q(f.Name))
				after = append(after, "ALTER TABLE "+q(t.Name)+" ADD "+foreignSQL(f))
			}
		}
	}
	return append(append(append(renames, before...), body...), after...), nil
}
func (m *Tables) connection(ctx context.Context) (*sql.Conn, string, string, serverInfo, error) {
	if m.DB == nil {
		return nil, "", "", serverInfo{}, errors.New("nil database connection")
	}
	conn, err := m.DB.Conn(ctx)
	if err != nil {
		return nil, "", "", serverInfo{}, err
	}
	var db sql.NullString
	var host, port, version string
	err = conn.QueryRowContext(ctx, "SELECT DATABASE(), @@hostname, @@port, VERSION()").Scan(&db, &host, &port, &version)
	if err != nil || !db.Valid || db.String == "" {
		conn.Close()
		if err == nil {
			err = errors.New("select a database in the MySQL connection configuration")
		}
		return nil, "", "", serverInfo{}, err
	}
	server, err := detectServer(version)
	if err != nil {
		conn.Close()
		return nil, "", "", serverInfo{}, err
	}
	target := host + ":" + port + "/" + db.String + "/" + server.marker()
	return conn, db.String, target, server, nil
}

// Plan always inspects MySQL and returns DDL without executing it or writing the hash.
func (m *Tables) Plan(ctx context.Context) ([]string, error) {
	s, _, err := m.decode()
	if err != nil {
		return nil, err
	}
	conn, db, _, server, err := m.connection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return buildPlan(ctx, conn, db, s, server)
}
func (m *Tables) hashPath() (string, error) {
	if m.HashFile != "" {
		return m.HashFile, nil
	}
	exe, err := os.Executable()
	if err != nil {
		return "", err
	}
	return filepath.Join(filepath.Dir(exe), "tables.hash"), nil
}

// Apply skips metadata inspection and DDL when the schema and database target
// match tables.hash. Delete the marker to force a fresh inspection after manual
// database changes. MySQL DDL is not wrapped in a fictitious rollback transaction:
// a failure may leave some changes applied; no success marker is written.
func (m *Tables) Apply(ctx context.Context) (result Result, err error) {
	s, hash, err := m.decode()
	if err != nil {
		return result, err
	}
	path, err := m.hashPath()
	if err != nil {
		return result, err
	}
	conn, db, target, server, err := m.connection(ctx)
	if err != nil {
		return result, err
	}
	defer conn.Close()
	// The version tag invalidates old markers when migration semantics change.
	sum := sha256.Sum256([]byte("go-tables-v2\x00" + target + "\x00" + hash))
	marker := hex.EncodeToString(sum[:]) + "\n"
	lockSum := sha256.Sum256([]byte(target))
	lockName := "go-tables:" + hex.EncodeToString(lockSum[:])[:48]
	var locked sql.NullInt64
	if err = conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 30)", lockName).Scan(&locked); err != nil {
		return result, err
	}
	if !locked.Valid || locked.Int64 != 1 {
		return result, errors.New("could not acquire database schema lock within 30 seconds")
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var released sql.NullInt64
		releaseErr := conn.QueryRowContext(releaseCtx, "SELECT RELEASE_LOCK(?)", lockName).Scan(&released)
		if releaseErr != nil || !released.Valid || released.Int64 != 1 { // Ensure a pooled connection cannot retain the advisory lock.
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			if err == nil {
				err = fmt.Errorf("release schema lock failed: %v", releaseErr)
			}
		}
	}()
	stored, readErr := os.ReadFile(path)
	if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
		return result, fmt.Errorf("read schema hash: %w", readErr)
	}
	if string(stored) == marker {
		result.Skipped = true
		return result, nil
	}
	queries, err := buildPlan(ctx, conn, db, s, server)
	if err != nil {
		return result, err
	}
	// Standard string escaping must work regardless of the driver's SQL mode.
	var originalMode string
	if err = conn.QueryRowContext(ctx, "SELECT @@SESSION.sql_mode").Scan(&originalMode); err != nil {
		return result, err
	}
	mode := originalMode
	if !strings.Contains(","+mode+",", ",NO_BACKSLASH_ESCAPES,") {
		if mode != "" {
			mode += ","
		}
		mode += "NO_BACKSLASH_ESCAPES"
		if _, err = conn.ExecContext(ctx, "SET SESSION sql_mode = ?", mode); err != nil {
			return result, err
		}
		defer func() {
			restoreCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, restoreErr := conn.ExecContext(restoreCtx, "SET SESSION sql_mode = ?", originalMode); restoreErr != nil {
				_ = conn.Raw(func(any) error { return driver.ErrBadConn })
				if err == nil {
					err = fmt.Errorf("restore SQL mode: %w", restoreErr)
				}
			}
		}()
	}
	// Check marker-directory writability before executing DDL.
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tables-hash-*")
	if err != nil {
		return result, fmt.Errorf("prepare schema hash: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err = tmp.WriteString(marker); err != nil {
		tmp.Close()
		return result, err
	}
	if err = tmp.Sync(); err != nil {
		tmp.Close()
		return result, err
	}
	if err = tmp.Close(); err != nil {
		return result, err
	}
	// Invalidate an old success marker before the first DDL statement. A failed
	// migration can partially change MySQL; even the old binary must then recheck.
	if len(queries) > 0 {
		if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return result, fmt.Errorf("invalidate schema hash: %w", removeErr)
		}
	}
	for _, query := range queries {
		if _, err = conn.ExecContext(ctx, query); err != nil {
			return result, fmt.Errorf("schema change failed (%s): %w", query, err)
		}
		result.Queries = append(result.Queries, query)
	}
	if err = os.Rename(tmpName, path); err != nil {
		return result, fmt.Errorf("schema applied, but saving hash failed: %w", err)
	}
	return result, nil
}
