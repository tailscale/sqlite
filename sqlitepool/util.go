//go:build cgo

package sqlitepool

import (
	"fmt"
	"strings"

	"github.com/tailscale/sqlite-exp/sqliteh"
)

// CopyAll copies the contents of one database to another.
//
// Traditionally this is done in sqlite by closing the database and copying
// the file. However it can be useful to do it online: a single exclusive
// transaction can cross multiple databases, and if multiple processes are
// using a file, this lets one replace the database without first
// communicating with the other processes, asking them to close the DB first.
//
// The dstSchemaName and srcSchemaName parameters follow the SQLite PRAMGA
// schema-name conventions: https://sqlite.org/pragma.html#syntax
func CopyAll(db sqliteh.DB, dstSchemaName, srcSchemaName string) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("sqlitepool.CopyAll: %w", err)
		}
	}()
	if dstSchemaName == "" {
		dstSchemaName = "main"
	}
	if srcSchemaName == "" {
		srcSchemaName = "main"
	}
	if dstSchemaName == srcSchemaName {
		return fmt.Errorf("source matches destination: %q", srcSchemaName)
	}
	// Filter on sql to avoid auto indexes.
	// See https://www.sqlite.org/schematab.html for sqlite_schema docs.
	rows, err := Query(db, fmt.Sprintf("SELECT name, type, sql FROM %q.sqlite_schema WHERE sql != ''", srcSchemaName))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name, sqlType, sqlText string
		if err := rows.Scan(&name, &sqlType, &sqlText); err != nil {
			return err
		}
		// Regardless of the case or whitespace used in the original
		// create statement (or whether or not "if not exists" is used),
		// the SQL text in the sqlite_schema table always reads:
		// 	"CREATE (TABLE|VIEW|INDEX|TRIGGER) name".
		// We take advantage of that here to rewrite the create
		// statement for a different schema.
		switch sqlType {
		case "index":
			sqlText = strings.TrimPrefix(sqlText, "CREATE INDEX ")
			sqlText = fmt.Sprintf("CREATE INDEX %q.%s", dstSchemaName, sqlText)
			if err := ExecScript(db, sqlText); err != nil {
				return err
			}
		case "table":
			sqlText = strings.TrimPrefix(sqlText, "CREATE TABLE ")
			sqlText = fmt.Sprintf("CREATE TABLE %q.%s", dstSchemaName, sqlText)
			if err := ExecScript(db, sqlText); err != nil {
				return err
			}
			if err := ExecScript(db, fmt.Sprintf("INSERT INTO %q.%q SELECT * FROM %q.%q;", dstSchemaName, name, srcSchemaName, name)); err != nil {
				return err
			}
		case "trigger":
			sqlText = strings.TrimPrefix(sqlText, "CREATE TRIGGER ")
			sqlText = fmt.Sprintf("CREATE TRIGGER %q.%s", dstSchemaName, sqlText)
			if err := ExecScript(db, sqlText); err != nil {
				return err
			}
		case "view":
			sqlText = strings.TrimPrefix(sqlText, "CREATE VIEW ")
			sqlText = fmt.Sprintf("CREATE VIEW %q.%s", dstSchemaName, sqlText)
			if err := ExecScript(db, sqlText); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown sqlite schema type %q for %q", sqlType, name)
		}
	}
	return rows.Err()
}
