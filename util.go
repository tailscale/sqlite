package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// DropAll deletes all the data from a database.
//
// The schemaName parameter follows the SQLite PRAMGA schema-name conventions:
// https://sqlite.org/pragma.html#syntax
func DropAll(ctx context.Context, conn *sql.Conn, schemaName string) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("sqlitedb.DropAll: %w", err)
		}
	}()

	if schemaName == "" {
		schemaName = "main"
	}

	var indexes, tables, triggers, views []string

	rows, err := conn.QueryContext(ctx, fmt.Sprintf("SELECT name, type FROM %q.sqlite_schema", schemaName))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name, sqlType string
		if err := rows.Scan(&name, &sqlType); err != nil {
			return err
		}
		switch sqlType {
		case "index":
			indexes = append(indexes, name)
		case "table":
			tables = append(tables, name)
		case "trigger":
			triggers = append(triggers, name)
		case "view":
			views = append(views, name)
		default:
			return fmt.Errorf("unknown sqlite schema type %q for %q", sqlType, name)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, name := range indexes {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("DROP INDEX %q.%q", schemaName, name)); err != nil {
			return err
		}
	}
	for _, name := range triggers {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("DROP TRIGGER %q.%q", schemaName, name)); err != nil {
			return err
		}
	}
	for _, name := range views {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("DROP VIEW %q.%q", schemaName, name)); err != nil {
			return err
		}
	}
	for _, name := range tables {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE %q.%q", schemaName, name)); err != nil {
			return err
		}
	}
	return nil
}

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
func CopyAll(ctx context.Context, conn *sql.Conn, dstSchemaName, srcSchemaName string) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("sqlitedb.CopyAll: %w", err)
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
	rows, err := conn.QueryContext(ctx, fmt.Sprintf("SELECT name, type, sql FROM %q.sqlite_schema", srcSchemaName))
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
			if _, err := conn.ExecContext(ctx, sqlText); err != nil {
				return err
			}
		case "table":
			sqlText = strings.TrimPrefix(sqlText, "CREATE TABLE ")
			sqlText = fmt.Sprintf("CREATE TABLE %q.%s", dstSchemaName, sqlText)
			if _, err := conn.ExecContext(ctx, sqlText); err != nil {
				return err
			}
			if _, err := conn.ExecContext(ctx, fmt.Sprintf("INSERT INTO %q.%q SELECT * FROM %q.%q;", dstSchemaName, name, srcSchemaName, name)); err != nil {
				return err
			}
		case "trigger":
			sqlText = strings.TrimPrefix(sqlText, "CREATE TRIGGER ")
			sqlText = fmt.Sprintf("CREATE TRIGGER %q.%s", dstSchemaName, sqlText)
			if _, err := conn.ExecContext(ctx, sqlText); err != nil {
				return err
			}
		case "view":
			sqlText = strings.TrimPrefix(sqlText, "CREATE VIEW ")
			sqlText = fmt.Sprintf("CREATE VIEW %q.%s", dstSchemaName, sqlText)
			if _, err := conn.ExecContext(ctx, sqlText); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown sqlite schema type %q for %q", sqlType, name)
		}
	}
	return rows.Err()
}
