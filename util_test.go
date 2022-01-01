package sqlite

import (
	"context"
	"testing"
)

func TestDropAll(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	err = ExecScript(conn, `
		ATTACH 'file:s1?mode=memory' AS "db two";
		BEGIN;
		CREATE TABLE "db two".customer (
			cust_id INTEGER PRIMARY KEY,
			cust_name TEXT,
			cust_addr TEXT
		);
		CREATE INDEX "db two".custname ON customer (cust_name);
		CREATE VIEW "db two".customer_address AS
			SELECT cust_id, cust_addr FROM "db two".customer;
		CREATE TRIGGER "db two".cust_addr_chng
		INSTEAD OF UPDATE OF cust_addr ON "db two".customer_address
		BEGIN
			UPDATE customer SET cust_addr=NEW.cust_addr
				WHERE cust_id=NEW.cust_id;
		END;

		-- Creates an auto-index we cannot delete.
		CREATE TABLE "db two".textkey (key TEXT PRIMARY KEY, val INTEGER);

		CREATE TABLE customer (
			cust_id INTEGER PRIMARY KEY,
			cust_name TEXT,
			cust_addr TEXT
		);
		CREATE INDEX custname ON customer (cust_name);
		CREATE VIEW customer_address AS
			SELECT cust_id, cust_addr FROM customer;
		CREATE TRIGGER cust_addr_chng
		INSTEAD OF UPDATE OF cust_addr ON customer_address
		BEGIN
			UPDATE customer SET cust_addr=NEW.cust_addr
				WHERE cust_id=NEW.cust_id;
		END;

		COMMIT;`)
	if err != nil {
		t.Fatal(err)
	}

	if err := DropAll(ctx, conn, "db two"); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := conn.QueryRowContext(ctx, "SELECT count(*) FROM \"db two\".sqlite_schema").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("%d unexpected 'db two' schema entries", count)
	}
	if err := conn.QueryRowContext(ctx, "SELECT count(*) FROM main.sqlite_schema").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 4 {
		t.Fatalf("%d main schema entries, want 4", count)
	}

	if err := DropAll(ctx, conn, "main"); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRowContext(ctx, "SELECT count(*) FROM main.sqlite_schema").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("%d unexpected main schema entries", count)
	}
}

func TestCopyAll(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	err = ExecScript(conn, `
		BEGIN;
		CREATE TABLE customer (
			cust_id INTEGER PRIMARY KEY,
			cust_name TEXT,
			cust_addr TEXT
		);
		CREATE INDEX custname ON customer (cust_name);
		CREATE VIEW customer_address AS
			SELECT cust_id, cust_addr FROM customer;
		CREATE TRIGGER cust_addr_chng
		INSTEAD OF UPDATE OF cust_addr ON customer_address
		BEGIN
			UPDATE customer SET cust_addr=NEW.cust_addr
				WHERE cust_id=NEW.cust_id;
		END;
		COMMIT;
		INSERT INTO customer (cust_id, cust_name, cust_addr) VALUES (1, 'joe', 'eldorado');

		-- Creates an auto-index we should not copy.
		CREATE TABLE textkey (key TEXT PRIMARY KEY, val INTEGER);

		ATTACH 'file:s1?mode=memory' AS "db two";
		`)
	if err != nil {
		t.Fatal(err)
	}

	if err := CopyAll(ctx, conn, "db two", "main"); err != nil {
		t.Fatal(err)
	}

	var name string
	if err := conn.QueryRowContext(ctx, "SELECT cust_name FROM \"db two\".customer WHERE cust_id=1").Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "joe" {
		t.Fatalf("name=%q, want %q", name, "joe")
	}
	var count int
	if err := conn.QueryRowContext(ctx, "SELECT count(*) FROM \"db two\".sqlite_schema").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 6 {
		t.Fatalf("dst schema count=%d, want 4", count)
	}
}
