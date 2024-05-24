package jsonb

import (
	"database/sql"
	"fmt"
	"reflect"
	"runtime"
	"testing"

	_ "github.com/tailscale/sqlite"
	_ "github.com/tailscale/sqlite/sqliteh"
)

func TestJSONB(t *testing.T) {
	tests := []struct {
		v Value

		// want values
		hdrLen int
		payLen int
		valid  bool
	}{
		{valid: false},
		{Value{0x13, 0x31}, 1, 1, true},
		{Value{0xc3, 0x01, 0x31}, 2, 1, true},
		{Value{0xd3, 0x00, 0x01, 0x31}, 3, 1, true},
		{Value{0xe3, 0x00, 0x00, 0x00, 0x01, 0x31}, 5, 1, true},
		{Value{0xf3, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x31}, 9, 1, true},
		{Value{0xc3, 0x01, 0x31, 'e', 'x', 't', 'r', 'a'}, 2, 1, false},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("test-%d-% 02x", i, []byte(tt.v)), func(t *testing.T) {
			if got := tt.v.HeaderLen(); got != tt.hdrLen {
				t.Errorf("HeaderLen = %v, want %v", got, tt.hdrLen)
			}
			if got := tt.v.PayloadLen(); got != tt.payLen {
				t.Errorf("PayloadLen = %v, want %v", got, tt.payLen)
			}
			if got := tt.v.Valid(); got != tt.valid {
				t.Errorf("Valid = %v, want %v", got, tt.valid)
			}
		})
	}
}

func toJSONB(t testing.TB, json string) Value {
	return valueFromDB(t, "SELECT jsonb(?)", json)
}

func valueFromDB(t testing.TB, sqlExpr string, args ...any) Value {
	db := openTestDB(t)
	defer db.Close()
	t.Helper()
	var b []byte
	if err := db.QueryRow(sqlExpr, args...).Scan(&b); err != nil {
		t.Fatal(err)
	}
	v := Value(b)
	if !v.Valid() {
		t.Fatalf("JSONB Value from DB was not valid")
	}
	return v
}

func TestFromSQLite(t *testing.T) {
	checks := func(fns ...func(testing.TB, Value)) []func(testing.TB, Value) {
		return fns
	}
	isType := func(want Type) func(t testing.TB, v Value) {
		return func(t testing.TB, v Value) {
			t.Helper()
			if got := v.Type(); got != want {
				t.Fatalf("Type = %v, want %v", got, want)
			}
		}
	}
	hasText := func(want string) func(t testing.TB, v Value) {
		return func(t testing.TB, v Value) {
			t.Helper()
			if got := v.Text(); got != want {
				t.Fatalf("Text = %q, want %q", got, want)
			}
		}
	}
	tests := []struct {
		name string

		// one of json or expr must be set
		json string // if non-empty, the JSON to pass to toJSONB
		expr string // if non-empty, the SQL expression to pass to valueFromDB

		checks []func(testing.TB, Value)
	}{
		{name: "null", json: "null", checks: checks(isType(Null))},
		{name: "true", json: "true", checks: checks(isType(True))},
		{name: "false", json: "false", checks: checks(isType(False))},
		{name: "int", json: "123", checks: checks(isType(Int))},
		{name: "int5", json: "0x20", checks: checks(isType(Int5))},
		{name: "float", json: "0.5", checks: checks(isType(Float))},
		{name: "float5", json: ".5", checks: checks(isType(Float5))},
		{name: "text", json: `"foo"`, checks: checks(isType(Text))},
		{name: "text5", json: `"\0"`, checks: checks(isType(Text5), hasText("\x00"))},
		{name: "textj", json: `"foo\nbar"`, checks: checks(isType(TextJ))},
		{name: "textraw", expr: `SELECT jsonb_replace('null','$','example')`, checks: checks(
			isType(TextRaw),
			hasText("example"),
		)},
		{name: "textraw", expr: `SELECT jsonb_replace('null','$','exam"ple')`, checks: checks(
			isType(TextRaw),
			hasText("exam\"ple"),
		)},
		{name: "array", json: `[1,2,3]`, checks: checks(isType(Array))},
		{name: "object", json: `{"foo":123}`, checks: checks(isType(Object))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var v Value
			if tt.json != "" {
				v = Value(toJSONB(t, tt.json))
			} else if tt.expr != "" {
				v = valueFromDB(t, tt.expr)
			} else {
				t.Fatal("json or expr must be set")
			}
			for _, check := range tt.checks {
				check(t, v)
			}
		})
	}

	t.Run("array", func(t *testing.T) {
		in := `[1, 2, 3.5, {"foo":[3,4,false,5]}, "six", true, null, "foo\nbar"]`
		v := Value(toJSONB(t, in))

		if v.Type() != Array {
			t.Fatalf("want array; got %v", v.Type())
		}
		got := []string{}
		want := []string{
			"Int-1",
			"Int-2",
			"Float-3.5",
			"Object",
			"Text-six",
			"True",
			"Null",
			"TextJ-foo\nbar"}
		if err := v.RangeArray(func(v Value) error {
			s := v.Type().String()
			if v.Type().CanText() {
				s += "-" + v.Text()
			}
			if v.Type().CanInt() {
				s += "-" + fmt.Sprint(v.Int())
			}
			if v.Type().CanFloat() {
				s += "-" + fmt.Sprint(v.Float())
			}
			got = append(got, s)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %q; want %q", got, want)
		}
	})

	t.Run("obj", func(t *testing.T) {
		in := `{"null":null,"int":123,"array":[],"t":true,"f":false,"float":123.45,"obj":{}}`
		v := Value(toJSONB(t, in))
		if v.Type() != Object {
			t.Fatalf("want array; got %v", v.Type())
		}
		got := map[string]string{}
		want := map[string]string{
			"null":  "Null",
			"int":   "Int",
			"array": "Array",
			"t":     "True",
			"f":     "False",
			"float": "Float",
			"obj":   "Object",
		}
		if err := v.RangeObject(func(k, v Value) error {
			var ks string
			if k.Type().CanText() {
				ks = k.Text()
			}
			got[ks] = v.Type().String()
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("json5", func(t *testing.T) {
		v := Value(toJSONB(t, `.5`))
		t.Logf("type: %v, %q, % 02x", v.Type(), string(v), v)
		t.Logf("payload: %q", v.Payload())

		v = Value(toJSONB(t, `0x20`))
		t.Logf("type: %v, %q, % 02x", v.Type(), string(v), v)
		t.Logf("payload: %q", v.Payload())
	})
}

func openTestDB(t testing.TB) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+t.TempDir()+"/test.db")
	if err != nil {
		t.Fatal(err)
	}
	configDB(t, db)
	return db
}

func configDB(t testing.TB, db *sql.DB) {
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("PRAGMA synchronous=OFF"); err != nil {
		t.Fatal(err)
	}
	numConns := runtime.GOMAXPROCS(0)
	db.SetMaxOpenConns(numConns)
	db.SetMaxIdleConns(numConns)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)
	t.Cleanup(func() { db.Close() })
}
