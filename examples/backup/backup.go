package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tailscale/sqlite"
)

var (
	// The way that SQLite3 backups work is that they restart if the database is
	// ever updated from a different context than the one backing up, however
	// modifications made using the same context as the backup can be observed
	// without restarting. The application must just ensure that the connection is
	// either performing queries, or performing a backup step at any given time.
	// The backup step size can be tuned by the application to appropriately
	// share time between the writer and backup operations.
	mu   sync.Mutex
	conn *sql.Conn

	inserted atomic.Int64

	walMode = func(ctx context.Context, conn driver.ConnPrepareContext) error {
		return sqlite.ExecScript(conn.(sqlite.SQLConn), "PRAGMA journal_mode=WAL;")
	}
)

func main() {
	ctx, cancelAndWait := withCancelWait(context.Background())
	db := sql.OpenDB(sqlite.Connector("file:/tmp/example.db", walMode, nil))
	defer db.Close()

	var err error
	conn, err = db.Conn(context.Background())
	must(err)
	defer conn.Close()

	must(initSchema(ctx))

	go fill(ctx)

	log.Printf("sleeping for 10 seconds to populate the table")
	time.Sleep(10 * time.Second)
	log.Printf("inserted: %d", inserted.Load())

	backup(ctx)

	cancelAndWait()
}

func backup(ctx context.Context) {
	bdb := sql.OpenDB(sqlite.Connector("file:/tmp/example-backup.db", walMode, nil))
	defer bdb.Close()
	bConn, err := bdb.Conn(ctx)
	must(err)
	defer bConn.Close()

	log.Printf("backing up")
	b, err := sqlite.NewBackup(bConn, "main", conn, "main")
	must(err)

	var (
		more      bool = true
		remaining int
		pageCount int
	)

	for more {
		mu.Lock()
		more, remaining, pageCount, err = b.Step(1024)
		mu.Unlock()
		if err != nil {
			// fatal errors are returned by finish too
			break
		}
		log.Printf("remaining=%5d pageCount=%5d (inserted: %5d)", remaining, pageCount, inserted.Load())
		time.Sleep(time.Millisecond)
	}
	log.Printf("backup steps done")
	must(b.Finish())
	log.Printf("backup finished")
}

func fill(ctx context.Context) {
	defer done(ctx)
	for alive(ctx) {
		mu.Lock()
		_, err := conn.ExecContext(ctx, "INSERT INTO foo (data) VALUES ('never gunna back you up, never gunna take you down, never gunna alter schema and hurt you');")
		inserted.Add(1)
		mu.Unlock()
		must(err)
	}
}

func initSchema(ctx context.Context) error {
	mu.Lock()
	defer mu.Unlock()
	_, err := conn.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS foo (
			id INTEGER PRIMARY KEY,
			data TEXT
		);
	`)
	return err
}

func must(err error) {
	_, file, no, _ := runtime.Caller(1)
	if err != nil {
		log.Fatalf("%s:%d %#v", file, no, err)
	}
}

var wgKey = &struct{}{}

type waitCtx struct {
	context.Context
	wg *sync.WaitGroup
}

func (c *waitCtx) Done() <-chan struct{} {
	return c.Context.Done()
}

func (c *waitCtx) Err() error {
	return c.Context.Err()
}

func (c *waitCtx) Deadline() (deadline time.Time, ok bool) {
	return c.Context.Deadline()
}

func (c *waitCtx) Value(key interface{}) interface{} {
	if key == wgKey {
		return c.wg
	}
	return c.Context.Value(key)
}

var _ context.Context = &waitCtx{}

func withWait(ctx context.Context) context.Context {
	wg, ok := ctx.Value(wgKey).(*sync.WaitGroup)
	if !ok {
		wg = &sync.WaitGroup{}
		ctx = &waitCtx{ctx, wg}
	}
	wg.Add(1)
	return ctx
}

func alive(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	default:
		return true
	}
}

func wait(ctx context.Context) {
	ctx.Value(wgKey).(*sync.WaitGroup).Wait()
}

func done(ctx context.Context) {
	ctx.Value(wgKey).(*sync.WaitGroup).Done()
}

func withCancelWait(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(withWait(ctx))
	return ctx, func() {
		cancel()
		wait(ctx)
	}
}
