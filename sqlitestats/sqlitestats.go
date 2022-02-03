// Package sqlstats implements an sqlite.Tacer for collecting debug statistics.
package sqlitestats

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/tailscale/sqlite"
)

// Stats tracks and reports connection stats.
//
// Stats implements sqlite.Tracer and http.Handler
type Stats struct {
	curTxs sync.Map // sqlite.TraceConnID -> *txStats
}

func (s *Stats) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var txs []*txStats
	s.curTxs.Range(func(_, value interface{}) bool {
		txs = append(txs, value.(*txStats))
		return true
	})
	sort.Slice(txs, func(i, j int) bool { return txs[i].start.Before(txs[j].start) })

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(200)
	io.WriteString(w, "<html><head><title>sqlite active transactions</title></head><body><pre>\n")
	fmt.Fprintf(w, "sqlite active transactions (%d):", len(txs))
	now := time.Now()
	for _, tx := range txs {
		ro := ""
		if tx.readOnly {
			ro = "read-only"
		}
		fmt.Fprintf(w, "\n\t%s\t%v\t%s", tx.name, now.Sub(tx.start).Round(time.Millisecond), ro)
	}
	io.WriteString(w, "\n</pre></body</html>")
}

func (s *Stats) Query(prepCtx context.Context, id sqlite.TraceConnID, query string, duration time.Duration, err error) {
	// TODO: collect when the http handler has enabled a short-running trace mode
}

func (s *Stats) BeginTx(beginCtx context.Context, id sqlite.TraceConnID, readOnly bool, err error) {
	name := ""
	if v := beginCtx.Value(txName{}); v != nil {
		name = v.(string)
	}

	if err != nil {
		// Not actually in tx.
		// TODO: Record error.
		return
	}

	curTx := &txStats{
		name:     name,
		start:    time.Now(),
		readOnly: readOnly,
	}
	s.curTxs.Store(id, curTx)
}

func (s *Stats) Commit(id sqlite.TraceConnID, err error) {
	s.txEnd(id, err, "COMMIT")
}

func (s *Stats) Rollback(id sqlite.TraceConnID, err error) {
	s.txEnd(id, err, "ROLLBACK")
}

func (s *Stats) txEnd(id sqlite.TraceConnID, err error, name string) {
	v, ok := s.curTxs.LoadAndDelete(id)
	if !ok {
		panic(fmt.Sprintf("sqlitestats.Commit: unknown TraceConnID: %v", id))
	}
	curTx := v.(*txStats)
	_ = curTx // TODO record in future collector
}

type txStats struct {
	name     string
	start    time.Time
	readOnly bool
}

type txName struct{}

// WithName makes a ctx that instructs the Stats tracer with a transaction name.
func WithName(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, txName{}, name)
}
