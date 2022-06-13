// Package sqlstats implements an SQLite Tracer that collects query stats.
package sqlstats

import (
	"context"
	"expvar"
	"fmt"
	"html"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tailscale/sqlite/sqliteh"
)

// Tracer implements sqlite.Tracer and collects query stats.
//
// To use, pass the tracer object to sqlite.Connector, then start a debug
// web server with http.HandlerFunc(sqlTracer.Handle).
type Tracer struct {
	TxCount        *expvar.Map
	TxCommit       *expvar.Map
	TxCommitError  *expvar.Map
	TxRollback     *expvar.Map
	TxTotalSeconds *expvar.Map

	curTxs sync.Map // TraceConnID -> *connStats

	// Once a query has been seen once, only the read lock
	// is required to update stats.
	//
	// TODO(crawshaw): assuming queries is effectively read-only
	// in the steady state, a sync.Map would be a faster object
	// here.
	mu      sync.RWMutex
	queries map[string]*queryStats // query -> stats
}

type connStats struct {
	mu       sync.Mutex
	why      string
	at       time.Time
	readOnly bool
}

func (t *Tracer) done(s *connStats) (why string, readOnly bool) {
	s.mu.Lock()
	why = s.why
	readOnly = s.readOnly
	at := s.at
	s.why = ""
	s.at = time.Time{}
	s.readOnly = false
	s.mu.Unlock()

	if t.TxTotalSeconds != nil {
		sec := time.Since(at).Seconds()
		t.TxTotalSeconds.AddFloat(why, sec)
		if readOnly {
			t.TxTotalSeconds.AddFloat("read", sec)
		} else {
			t.TxTotalSeconds.AddFloat("write", sec)
		}
	}
	return why, readOnly
}

type queryStats struct {
	query string

	// When inside the queries map all fields must be accessed as atomics.
	count    int64
	errors   int64
	duration int64 // time.Duration
	mean     int64
	// TODO lastErr atomic.Value
}

func (t *Tracer) queryStats(query string) *queryStats {
	t.mu.RLock()
	stats := t.queries[query]
	t.mu.RUnlock()

	if stats != nil {
		return stats
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.queries == nil {
		t.queries = make(map[string]*queryStats)
	}
	stats = t.queries[query]
	if stats == nil {
		stats = &queryStats{query: query}
		t.queries[query] = stats
	}
	return stats
}

func (t *Tracer) collect() (rows []queryStats) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for query, s := range t.queries {
		row := queryStats{
			query:    query,
			count:    atomic.LoadInt64(&s.count),
			errors:   atomic.LoadInt64(&s.errors),
			duration: atomic.LoadInt64(&s.duration),
		}
		row.mean = row.duration / row.count
		rows = append(rows, row)
	}
	return rows
}

func (t *Tracer) Query(prepCtx context.Context, id sqliteh.TraceConnID, query string, duration time.Duration, err error) {
	stats := t.queryStats(query)

	atomic.AddInt64(&stats.count, 1)
	atomic.AddInt64(&stats.duration, int64(duration))
	if err != nil {
		atomic.AddInt64(&stats.errors, 1)
	}
}

func (t *Tracer) connStats(id sqliteh.TraceConnID) *connStats {
	var s *connStats
	v, ok := t.curTxs.Load(id)
	if ok {
		s = v.(*connStats)
	} else {
		s = &connStats{}
		t.curTxs.Store(id, s)
	}
	return s
}

func (t *Tracer) BeginTx(beginCtx context.Context, id sqliteh.TraceConnID, why string, readOnly bool, err error) {
	s := t.connStats(id)

	s.mu.Lock()
	s.why = why
	s.at = time.Now()
	s.readOnly = readOnly
	s.mu.Unlock()

	if t.TxCount != nil {
		t.TxCount.Add(why, 1)
		if readOnly {
			t.TxCount.Add("read", 1)
		} else {
			t.TxCount.Add("write", 1)
		}
	}
}

func (t *Tracer) Commit(id sqliteh.TraceConnID, err error) {
	s := t.connStats(id)
	why, readOnly := t.done(s)
	if err == nil {
		if t.TxCommit != nil {
			t.TxCommit.Add(why, 1)
			if readOnly {
				t.TxCommit.Add("read", 1)
			} else {
				t.TxCommit.Add("write", 1)
			}
		}
	} else {
		if t.TxCommitError != nil {
			t.TxCommitError.Add(why, 1)
			if readOnly {
				t.TxCommitError.Add("read", 1)
			} else {
				t.TxCommitError.Add("write", 1)
			}
		}
	}
}

func (t *Tracer) Rollback(id sqliteh.TraceConnID, err error) {
	s := t.connStats(id)
	why, readOnly := t.done(s)
	if t.TxRollback != nil {
		t.TxRollback.Add(why, 1)
		if readOnly {
			t.TxRollback.Add("read", 1)
		} else {
			t.TxRollback.Add("write", 1)
		}
	}
}

func (t *Tracer) HandleConns(w http.ResponseWriter, r *http.Request) {
	type txSummary struct {
		name     string
		start    time.Time
		readOnly bool
	}
	var summary []txSummary

	t.curTxs.Range(func(k, v any) bool {
		s := v.(*connStats)

		s.mu.Lock()
		summary = append(summary, txSummary{
			name:     s.why,
			start:    s.at,
			readOnly: s.readOnly,
		})
		s.mu.Unlock()

		return true
	})

	sort.Slice(summary, func(i, j int) bool { return summary[i].start.Before(summary[j].start) })

	now := time.Now()

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(200)
	fmt.Fprintf(w, "<!DOCTYPE html><html><title>sqlite conns</title><body>\n")
	fmt.Fprintf(w, "<p>outstanding sqlite transactions: %d</p>\n", len(summary))
	fmt.Fprintf(w, "<pre>\n")
	for _, s := range summary {
		rw := ""
		if !s.readOnly {
			rw = " read-write"
		}
		fmt.Fprintf(w, "\n\t%s (%v)%s", html.EscapeString(s.name), now.Sub(s.start).Round(time.Millisecond), rw)
	}
	fmt.Fprintf(w, "</pre></body></html>\n")
}

func (t *Tracer) Handle(w http.ResponseWriter, r *http.Request) {
	getArgs, _ := url.ParseQuery(r.URL.RawQuery)
	sortParam := strings.TrimSpace(getArgs.Get("sort"))
	rows := t.collect()

	switch sortParam {
	case "", "count":
		sort.Slice(rows, func(i, j int) bool { return rows[i].count > rows[j].count })
	case "query":
		sort.Slice(rows, func(i, j int) bool { return rows[i].query < rows[j].query })
	case "duration":
		sort.Slice(rows, func(i, j int) bool { return rows[i].duration > rows[j].duration })
	case "errors":
		sort.Slice(rows, func(i, j int) bool { return rows[i].errors > rows[j].errors })
	case "mean":
		sort.Slice(rows, func(i, j int) bool { return rows[i].mean > rows[j].mean })
	default:
		http.Error(w, fmt.Sprintf("unknown sort: %q", sortParam), 400)
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(200)
	fmt.Fprintf(w, `<!DOCTYPE html><html><body>
	<p>Trace of SQLite queries run via the github.com/tailscale/sqlite driver.</p>
	<table border="1">
	<tr>
	<th><a href="?sort=query">Query</a></th>
	<th><a href="?sort=count">Count</a></th>
	<th><a href="?sort=duration">Duration</a></th>
	<th><a href="?sort=mean">Mean</a></th>
	<th><a href="?sort=errors">Errors</a></th>
	</tr>
	`)
	for _, row := range rows {
		fmt.Fprintf(w, "<tr><td>%s</td><td>%d</td><td>%s</td><td>%s</td><td>%d</td></tr>\n",
			row.query,
			row.count,
			time.Duration(row.duration).Round(time.Second),
			time.Duration(row.mean).Round(time.Millisecond),
			row.errors,
		)
	}
	fmt.Fprintf(w, "</table></body></html>")
}
