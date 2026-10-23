package status

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vksoftwares/vknex-dbsync/internal/config"
	"github.com/vksoftwares/vknex-dbsync/internal/pg"
)

//go:embed index.html
var indexHTML []byte

// Setting is one configuration value shown on the page.
type Setting struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type server struct {
	st       *Status
	cfg      *config.Config
	log      *slog.Logger
	host     string
	logFile  string
	settings []Setting
	state    cache[map[string]int64] // table key -> last_synced_at, from the target
	runs     cache[[]runRow]         // run_log of the target
}

// Start serves the status page at addr (host:port) until ctx ends and
// returns its URL. It fails only when the address cannot be bound. logFile
// is where the logs go ("" = the console); it is named on the page.
func Start(ctx context.Context, addr string, st *Status, cfg *config.Config, logFile string, log *slog.Logger) (string, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", err
	}
	host, err := os.Hostname()
	if err != nil {
		host = "unknown host"
	}
	s := &server{st: st, cfg: cfg, log: log, host: host, logFile: logFile, settings: settings(cfg, logFile)}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.index)
	mux.HandleFunc("GET /api/status", s.status)
	mux.HandleFunc("GET /api/tables", s.tables)
	mux.HandleFunc("GET /api/runs", s.runLog)
	mux.HandleFunc("POST /api/sync", s.syncNow)
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		stop, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		srv.Shutdown(stop)
	}()
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("status page stopped", "err", err)
		}
	}()
	return URL(ln.Addr().String()), nil
}

// URL is the address of the page served at addr; an empty or wildcard host
// means this machine.
func URL(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "http://" + addr + "/"
	}
	switch host {
	case "", "0.0.0.0", "::":
		host = "127.0.0.1"
	}
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	return fmt.Sprintf("http://%s:%s/", host, port)
}

func (s *server) index(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Write(indexHTML)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(v)
}

type statusResponse struct {
	Host       string    `json:"host"`
	PID        int       `json:"pid"`
	Now        int64     `json:"now"`
	StartedAt  int64     `json:"startedAt"`
	Source     string    `json:"source"`
	Target     string    `json:"target"`
	LogFile    string    `json:"logFile"`
	Settings   []Setting `json:"settings"`
	Runs       Runs      `json:"runs"`
	Instant    Instant   `json:"instant"`
	TableCount int       `json:"tableCount"`
	TablesAt   int64     `json:"tablesAt"`
	Log        []string  `json:"log"`
}

func (s *server) status(w http.ResponseWriter, r *http.Request) {
	lines := 300
	if n, err := strconv.Atoi(r.URL.Query().Get("lines")); err == nil && n > 0 && n <= 1000 {
		lines = n
	}
	snap := s.st.Snapshot()
	writeJSON(w, statusResponse{
		Host:       s.host,
		PID:        os.Getpid(),
		Now:        ms(time.Now()),
		StartedAt:  snap.StartedAt,
		Source:     pg.Describe(s.cfg.SourceURL),
		Target:     pg.Describe(s.cfg.TargetURL),
		LogFile:    s.logFile,
		Settings:   s.settings,
		Runs:       snap.Runs,
		Instant:    snap.Instant,
		TableCount: snap.TableCount,
		TablesAt:   snap.TablesAt,
		Log:        s.st.Log.Lines(lines),
	})
}

type tablesResponse struct {
	Tables []Table `json:"tables"`
	AsOf   int64   `json:"asOf"`  // when the table list was read from the source
	Error  string  `json:"error"` // why the target's sync state could not be read
}

func (s *server) tables(w http.ResponseWriter, r *http.Request) {
	tables, at := s.st.Tables()
	if tables == nil {
		tables = []Table{} // an empty list, not null, for the page
	}
	resp := tablesResponse{Tables: tables, AsOf: ms(at)}
	state, err := s.state.get(5*time.Second, s.readState)
	if err != nil {
		resp.Error = err.Error()
	}
	for i := range tables {
		tables[i].LastSyncedAt = state[tables[i].Key]
	}
	writeJSON(w, resp)
}

// readState reads the watermark of every table from the target.
func (s *server) readState() (map[string]int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := pg.Connect(ctx, s.cfg.TargetURL)
	if err != nil {
		return nil, fmt.Errorf("connect target: %w", err)
	}
	defer conn.Close(context.Background())
	rows, err := conn.Query(ctx, "SELECT schema_name, table_name, last_synced_at FROM "+pg.QName(s.cfg.MetaSchema, "table_state"))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var ns, name string
		var at time.Time
		if err := rows.Scan(&ns, &name, &at); err != nil {
			return nil, err
		}
		out[ns+"."+name] = ms(at)
	}
	return out, rows.Err()
}

type runRow struct {
	ID          int64  `json:"id"`
	Mode        string `json:"mode"`
	SnapshotAt  int64  `json:"snapshotAt"`
	StartedAt   int64  `json:"startedAt"`
	FinishedAt  int64  `json:"finishedAt"`
	TookMs      int64  `json:"tookMs"`
	Tables      int    `json:"tables"`
	RowsRead    int64  `json:"rowsRead"`
	RowsWritten int64  `json:"rowsWritten"`
	RowsDeleted int64  `json:"rowsDeleted"`
}

type runsResponse struct {
	Runs  []runRow `json:"runs"`
	Error string   `json:"error"`
}

func (s *server) runLog(w http.ResponseWriter, r *http.Request) {
	runs, err := s.runs.get(5*time.Second, s.readRuns)
	resp := runsResponse{Runs: runs}
	if err != nil {
		resp.Error = err.Error()
	}
	if resp.Runs == nil {
		resp.Runs = []runRow{}
	}
	writeJSON(w, resp)
}

// readRuns reads the last runs recorded on the target, newest first.
func (s *server) readRuns() ([]runRow, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := pg.Connect(ctx, s.cfg.TargetURL)
	if err != nil {
		return nil, fmt.Errorf("connect target: %w", err)
	}
	defer conn.Close(context.Background())
	rows, err := conn.Query(ctx, "SELECT id, mode, snapshot_at, started_at, finished_at, tables, rows_read, rows_written, rows_deleted FROM "+
		pg.QName(s.cfg.MetaSchema, "run_log")+" ORDER BY id DESC LIMIT 100")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []runRow
	for rows.Next() {
		var r runRow
		var snapshot, started, finished time.Time
		if err := rows.Scan(&r.ID, &r.Mode, &snapshot, &started, &finished, &r.Tables, &r.RowsRead, &r.RowsWritten, &r.RowsDeleted); err != nil {
			return nil, err
		}
		r.SnapshotAt, r.StartedAt, r.FinishedAt = ms(snapshot), ms(started), ms(finished)
		r.TookMs = finished.Sub(started).Milliseconds()
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *server) syncNow(w http.ResponseWriter, r *http.Request) {
	ok := s.st.SyncNow()
	msg := "sync requested"
	if !ok {
		msg = "a run is in progress or already requested"
	} else {
		s.log.Info("sync requested from the status page", "from", r.RemoteAddr)
	}
	writeJSON(w, map[string]any{"ok": ok, "message": msg})
}

// cache keeps one value for a while, so that several open pages do not query
// the target more often than needed.
type cache[T any] struct {
	mu  sync.Mutex
	at  time.Time
	val T
	err error
}

func (c *cache[T]) get(ttl time.Duration, fetch func() (T, error)) (T, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.at.IsZero() && time.Since(c.at) < ttl {
		return c.val, c.err
	}
	c.val, c.err = fetch()
	c.at = time.Now()
	return c.val, c.err
}

// settings lists the configuration for the page, in reading order.
func settings(cfg *config.Config, logFile string) []Setting {
	yn := func(b bool) string {
		if b {
			return "yes"
		}
		return "no"
	}
	list := func(l []string, empty string) string {
		if len(l) == 0 {
			return empty
		}
		return strings.Join(l, ", ")
	}
	var kinds []string
	for _, k := range slices.Sorted(maps.Keys(cfg.KindIntervals)) {
		kinds = append(kinds, k+" every "+cfg.KindIntervals[k].String())
	}
	interval := "run once"
	if cfg.Interval > 0 {
		interval = "every " + cfg.Interval.String()
	}
	master := cfg.TableMaster
	if master == "" {
		master = "off (-txn-tables decides)"
	}
	if logFile == "" {
		logFile = "console"
	}
	return []Setting{
		{"Mode", cfg.Mode},
		{"Interval runs", interval},
		{"Instant mirroring", yn(cfg.Instant)},
		{"Table master", master},
		{"Instant kinds", list(cfg.InstantKinds, "none")},
		{"Kind intervals", list(kinds, "none")},
		{"Transaction tables (-txn-tables)", list(cfg.TxnTables, "none")},
		{"Schemas", list(cfg.Schemas, "")},
		{"Include", list(cfg.Include, "all tables")},
		{"Exclude", list(cfg.Exclude, "none")},
		{"Schema changes (-ddl)", yn(cfg.DDL)},
		{"Drop removed objects (-ddl-drop)", yn(cfg.DDLDrop)},
		{"Mirror deletes", yn(cfg.Deletes)},
		{"Disable triggers on the target", yn(cfg.DisableTriggers)},
		{"Mark synced (-mark-synced)", yn(cfg.MarkSynced)},
		{"Mark transaction tables (-mark-txn)", yn(cfg.MarkTxn)},
		{"Change columns", list(cfg.UpdatedCols, "")},
		{"Overlap", cfg.Overlap.String()},
		{"Lock timeout", cfg.LockTimeout.String()},
		{"Naive timestamp zone", cfg.NaiveTZ},
		{"Sync state schema", cfg.MetaSchema},
		{"Log file", logFile},
	}
}
