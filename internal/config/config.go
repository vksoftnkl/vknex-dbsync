// Package config reads the runtime options from flags, the environment and .env.
package config

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Config holds every runtime option. Each flag can also be set through an
// environment variable (or a .env file next to the binary / in the working dir).
type Config struct {
	SourceURL       string
	TargetURL       string
	Mode            string // "incremental" | "full"
	Schemas         []string
	Include         []string
	Exclude         []string
	UpdatedCols     []string
	Overlap         time.Duration
	Deletes         bool
	DisableTriggers bool
	DDL             bool
	DDLDrop         bool
	MarkSynced      bool
	MarkTxn         bool
	Instant         bool
	TxnTables       []string
	DryRun          bool
	Yes             bool
	Interval        time.Duration
	LockTimeout     time.Duration
	NaiveTZ         string
	MetaSchema      string
	LogJSON         bool
	LogFile         string
}

// Load reads .env, then the flags (defaulting to the environment), and checks the result.
func Load(args []string) (*Config, error) {
	loadDotEnv(".env")
	if exe, err := os.Executable(); err == nil { // a double-clicked or scheduled binary may start elsewhere
		loadDotEnv(filepath.Join(filepath.Dir(exe), ".env"))
	}

	c := &Config{}
	fs := flag.NewFlagSet("vknex-dbsync", flag.ContinueOnError)

	fs.StringVar(&c.SourceURL, "source", env("SYNC_SOURCE_URL", ""), "source (local) Postgres URL [SYNC_SOURCE_URL]")
	fs.StringVar(&c.TargetURL, "target", env("SYNC_TARGET_URL", ""), "target (live/RDS) Postgres URL [SYNC_TARGET_URL]")
	fs.StringVar(&c.Mode, "mode", env("SYNC_MODE", "incremental"), "incremental | full [SYNC_MODE]")
	schemas := fs.String("schema", env("SYNC_SCHEMA", "public"), "comma-separated schemas to sync [SYNC_SCHEMA]")
	include := fs.String("include", env("SYNC_INCLUDE", ""), "comma-separated tables (name or schema.name) to sync; empty = all [SYNC_INCLUDE]")
	exclude := fs.String("exclude", env("SYNC_EXCLUDE", "_prisma_migrations"), "comma-separated tables (name or schema.name) to skip [SYNC_EXCLUDE]")
	updated := fs.String("updated-cols", env("SYNC_UPDATED_COLS", "updatedAt,updated_at"), "change-tracking column names, first match wins [SYNC_UPDATED_COLS]")
	fs.DurationVar(&c.Overlap, "overlap", envDur("SYNC_OVERLAP", 5*time.Minute), "safety overlap re-read on each incremental run [SYNC_OVERLAP]")
	fs.BoolVar(&c.Deletes, "deletes", envBool("SYNC_DELETES", true), "delete target rows that no longer exist in source [SYNC_DELETES]")
	fs.BoolVar(&c.DisableTriggers, "disable-triggers", envBool("SYNC_DISABLE_TRIGGERS", false), "set session_replication_role=replica on target (skips FK/trigger checks; needs rds_superuser) [SYNC_DISABLE_TRIGGERS]")
	fs.BoolVar(&c.DDL, "ddl", envBool("SYNC_DDL", false), "create/alter target schemas, enum types, tables, partitions, columns, constraints and indexes to match the source [SYNC_DDL]")
	fs.BoolVar(&c.DDLDrop, "ddl-drop", envBool("SYNC_DDL_DROP", false), "with -ddl, also drop target tables, partitions and columns the source no longer has; their data is lost [SYNC_DDL_DROP]")
	fs.BoolVar(&c.MarkSynced, "mark-synced", envBool("SYNC_MARK_SYNCED", false), "after a run, set each table's sync column (…sync_date) on target and source for the rows that were waiting (NULL), and sdr_sync_date in public.sync_deleted_rows [SYNC_MARK_SYNCED]")
	fs.BoolVar(&c.MarkTxn, "mark-txn", envBool("SYNC_MARK_TXN", true), "with -mark-synced, also set the sync column of the -txn-tables; false = only the master tables (the others) [SYNC_MARK_TXN]")
	fs.BoolVar(&c.Instant, "instant", envBool("SYNC_INSTANT", false), "mirror every table not in -txn-tables within seconds of a change on the source; -txn-tables follow -interval [SYNC_INSTANT]")
	txn := fs.String("txn-tables", env("SYNC_TXN_TABLES", ""), "with -instant: comma-separated tables (name or schema.name) synced only every -interval [SYNC_TXN_TABLES]")
	fs.BoolVar(&c.DryRun, "dry-run", envBool("SYNC_DRY_RUN", false), "show the plan and row counts, change nothing [SYNC_DRY_RUN]")
	fs.BoolVar(&c.Yes, "yes", envBool("SYNC_YES", false), "required for -mode full (it truncates target tables) [SYNC_YES]")
	fs.DurationVar(&c.Interval, "interval", envDur("SYNC_INTERVAL", 0), "repeat every interval (e.g. 5m); 0 = run once [SYNC_INTERVAL]")
	fs.DurationVar(&c.LockTimeout, "lock-timeout", envDur("SYNC_LOCK_TIMEOUT", 30*time.Second), "target lock_timeout [SYNC_LOCK_TIMEOUT]")
	fs.StringVar(&c.NaiveTZ, "naive-tz", env("SYNC_NAIVE_TZ", "UTC"), "time zone of 'timestamp without time zone' change columns (Prisma stores UTC) [SYNC_NAIVE_TZ]")
	fs.StringVar(&c.MetaSchema, "meta-schema", env("SYNC_META_SCHEMA", "vknex_sync"), "schema on target holding sync state [SYNC_META_SCHEMA]")
	fs.BoolVar(&c.LogJSON, "log-json", envBool("SYNC_LOG_JSON", false), "JSON logs [SYNC_LOG_JSON]")
	fs.StringVar(&c.LogFile, "log-file", env("SYNC_LOG_FILE", ""), "append the logs to this file instead of stdout; without a console they go to vknex-dbsync.log next to the binary [SYNC_LOG_FILE]")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	c.Schemas = splitList(*schemas)
	c.Include = splitList(*include)
	c.Exclude = splitList(*exclude)
	c.UpdatedCols = splitList(*updated)
	c.TxnTables = splitList(*txn)
	c.Mode = strings.ToLower(strings.TrimSpace(c.Mode))

	switch {
	case len(c.Schemas) == 0:
		return nil, errors.New("at least one schema is required (-schema or SYNC_SCHEMA)")
	case c.SourceURL == "":
		return nil, errors.New("source URL is required (-source or SYNC_SOURCE_URL)")
	case c.TargetURL == "":
		return nil, errors.New("target URL is required (-target or SYNC_TARGET_URL)")
	case c.SourceURL == c.TargetURL:
		return nil, errors.New("source and target URLs are identical")
	case c.DDLDrop && !c.DDL:
		return nil, errors.New("-ddl-drop only works together with -ddl")
	case c.Mode != "incremental" && c.Mode != "full":
		return nil, fmt.Errorf("unknown mode %q (use incremental or full)", c.Mode)
	case c.Mode == "full" && !c.Yes && !c.DryRun:
		return nil, errors.New("-mode full TRUNCATES every synced table on the target; re-run with -yes to confirm (take an RDS snapshot first)")
	case c.Instant && c.Interval <= 0 && !c.DryRun:
		return nil, errors.New("-instant needs -interval: the -txn-tables are synced on that schedule")
	}
	return c, nil
}

// Txn reports whether a table is one of -txn-tables ("table" or "schema.table"),
// which -instant leaves to the -interval runs.
func (c *Config) Txn(schema, name string) bool {
	return slices.Contains(c.TxnTables, name) || slices.Contains(c.TxnTables, schema+"."+name)
}

// Marked reports whether -mark-synced sets a table's sync column: every synced
// table, or with -mark-txn=false only those outside -txn-tables.
func (c *Config) Marked(schema, name string) bool {
	return c.MarkSynced && (c.MarkTxn || !c.Txn(schema, name))
}

// Wanted applies -include / -exclude, which accept "table" or "schema.table".
func (c *Config) Wanted(schema, name string) bool {
	key := schema + "." + name
	if len(c.Include) > 0 && !slices.Contains(c.Include, name) && !slices.Contains(c.Include, key) {
		return false
	}
	return name != c.MetaSchema && !slices.Contains(c.Exclude, name) && !slices.Contains(c.Exclude, key)
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func env(k, def string) string {
	if v, ok := os.LookupEnv(k); ok {
		return v
	}
	return def
}

func envBool(k string, def bool) bool {
	if v, ok := os.LookupEnv(k); ok {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func envDur(k string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(k); ok {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// loadDotEnv reads KEY=VALUE lines; real environment variables win.
func loadDotEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		k = strings.TrimSpace(strings.TrimPrefix(k, "export "))
		v = strings.Trim(strings.TrimSpace(v), `"'`)
		if _, exists := os.LookupEnv(k); !exists {
			os.Setenv(k, v)
		}
	}
}
