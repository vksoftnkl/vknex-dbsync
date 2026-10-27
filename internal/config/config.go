// Package config reads the runtime options from flags, the environment and .env.
package config

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/mail"
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
	TableMaster     string                   // "schema.table" classifying the tables, e.g. public.db_table_master
	InstantKinds    []string                 // upper case
	KindIntervals   map[string]time.Duration // upper-case kind -> how often the interval runs move it
	TwoWay          bool                     // also bring changes made on the target to the TwoWayKinds tables back to the source
	TwoWayKinds     []string                 // upper case
	TwoWayWins      string                   // "local" | "live": whose version a row changed on both sides keeps
	TwoWayLiveIDs   int64                    // first id the target's sequences of two-way tables hand out; 0 = the source's next
	DryRun          bool
	Yes             bool
	Interval        time.Duration
	LockTimeout     time.Duration
	NaiveTZ         string
	MetaSchema      string
	LogJSON         bool
	LogFile         string
	UI              string   // host:port of the status page served while running with -interval; "" = none
	MailTo          []string // mailed when a sync fails, works again or the program stops; empty = no mail
	MailFrom        string
	SMTPAddr        string // host:port of the mail server
	SMTPUser        string
	SMTPPassword    string
	MailRepeat      time.Duration // while a sync keeps failing, mail again at most this often; 0 = once
	MailTest        bool
	ClientName      string // saved with the target's database name in ClientDB's client_master; "" = not saved
	ClientDB        string // database on the target's server holding the client list
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
	fs.BoolVar(&c.MarkSynced, "mark-synced", envBool("SYNC_MARK_SYNCED", false), "after a run, set each table's sync column (dtm_sync_col in -table-master, else its …sync_date column) on target and source for the rows that were waiting (NULL), and sdr_sync_date in public.sync_deleted_rows [SYNC_MARK_SYNCED]")
	fs.BoolVar(&c.MarkTxn, "mark-txn", envBool("SYNC_MARK_TXN", true), "with -mark-synced, also set the sync column of the -txn-tables; false = only the master tables (the others) [SYNC_MARK_TXN]")
	fs.BoolVar(&c.Instant, "instant", envBool("SYNC_INSTANT", false), "mirror the master tables (see -table-master) within seconds of a change on the source; the others follow -interval [SYNC_INSTANT]")
	txn := fs.String("txn-tables", env("SYNC_TXN_TABLES", ""), "with -instant and an empty -table-master: comma-separated tables (name or schema.name) synced only every -interval, all others are instant [SYNC_TXN_TABLES]")
	fs.StringVar(&c.TableMaster, "table-master", env("SYNC_TABLE_MASTER", "public.db_table_master"), "source table (schema.name) listing the tables by dtm_kind, with their sync column in dtm_sync_col, read on every run: -instant mirrors the -instant-kinds tables, all others follow -interval (or -kind-intervals); empty = use -txn-tables [SYNC_TABLE_MASTER]")
	kinds := fs.String("instant-kinds", env("SYNC_INSTANT_KINDS", "MASTER,SETTING,METADATA,LEDGER,BALANCE,SYSTEM"), "with -table-master: comma-separated dtm_kind values mirrored within seconds [SYNC_INSTANT_KINDS]")
	every := fs.String("kind-intervals", env("SYNC_KIND_INTERVALS", "LOG:1h"), "with -table-master: comma-separated kind:interval for kinds the interval runs move less often than -interval [SYNC_KIND_INTERVALS]")
	fs.BoolVar(&c.TwoWay, "two-way", envBool("SYNC_TWO_WAY", false), "also copy rows inserted, changed or deleted on the target in the -two-way-kinds tables back to the source; needs -table-master and -mark-synced [SYNC_TWO_WAY]")
	twoWayKinds := fs.String("two-way-kinds", env("SYNC_TWO_WAY_KINDS", "MASTER"), "with -two-way: comma-separated dtm_kind values synced both ways [SYNC_TWO_WAY_KINDS]")
	fs.StringVar(&c.TwoWayWins, "two-way-wins", env("SYNC_TWO_WAY_WINS", "local"), "with -two-way, for a row changed on both sides since the last sync: local keeps the source's version, live takes the target's [SYNC_TWO_WAY_WINS]")
	fs.Int64Var(&c.TwoWayLiveIDs, "two-way-live-ids", envInt("SYNC_TWO_WAY_LIVE_IDS", 0), "with -two-way: the target's sequences of the two-way tables hand out ids from this one on (e.g. 1000000000), so that rows created on each side never get the same id; 0 = they follow the source's [SYNC_TWO_WAY_LIVE_IDS]")
	fs.BoolVar(&c.DryRun, "dry-run", envBool("SYNC_DRY_RUN", false), "show the plan and row counts, change nothing [SYNC_DRY_RUN]")
	fs.BoolVar(&c.Yes, "yes", envBool("SYNC_YES", false), "required for -mode full (it truncates target tables) [SYNC_YES]")
	fs.DurationVar(&c.Interval, "interval", envDur("SYNC_INTERVAL", 0), "repeat every interval (e.g. 5m); 0 = run once [SYNC_INTERVAL]")
	fs.DurationVar(&c.LockTimeout, "lock-timeout", envDur("SYNC_LOCK_TIMEOUT", 30*time.Second), "target lock_timeout [SYNC_LOCK_TIMEOUT]")
	fs.StringVar(&c.NaiveTZ, "naive-tz", env("SYNC_NAIVE_TZ", "UTC"), "time zone of 'timestamp without time zone' change columns (Prisma stores UTC) [SYNC_NAIVE_TZ]")
	fs.StringVar(&c.MetaSchema, "meta-schema", env("SYNC_META_SCHEMA", "vknex_sync"), "schema on target holding sync state [SYNC_META_SCHEMA]")
	fs.BoolVar(&c.LogJSON, "log-json", envBool("SYNC_LOG_JSON", false), "JSON logs [SYNC_LOG_JSON]")
	fs.StringVar(&c.LogFile, "log-file", env("SYNC_LOG_FILE", ""), "append the logs to this file instead of stdout; without a console they go to vknex-dbsync.log next to the binary [SYNC_LOG_FILE]")
	fs.StringVar(&c.UI, "ui", env("SYNC_UI", "127.0.0.1:7645"), "with -interval: serve the status page at this host:port (open http://127.0.0.1:7645/); empty = no page [SYNC_UI]")
	mailTo := fs.String("mail-to", env("SYNC_MAIL_TO", ""), "comma-separated addresses mailed when a sync fails, works again or the program stops; empty = no mail [SYNC_MAIL_TO]")
	fs.StringVar(&c.MailFrom, "mail-from", env("SYNC_MAIL_FROM", ""), "sender address; default -smtp-user [SYNC_MAIL_FROM]")
	fs.StringVar(&c.SMTPAddr, "smtp", env("SYNC_SMTP", ""), "mail server host:port, e.g. smtp.gmail.com:587 (STARTTLS) or :465 (TLS) [SYNC_SMTP]")
	fs.StringVar(&c.SMTPUser, "smtp-user", env("SYNC_SMTP_USER", ""), "mail server login; empty = none [SYNC_SMTP_USER]")
	fs.StringVar(&c.SMTPPassword, "smtp-password", env("SYNC_SMTP_PASSWORD", ""), "mail server password (Gmail: an app password) [SYNC_SMTP_PASSWORD]")
	fs.DurationVar(&c.MailRepeat, "mail-repeat", envDur("SYNC_MAIL_REPEAT", time.Hour), "while a sync keeps failing, mail again at most this often; 0 = once per outage [SYNC_MAIL_REPEAT]")
	fs.BoolVar(&c.MailTest, "mail-test", envBool("SYNC_MAIL_TEST", false), "send a test mail to -mail-to and exit [SYNC_MAIL_TEST]")
	fs.StringVar(&c.ClientName, "client-name", env("SYNC_CLIENT_NAME", ""), "on start, save this client name with the target's database name in public.client_master of -client-db; empty = not saved [SYNC_CLIENT_NAME]")
	fs.StringVar(&c.ClientDB, "client-db", env("SYNC_CLIENT_DB", "vknex_clients"), "database on the target's server holding the client list (public.client_master) [SYNC_CLIENT_DB]")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	c.Schemas = splitList(*schemas)
	c.Include = splitList(*include)
	c.Exclude = splitList(*exclude)
	c.UpdatedCols = splitList(*updated)
	c.TxnTables = splitList(*txn)
	c.InstantKinds = splitList(strings.ToUpper(*kinds))
	c.TwoWayKinds = splitList(strings.ToUpper(*twoWayKinds))
	c.TwoWayWins = strings.ToLower(strings.TrimSpace(c.TwoWayWins))
	ki, err := parseKindIntervals(*every)
	if err != nil {
		return nil, err
	}
	c.KindIntervals = ki
	c.TableMaster = strings.TrimSpace(c.TableMaster)
	c.Mode = strings.ToLower(strings.TrimSpace(c.Mode))
	c.MailTo = splitList(*mailTo)
	c.ClientName = strings.TrimSpace(c.ClientName)
	c.ClientDB = strings.TrimSpace(c.ClientDB)

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
	case c.TableMaster != "" && len(c.InstantKinds) == 0:
		return nil, errors.New("-table-master needs at least one -instant-kinds value, e.g. MASTER")
	case c.TwoWay && c.TableMaster == "":
		return nil, errors.New("-two-way needs -table-master: its dtm_kind picks the tables synced both ways")
	case c.TwoWay && !c.MarkSynced:
		return nil, errors.New("-two-way needs -mark-synced: an empty sync column is how a source row that changed since the last sync is told apart")
	case c.TwoWay && len(c.TwoWayKinds) == 0:
		return nil, errors.New("-two-way needs at least one -two-way-kinds value, e.g. MASTER")
	case c.TwoWayWins != "local" && c.TwoWayWins != "live":
		return nil, fmt.Errorf("unknown -two-way-wins %q (use local or live)", c.TwoWayWins)
	case c.TwoWayLiveIDs < 0:
		return nil, errors.New("-two-way-live-ids must be 0 (off) or the first id handed out on the target, e.g. 1000000000")
	case c.ClientName != "" && c.ClientDB == "":
		return nil, errors.New("-client-name needs -client-db, the database holding the client list, e.g. vknex_clients")
	}
	for kind := range c.KindIntervals {
		if slices.Contains(c.InstantKinds, kind) {
			return nil, fmt.Errorf("-kind-intervals gives %s an interval, but -instant-kinds mirrors it instantly", kind)
		}
	}
	if err := c.checkMail(); err != nil {
		return nil, err
	}
	return c, nil
}

// checkMail fills in and checks the mail settings; without -mail-to nothing is
// mailed. Addresses may carry a name ("Name <a@b>"); only the address is kept.
func (c *Config) checkMail() error {
	if len(c.MailTo) == 0 {
		if c.MailTest {
			return errors.New("-mail-test needs -mail-to")
		}
		return nil
	}
	if c.SMTPAddr == "" {
		return errors.New("-mail-to needs -smtp, the mail server's host:port (e.g. smtp.gmail.com:587)")
	}
	if _, _, err := net.SplitHostPort(c.SMTPAddr); err != nil {
		return fmt.Errorf("-smtp must be host:port, e.g. smtp.gmail.com:587: %w", err)
	}
	if c.MailFrom == "" {
		c.MailFrom = c.SMTPUser
	}
	from, err := mail.ParseAddress(c.MailFrom)
	if err != nil {
		return fmt.Errorf("-mail-from %q is not an address (set it when -smtp-user is a login name, not an address)", c.MailFrom)
	}
	c.MailFrom = from.Address
	for i, to := range c.MailTo {
		a, err := mail.ParseAddress(to)
		if err != nil {
			return fmt.Errorf("-mail-to %q is not an address", to)
		}
		c.MailTo[i] = a.Address
	}
	return nil
}

// parseKindIntervals reads -kind-intervals, e.g. "LOG:1h,AUDIT:24h".
func parseKindIntervals(s string) (map[string]time.Duration, error) {
	out := map[string]time.Duration{}
	for _, p := range splitList(s) {
		kind, every, ok := strings.Cut(p, ":")
		kind = strings.ToUpper(strings.TrimSpace(kind))
		d, err := time.ParseDuration(strings.TrimSpace(every))
		if !ok || kind == "" || err != nil || d <= 0 {
			return nil, fmt.Errorf("-kind-intervals: %q is not kind:interval, e.g. LOG:1h", p)
		}
		out[kind] = d
	}
	return out, nil
}

// TxnListed reports whether a table is one of -txn-tables ("table" or
// "schema.table"), which -instant leaves to the -interval runs when
// -table-master is empty.
func (c *Config) TxnListed(schema, name string) bool {
	return slices.Contains(c.TxnTables, name) || slices.Contains(c.TxnTables, schema+"."+name)
}

// TableMasterName splits -table-master; a bare name is in public.
func (c *Config) TableMasterName() (schema, name string) {
	if s, n, ok := strings.Cut(c.TableMaster, "."); ok {
		return s, n
	}
	return "public", c.TableMaster
}

// Marked reports whether -mark-synced sets a table's sync column: every synced
// table, or with -mark-txn=false only those -instant mirrors (txn false, see
// schema.Table.Txn).
func (c *Config) Marked(txn bool) bool {
	return c.MarkSynced && (c.MarkTxn || !txn)
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

func envInt(k string, def int64) int64 {
	if v, ok := os.LookupEnv(k); ok {
		if n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil {
			return n
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
