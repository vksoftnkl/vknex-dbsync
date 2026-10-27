// Package syncer runs one sync of the source database into the target.
package syncer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/vksoftwares/vknex-dbsync/internal/config"
	"github.com/vksoftwares/vknex-dbsync/internal/ddl"
	"github.com/vksoftwares/vknex-dbsync/internal/notify"
	"github.com/vksoftwares/vknex-dbsync/internal/pg"
	"github.com/vksoftwares/vknex-dbsync/internal/schema"
	"github.com/vksoftwares/vknex-dbsync/internal/status"
)

// Any constants work; they only stop two syncs of the same kind from writing
// at once. Interval runs and instant runs (-instant) move different tables, so
// each kind has its own lock.
const (
	advisoryLockKey int64 = 0x564B4E4558 // "VKNEX"
	instantLockKey  int64 = advisoryLockKey + 1
	metaLockKey     int64 = advisoryLockKey + 2 // see ensureMeta
	pullLockKey     int64 = advisoryLockKey + 3 // -two-way: one pull at a time, apart from the pushes
)

// Syncer mirrors the source database into the target, one run at a time.
type Syncer struct {
	Status *status.Status // what the runs report for the status page (-ui); nil = no page

	cfg      *config.Config
	log      *slog.Logger
	notify   *notify.Notifier     // mails when the instant sync fails and recovers; nil without -mail-to
	slowDone map[string]time.Time // -kind-intervals kind -> start of the last run that moved it
}

// New returns a Syncer for cfg that logs to log and mails through n (may be nil).
func New(cfg *config.Config, log *slog.Logger, n *notify.Notifier) *Syncer {
	return &Syncer{cfg: cfg, log: log, notify: n, slowDone: map[string]time.Time{}}
}

// due reports whether this interval run moves the tables of kind: always,
// unless -kind-intervals gives the kind an interval of its own that has not
// passed yet (the run nearest to it counts). After a restart every kind is due.
func (s *Syncer) due(kind string) bool {
	every, ok := s.cfg.KindIntervals[kind]
	return !ok || time.Since(s.slowDone[kind]) >= every-s.cfg.Interval/2
}

type runStats struct {
	Copied   int64
	Upserted int64
	Deleted  int64
	Tables   int
	ReAdd    []string // NOT VALID constraints to put back after the data
}

// RunOnce performs one complete, atomic sync. Either every table on the target
// is updated in one transaction, or nothing changes. With -instant it moves the
// data of the interval tables only (Table.Txn; Watch mirrors the others), and
// a -kind-intervals kind only when it is due, unless it changes the schema:
// then every table, while Watch waits. With -two-way and without -instant it
// first pulls the live changes (see pull); with -instant, watchLive does.
func (s *Syncer) RunOnce(ctx context.Context) (err error) {
	started := time.Now()
	cfg := s.cfg
	run := status.Run{Mode: cfg.Mode, StartedAt: started.UnixMilli()}
	s.Status.RunStarted(started)
	defer func() { s.Status.RunFinished(run, err) }()

	src, err := pg.Connect(ctx, cfg.SourceURL)
	if err != nil {
		return fmt.Errorf("connect source: %w", err)
	}
	defer src.Close(context.Background())

	dst, err := pg.Connect(ctx, cfg.TargetURL)
	if err != nil {
		return fmt.Errorf("connect target: %w", err)
	}
	defer dst.Close(context.Background())

	if err := guardDifferent(ctx, src, dst); err != nil {
		return err
	}
	// Checked on the source: no round trip to the target, and both servers know the same zone names.
	if _, err := src.Exec(ctx, "SELECT now() AT TIME ZONE $1", cfg.NaiveTZ); err != nil {
		return fmt.Errorf("invalid -naive-tz %q: %w", cfg.NaiveTZ, err)
	}
	if cfg.TwoWay && !cfg.Instant && !cfg.DryRun {
		// Before the snapshot, so that this run pushes what it pulled. A failed
		// pull holds up nothing: the push leaves those live rows alone.
		err := s.ensureMeta(ctx, dst)
		if err == nil {
			err = s.pull(ctx, src, dst)
		}
		switch {
		case err == nil:
			s.notify.Recovered("two-way pull")
		case errors.Is(err, errBusy): // another sync pulls
		default:
			s.log.Error("pull from the live DB failed; its changes wait for the next run", "err", err)
			s.notify.Failed("two-way pull", err)
		}
	}

	srcTx, snapshot, err := beginSnapshot(ctx, src)
	if err != nil {
		return err
	}
	defer srcTx.Rollback(context.Background())

	var srcAll map[string]*schema.Table
	var srcInfo *ddl.Info
	if err := schema.CatalogPath(ctx, srcTx, func() error {
		var err error
		if srcAll, err = schema.Load(ctx, srcTx, cfg.Schemas); err != nil {
			return fmt.Errorf("source schema: %w", err)
		}
		if cfg.DDL {
			srcInfo, err = ddl.LoadInfo(ctx, srcTx, cfg.Schemas)
		}
		return err
	}); err != nil {
		return err
	}
	master, err := schema.LoadMaster(ctx, srcTx, cfg)
	if err != nil {
		return err
	}
	dstAll, dstInfo, err := readTarget(ctx, dst, cfg)
	if err != nil {
		return err
	}

	tables, warnings := schema.Select(srcAll, cfg, master)
	if len(tables) == 0 {
		return errors.New("no tables selected; check -schema / -include / -exclude")
	}
	for _, w := range warnings {
		s.log.Warn("sync column", "detail", w)
	}
	s.reportTables(tables)

	changes := &ddl.Plan{}
	if cfg.DDL {
		if changes, err = ddl.Build(ctx, dst, tables, dstAll, srcInfo, dstInfo, cfg); err != nil {
			return err
		}
	} else {
		problems, warnings := schema.Compare(tables, dstAll)
		for _, w := range warnings {
			s.log.Warn("schema", "detail", w)
		}
		if len(problems) > 0 {
			return fmt.Errorf("source and target schemas differ; re-run with -ddl to apply the source's structure to the target:\n  - %s",
				strings.Join(problems, "\n  - "))
		}
	}

	// The tables whose data this run moves: with -instant the interval tables
	// only, and a -kind-intervals kind only when it is due. Constraints and NOT
	// NULL added after the data need every table's rows current, so a run that
	// changes the schema moves them all. A dry run shows them all.
	data := tables
	whole := cfg.DryRun || changes.Count() > 0
	if !whole {
		data = schema.Subset(tables, func(t *schema.Table) bool { return (t.Txn || !cfg.Instant) && s.due(t.Kind) })
	}
	var slow []string // the -kind-intervals kinds this run moves
	for _, t := range data {
		if _, ok := cfg.KindIntervals[t.Kind]; ok && !slices.Contains(slow, t.Kind) {
			slow = append(slow, t.Kind)
		}
	}
	slices.Sort(slow)
	order, err := s.order(data)
	if err != nil {
		return err
	}

	srcSeq, err := readSourceSequences(ctx, srcTx, data)
	if err != nil {
		return err
	}
	marks := &syncMarks{}
	if cfg.MarkSynced {
		if marks, err = readPending(ctx, srcTx, data); err != nil {
			return err
		}
	}

	if cfg.DryRun {
		for _, q := range slices.Concat(changes.EnumValues, changes.Before) {
			s.log.Info("schema change (before data)", "sql", q)
		}
		for _, q := range changes.After {
			s.log.Info("schema change (after data)", "sql", q)
		}
		if cfg.MarkSynced {
			s.log.Info("DRY RUN: sync time would be set", "rows", marks.count(), "tables", len(marks.rows), "deletes_up_to_id", marks.deletesUpTo)
		}
		if cfg.TwoWay {
			if err := s.planPull(ctx, dst, data); err != nil {
				return err
			}
		}
		return s.plan(ctx, srcTx, dst, data, order, changes.Reset)
	}

	// A new enum value cannot be used in the transaction that adds it.
	for _, q := range changes.EnumValues {
		s.log.Info("schema change", "sql", q)
		if _, err := dst.Exec(ctx, q); err != nil {
			return fmt.Errorf("%s: %w", q, err)
		}
	}

	if err := s.ensureMeta(ctx, dst); err != nil {
		return err
	}
	dstTx, locked, state, err := s.begin(ctx, dst, advisoryLockKey)
	if err != nil {
		return err
	}
	defer dstTx.Rollback(context.Background())
	if !locked {
		s.log.Warn("another sync is already running against this target; skipping this run")
		run.Skipped = true
		return nil
	}
	if cfg.Instant && whole {
		// Keep Watch out of the instant tables until this commits. It touches no
		// table before this point, so waiting for a running instant sync is safe.
		s.log.Info("schema changes: this run moves every table; instant sync pauses until it commits")
		if err := waitLock(ctx, dstTx, instantLockKey, cfg.LockTimeout); err != nil {
			return err
		}
	}

	if err := s.applyDDL(ctx, dstTx, changes.Before); err != nil {
		return err
	}
	if len(changes.Before) > 0 {
		// Make sure the target can now take every source column before copying.
		var now map[string]*schema.Table
		if err := schema.CatalogPath(ctx, dstTx, func() error {
			var err error
			now, err = schema.Load(ctx, dstTx, cfg.Schemas)
			return err
		}); err != nil {
			return fmt.Errorf("target schema: %w", err)
		}
		if problems, _ := schema.Compare(tables, now); len(problems) > 0 {
			return fmt.Errorf("target still differs after the schema changes:\n  - %s", strings.Join(problems, "\n  - "))
		}
	}

	var st runStats
	switch cfg.Mode {
	case "full":
		err = s.full(ctx, src, dstTx, data, order, &st)
	default:
		for name := range changes.Reset {
			delete(state, name)
		}
		err = s.incremental(ctx, src, dstTx, data, order, state, snapshot, &st)
	}
	if err != nil {
		return err
	}
	if err := s.applyDDL(ctx, dstTx, slices.Concat(st.ReAdd, changes.After)); err != nil {
		return err
	}

	// Tell Watch about instant tables this run created or reshaped, so that new
	// ones get the change trigger.
	var handOver []string
	if cfg.Instant {
		for name := range changes.Reset {
			if t := tables[name]; t != nil && !t.Txn {
				handOver = append(handOver, name)
			}
		}
	}

	syncedAt, err := s.finish(ctx, dstTx, data, marks, srcSeq, snapshot, started, cfg.Mode, &st)
	if err != nil {
		return err
	}
	if cfg.TwoWay && (!cfg.Instant || changes.Count() > 0) {
		// After the commit, so that tables this run created have the trigger
		// put on. With -instant, Watch keeps the triggers current otherwise.
		if err := s.ensureCapture(ctx, dst, twoWayOf(tables)); err != nil {
			s.log.Warn("could not update the change capture on the live DB; the next run retries", "err", err)
		}
	}
	for _, kind := range slow {
		s.slowDone[kind] = started
	}

	done := []any{
		"mode", cfg.Mode,
		"schema_changes", changes.Count(),
		"tables", st.Tables,
		"rows_read", st.Copied,
		"rows_written", st.Upserted,
		"rows_deleted", st.Deleted,
		"took", time.Since(started).Round(time.Millisecond).String()}
	if len(slow) > 0 {
		done = append(done, "with_kinds", strings.Join(slow, ","))
	}
	s.log.Info("sync complete", done...)
	run.SchemaChanges, run.Tables, run.WithKinds = changes.Count(), st.Tables, slow
	run.RowsRead, run.RowsWritten, run.RowsDeleted = st.Copied, st.Upserted, st.Deleted

	if err := srcTx.Rollback(ctx); err != nil { // end the read-only snapshot
		return err
	}
	if cfg.MarkSynced {
		// The data is on the target already. If this fails, the rows stay waiting
		// on the source and the next run copies them (and their sync time) again.
		if err := s.markSource(ctx, src, data, marks, syncedAt); err != nil {
			s.log.Warn("could not set the sync time on the source; the next run retries", "err", err)
		}
	}
	if len(handOver) > 0 {
		if err := announce(ctx, src, handOver); err != nil {
			s.log.Warn("could not hand the changed tables to the instant sync; they are synced on their next change", "err", err)
		}
	}
	return nil
}

// beginSnapshot opens one consistent, read-only snapshot of the source and
// returns its time.
func beginSnapshot(ctx context.Context, src *pgx.Conn) (pgx.Tx, time.Time, error) {
	tx, err := src.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("begin source snapshot: %w", err)
	}
	var at time.Time
	if err := tx.QueryRow(ctx, "SELECT now()").Scan(&at); err != nil {
		tx.Rollback(context.Background())
		return nil, time.Time{}, err
	}
	return tx, at, nil
}

// order sorts tables parents first and refuses FK cycles it cannot break.
func (s *Syncer) order(tables map[string]*schema.Table) ([]string, error) {
	order, cyclic := schema.FKOrder(tables, !s.cfg.DisableTriggers)
	if len(cyclic) > 0 && !s.cfg.DisableTriggers {
		return nil, fmt.Errorf("tables %s form a foreign-key cycle with no nullable column to break it; run with -disable-triggers", strings.Join(cyclic, ", "))
	}
	for _, name := range order {
		if t := tables[name]; len(t.Break) > 0 {
			if len(t.PK) == 0 {
				return nil, fmt.Errorf("table %s is in a foreign-key cycle but has no primary key; run with -disable-triggers", name)
			}
			s.log.Info("foreign-key cycle: these columns are filled in a second pass", "table", name, "columns", strings.Join(t.Break, ","))
		}
	}
	return order, nil
}

// begin opens the target transaction, takes the advisory lock key, sets the
// session options and reads the watermarks (loadState), in one round trip.
// locked is false when another sync holds the lock.
func (s *Syncer) begin(ctx context.Context, dst *pgx.Conn, key int64) (tx pgx.Tx, locked bool, state map[string]time.Time, err error) {
	if tx, err = dst.Begin(ctx); err != nil {
		return nil, false, nil, fmt.Errorf("begin target: %w", err)
	}
	batch := &pgx.Batch{}
	batch.Queue("SELECT pg_try_advisory_xact_lock($1)", key).QueryRow(func(row pgx.Row) error { return row.Scan(&locked) })
	batch.Queue(s.stateSQL(), s.cfg.Schemas).Query(func(rows pgx.Rows) error {
		var err error
		state, err = scanState(rows)
		return err
	})
	batch.Queue(fmt.Sprintf("SET LOCAL lock_timeout = %d", s.cfg.LockTimeout.Milliseconds()))
	batch.Queue("SET LOCAL statement_timeout = 0")
	if s.cfg.DisableTriggers {
		batch.Queue("SET LOCAL session_replication_role = replica")
	}
	if s.cfg.TwoWay {
		batch.Queue(fmt.Sprintf("SET LOCAL %s = on", applyingSetting)) // the capture trigger leaves these writes out
	}
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		tx.Rollback(context.Background())
		return nil, false, nil, fmt.Errorf("prepare target transaction: %w", err)
	}
	return tx, locked, state, nil
}

// waitLock waits for advisory lock key without the -lock-timeout limit; it is
// held until tx ends.
func waitLock(ctx context.Context, tx pgx.Tx, key int64, restore time.Duration) error {
	batch := &pgx.Batch{}
	batch.Queue("SET LOCAL lock_timeout = 0")
	batch.Queue("SELECT pg_advisory_xact_lock($1)", key)
	batch.Queue(fmt.Sprintf("SET LOCAL lock_timeout = %d", restore.Milliseconds()))
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("wait for the instant sync: %w", err)
	}
	return nil
}

// finish stamps the sync time, moves the sequences past the copied ids,
// records the run and commits. It returns the sync time.
func (s *Syncer) finish(ctx context.Context, tx pgx.Tx, tables map[string]*schema.Table, marks *syncMarks, srcSeq map[seqKey]int64,
	snapshot, started time.Time, mode string, st *runStats) (time.Time, error) {
	syncedAt := time.Now().Truncate(time.Microsecond)
	if err := s.markTarget(ctx, tx, tables, marks, snapshot, syncedAt); err != nil {
		return syncedAt, err
	}
	batch := &pgx.Batch{} // the sequences and the state in one round trip
	cols := s.queueSequences(batch, tables, srcSeq, true)
	s.queueState(batch, tables, mode, snapshot, started, st)
	br := tx.SendBatch(ctx, batch)
	if err := sequencesDone(br, cols); err != nil {
		br.Close()
		return syncedAt, err
	}
	if err := br.Close(); err != nil {
		return syncedAt, fmt.Errorf("save sync state: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return syncedAt, fmt.Errorf("commit target: %w", err)
	}
	return syncedAt, nil
}

// guardDifferent refuses to run when both URLs reach the same database.
func guardDifferent(ctx context.Context, src, dst *pgx.Conn) error {
	const q = `SELECT current_database() || '@' || COALESCE(host(inet_server_addr()), 'socket') || ':' || current_setting('port')`
	var a, b string
	if err := src.QueryRow(ctx, q).Scan(&a); err != nil {
		return err
	}
	if err := dst.QueryRow(ctx, q).Scan(&b); err != nil {
		return err
	}
	if a == b {
		return fmt.Errorf("source and target are the same database (%s)", a)
	}
	return nil
}

func (s *Syncer) applyDDL(ctx context.Context, tx pgx.Tx, stmts []string) error {
	for _, q := range stmts {
		s.log.Info("schema change", "sql", q)
		if _, err := tx.Exec(ctx, q); err != nil {
			return fmt.Errorf("schema change failed: %s: %w", q, err)
		}
	}
	return nil
}

// readTarget introspects the target in a short read-only transaction. The
// catalog queries go out in one batch: one round trip instead of one each.
// search_path is set as in schema.CatalogPath; the transaction is rolled back,
// so it needs no restoring.
func readTarget(ctx context.Context, dst *pgx.Conn, cfg *config.Config) (map[string]*schema.Table, *ddl.Info, error) {
	tx, err := dst.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback(context.Background())
	queries := schema.LoadQueries
	if cfg.DDL {
		queries = slices.Concat(queries, ddl.InfoQueries)
	}
	q, err := pg.Prefetch(ctx, tx, []string{"SET LOCAL search_path = pg_catalog"}, queries, cfg.Schemas)
	if err != nil {
		return nil, nil, fmt.Errorf("target schema: %w", err)
	}
	defer q.Close()
	all, err := schema.Load(ctx, q, cfg.Schemas)
	if err != nil {
		return nil, nil, fmt.Errorf("target schema: %w", err)
	}
	var info *ddl.Info
	if cfg.DDL {
		if info, err = ddl.LoadInfo(ctx, q, cfg.Schemas); err != nil {
			return nil, nil, err
		}
	}
	return all, info, q.Close()
}
