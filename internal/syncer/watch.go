package syncer

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/vksoftwares/vknex-dbsync/internal/pg"
	"github.com/vksoftwares/vknex-dbsync/internal/schema"
	"github.com/vksoftwares/vknex-dbsync/internal/status"
)

// With -instant, every synced table outside -txn-tables is mirrored within
// seconds of a change; with -table-master, only the tables of -instant-kinds
// (schema.Select). A statement trigger on the source (notifyTrigger, also on
// every partition of a partitioned table) sends the table's name on
// notifyChannel when a transaction that changed it commits; Watch listens
// there and runs a small incremental sync of just the tables named. The
// interval tables (Table.Txn) keep following -interval in RunOnce.
// -table-master carries the trigger too: a change there sorts the tables again.
const (
	notifyChannel  = "vknex_sync"
	notifyTrigger  = "zz_vknex_sync_notify"
	notifyFunction = "notify_change" // in the -meta-schema schema on the source

	settleDelay = 300 * time.Millisecond // gathers the notifications of one save into one run
	retryMin    = 5 * time.Second
	retryMax    = time.Minute
)

// Watch mirrors the instant tables until ctx ends, starting over after an error
// (e.g. the source restarted). On every start it first syncs all of them, to
// catch up on changes made while nothing was listening.
func (s *Syncer) Watch(ctx context.Context) {
	for {
		err := s.watch(ctx)
		if ctx.Err() != nil {
			return
		}
		s.log.Error("instant sync stopped; restarting", "err", err, "in", retryMax.String())
		s.notify.Failed("instant sync", err)
		s.Status.InstantDown(err, retryMax)
		select {
		case <-ctx.Done():
			return
		case <-time.After(retryMax):
		}
	}
}

// watcher holds the connections of the instant runs between changes.
type watcher struct {
	s        *Syncer
	src, dst *pgx.Conn       // reopened after an error
	tables   map[string]bool // every synced table
	instant  map[string]bool // the tables Watch mirrors
	master   string          // key of -table-master, "" without it
	paused   bool            // waiting for an interval run (errBusy)
}

func (s *Syncer) watch(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w := &watcher{s: s}
	if s.cfg.TableMaster != "" {
		w.master = schema.Key(s.cfg.TableMasterName())
	}
	defer w.close()

	listen, err := pg.Connect(ctx, s.cfg.SourceURL)
	if err != nil {
		return fmt.Errorf("connect source: %w", err)
	}
	// Listen before the triggers go in and the first snapshot is taken, so that
	// no change falls in between.
	if _, err := listen.Exec(ctx, "LISTEN "+pg.Ident(notifyChannel)); err != nil {
		listen.Close(context.Background())
		return fmt.Errorf("listen on source: %w", err)
	}
	notes := make(chan string, 1024) // keeps filling while a run is busy
	lost := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			n, err := listen.WaitForNotification(ctx)
			if err != nil {
				lost <- err
				return
			}
			select {
			case notes <- n.Payload:
			case <-ctx.Done():
				return
			}
		}
	}()
	defer func() {
		cancel()
		wg.Wait()
		listen.Close(context.Background())
	}()

	if err := w.refresh(ctx); err != nil {
		return err
	}
	for _, entry := range s.cfg.TxnTables {
		found := w.master != "" // then -txn-tables is ignored (see main)
		for key := range w.tables {
			_, tbl, _ := strings.Cut(key, ".")
			found = found || entry == key || entry == tbl
		}
		if !found {
			s.log.Warn("-txn-tables names a table that is not synced", "table", entry)
		}
	}
	started := []any{"instant_tables", len(w.instant), "txn_tables", len(w.tables) - len(w.instant)}
	if w.master != "" {
		started = append(started, "table_master", w.master, "instant_kinds", strings.Join(s.cfg.InstantKinds, ","))
	}
	s.log.Info("instant sync started", started...)
	s.Status.InstantStarted(len(w.instant), len(w.tables)-len(w.instant))

	pending := maps.Clone(w.instant) // catch up first
	note := func(name string) error {
		if !w.tables[name] || name == w.master {
			// New since the last look (an interval run hands over tables it
			// just created), or -table-master changed: read the table list
			// again. A table that just became instant catches up at once, as
			// the interval runs no longer move it.
			was := w.instant
			if err := w.refresh(ctx); err != nil {
				return err
			}
			var gained, lost []string
			for key := range w.instant {
				if !was[key] {
					gained = append(gained, key)
					pending[key] = true
				}
			}
			for key := range was {
				if w.tables[key] && !w.instant[key] {
					lost = append(lost, key)
				}
			}
			if len(gained)+len(lost) > 0 {
				slices.Sort(gained)
				slices.Sort(lost)
				s.log.Info("instant tables changed", "now_instant", listOf(gained), "now_interval", listOf(lost))
			}
		}
		if w.instant[name] {
			pending[name] = true
		}
		return nil
	}
	now := make(chan time.Time)
	close(now) // always ready

	failed := map[string]bool{} // retried when retry fires
	var solo []string           // tried one at a time, in between new changes
	var settle, retry <-chan time.Time
	var retryAt time.Time // when retry fires
	backoff := retryMin
	for {
		// Changes that came in during the last run go first.
		for drained := false; !drained; {
			select {
			case name := <-notes:
				if err := note(name); err != nil {
					return err
				}
			default:
				drained = true
			}
		}
		if len(pending) > 0 && settle == nil {
			settle = time.After(settleDelay)
		}
		var next <-chan time.Time
		if settle == nil && len(solo) > 0 {
			next = now
		}

		var names []string
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-lost:
			return fmt.Errorf("listen on source: %w", err)
		case name := <-notes:
			if err := note(name); err != nil {
				return err
			}
			continue
		case <-retry:
			retry, retryAt = nil, time.Time{}
			maps.Copy(pending, failed)
			clear(failed)
			continue
		case <-settle:
			settle = nil
			names = slices.Sorted(maps.Keys(pending))
			clear(pending)
		case <-next:
			names, solo = solo[:1], solo[1:]
		}

		s.Status.InstantSyncing(names)
		err := w.sync(ctx, names)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		switch {
		case err == nil:
		case len(names) > 1 && perTable(err):
			// One table's data is the problem: try each on its own so that it
			// does not hold up the others.
			for _, name := range names {
				if !slices.Contains(solo, name) {
					solo = append(solo, name)
				}
			}
		default:
			// Not one table's fault: the queued tables would fail the same way,
			// so they are retried together.
			for _, name := range slices.Concat(names, solo) {
				failed[name] = true
			}
			solo = nil
		}
		switch {
		case len(failed) == 0:
			backoff = retryMin
		case retry == nil:
			retry, retryAt = time.After(backoff), time.Now().Add(backoff)
			if !errors.Is(err, errBusy) { // a schema-change run ends soon: keep checking
				backoff = min(2*backoff, retryMax)
			}
		}
		s.Status.InstantSettled(names, err, slices.Concat(slices.Sorted(maps.Keys(failed)), solo), retryAt, w.paused)
	}
}

// errBusy: another sync holds the instant tables on the target, normally an
// interval run applying schema changes.
var errBusy = errors.New("another sync is writing the instant tables on the target")

// connectError marks a failure to open the connections: no table is to blame.
type connectError struct{ error }

func (e connectError) Unwrap() error { return e.error }

// perTable reports whether err comes from one table's data or structure, as
// opposed to the connection, a lock wait or a busy target.
func perTable(err error) bool {
	var ce connectError
	var pgErr *pgconn.PgError
	if errors.As(err, &ce) || !errors.As(err, &pgErr) {
		return false
	}
	switch pgErr.Code[:2] {
	case "08", "40", "53", "57": // connection, deadlock / serialization, resources, operator intervention
		return false
	}
	return pgErr.Code != "55P03" // a lock wait timed out
}

// sync runs names once and logs the outcome.
func (w *watcher) sync(ctx context.Context, names []string) error {
	err := w.open(ctx)
	if err != nil {
		err = connectError{err}
	} else {
		err = w.run(ctx, names)
	}
	switch {
	case err == nil:
		if w.paused {
			w.s.log.Info("instant sync resumed")
			w.paused = false
		}
		w.s.notify.Recovered("instant sync")
	case errors.Is(err, errBusy):
		if !w.paused {
			w.s.log.Info("instant sync paused: an interval run is applying schema changes and moving every table", "tables", listOf(names))
			w.paused = true
		}
	default:
		w.report(ctx, names, err)
	}
	return err
}

func (w *watcher) report(ctx context.Context, names []string, err error) {
	if ctx.Err() != nil {
		return // stopping: the run was cut short on purpose
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && (pgErr.Code == "42703" || pgErr.Code == "42P01") { // undefined column / table
		w.s.log.Warn("instant sync waits for the target to get the source's table structure (the next interval run with -ddl applies it)",
			"tables", listOf(names), "err", err)
		return
	}
	w.s.log.Error("instant sync failed; will retry", "tables", listOf(names), "err", err)
	w.s.notify.Failed("instant sync", err)
}

// listOf names a few tables for a log line; a catch-up run has them all.
func listOf(names []string) string {
	if len(names) <= 5 {
		return strings.Join(names, ",")
	}
	return fmt.Sprintf("%s and %d more", strings.Join(names[:5], ","), len(names)-5)
}

// run mirrors the named tables into the target in one transaction, like an
// incremental RunOnce limited to them, without schema changes.
func (w *watcher) run(ctx context.Context, names []string) error {
	err := w.runTables(ctx, names)
	if err != nil {
		w.close() // start the next run on fresh connections
	}
	return err
}

func (w *watcher) runTables(ctx context.Context, names []string) error {
	s, cfg := w.s, w.s.cfg
	started := time.Now()
	if err := w.open(ctx); err != nil {
		return err
	}

	srcTx, snapshot, err := beginSnapshot(ctx, w.src)
	if err != nil {
		return err
	}
	defer srcTx.Rollback(context.Background())

	var srcAll map[string]*schema.Table
	if err := schema.CatalogPath(ctx, srcTx, func() error {
		var err error
		srcAll, err = schema.Load(ctx, srcTx, cfg.Schemas)
		return err
	}); err != nil {
		return fmt.Errorf("source schema: %w", err)
	}
	kinds, err := schema.LoadKinds(ctx, srcTx, cfg)
	if err != nil {
		return err
	}
	want := toSet(names)
	tables := schema.Subset(schema.Select(srcAll, cfg, kinds), func(t *schema.Table) bool {
		return want[schema.Key(t.Schema, t.Name)] && !t.Txn // still instant at this snapshot
	})
	if len(tables) == 0 {
		return nil // dropped on the source since the notification
	}
	order, err := s.order(tables)
	if err != nil {
		return err
	}
	srcSeq, err := readSourceSequences(ctx, srcTx, tables)
	if err != nil {
		return err
	}
	marks := &syncMarks{}
	if cfg.MarkSynced {
		if marks, err = readPending(ctx, srcTx, tables); err != nil {
			return err
		}
	}

	dstTx, locked, err := s.begin(ctx, w.dst, instantLockKey)
	if err != nil {
		return err
	}
	defer dstTx.Rollback(context.Background())
	if !locked {
		return errBusy
	}

	state, err := s.loadState(ctx, dstTx)
	if err != nil {
		return err
	}
	var st runStats
	if err := s.incremental(ctx, w.src, dstTx, tables, order, state, &st); err != nil {
		return err
	}
	if err := s.applyDDL(ctx, dstTx, st.ReAdd); err != nil {
		return err
	}
	syncedAt, err := s.finish(ctx, dstTx, tables, marks, srcSeq, snapshot, started, "instant", &st)
	if err != nil {
		return err
	}
	s.log.Info("instant sync complete",
		"tables", listOf(order),
		"rows_written", st.Upserted,
		"rows_deleted", st.Deleted,
		"took", time.Since(started).Round(time.Millisecond).String())
	s.Status.InstantSynced(status.InstantRun{Tables: order, At: time.Now().UnixMilli(), TookMs: time.Since(started).Milliseconds(), RowsWritten: st.Upserted, RowsDeleted: st.Deleted})

	if err := srcTx.Rollback(ctx); err != nil {
		return err
	}
	if cfg.MarkSynced {
		if err := s.markSource(ctx, w.src, tables, marks, syncedAt); err != nil {
			s.log.Warn("could not set the sync time on the source; the next run retries", "err", err)
		}
	}
	return nil
}

// refresh reads which tables are synced and which of them are instant, and
// makes sure that exactly the instant tables (and -table-master) carry the
// notify trigger on the source.
func (w *watcher) refresh(ctx context.Context) error {
	cfg := w.s.cfg
	if err := w.openSource(ctx); err != nil {
		return err
	}
	tx, err := w.src.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = %d", cfg.LockTimeout.Milliseconds())); err != nil {
		return err
	}

	var all map[string]*schema.Table
	if err := schema.CatalogPath(ctx, tx, func() error {
		var err error
		all, err = schema.Load(ctx, tx, cfg.Schemas)
		return err
	}); err != nil {
		return fmt.Errorf("source schema: %w", err)
	}
	kinds, err := schema.LoadKinds(ctx, tx, cfg)
	if err != nil {
		return err
	}
	selected := schema.Select(all, cfg, kinds)
	w.tables, w.instant = map[string]bool{}, map[string]bool{}
	// The relations that carry the trigger, each with the table its
	// notification names. A statement on a partition fires only the
	// partition's own trigger, so a partition names its partitioned table
	// ("" = the relation itself).
	notified := map[string]string{}
	if w.master != "" {
		notified[w.master] = ""
	}
	var crossing []string // instant table -> interval table it references
	for _, name := range slices.Sorted(maps.Keys(selected)) {
		t := selected[name]
		w.tables[name] = true
		if t.Txn {
			continue
		}
		w.instant[name] = true
		notified[name] = ""
		for _, p := range t.Partitions {
			notified[p.Key] = name
		}
		for _, fk := range t.FKs {
			if ref := name + " -> " + fk.Parent; selected[fk.Parent].Txn && !slices.Contains(crossing, ref) {
				crossing = append(crossing, ref)
			}
		}
	}
	if len(crossing) > 0 {
		w.s.log.Warn("instant tables reference interval tables: a new row can reach the live DB before the row it points at, "+
			"until the next interval run moves that one (without -disable-triggers its instant sync fails and retries until then)",
			"references", strings.Join(crossing, ","))
	}

	fn := pg.QName(cfg.MetaSchema, notifyFunction)
	for _, q := range []string{
		"CREATE SCHEMA IF NOT EXISTS " + pg.Ident(cfg.MetaSchema),
		fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  PERFORM pg_notify(%s, COALESCE(TG_ARGV[0], TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME));
  RETURN NULL;
END $$`, fn, pg.Literal(notifyChannel)),
	} {
		if _, err := tx.Exec(ctx, q); err != nil {
			return fmt.Errorf("install the change notification on the source: %w", err)
		}
	}

	rows, err := tx.Query(ctx, `SELECT n.nspname || '.' || c.relname FROM pg_trigger t
		JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE t.tgname = $1`, notifyTrigger)
	if err != nil {
		return err
	}
	has, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	var added, removed int
	for _, name := range has {
		if _, ok := notified[name]; ok {
			continue
		}
		ns, tbl, _ := strings.Cut(name, ".")
		if _, err := tx.Exec(ctx, fmt.Sprintf("DROP TRIGGER %s ON %s", pg.Ident(notifyTrigger), pg.QName(ns, tbl))); err != nil {
			return fmt.Errorf("table %s: remove the change notification: %w", name, err)
		}
		removed++
	}
	for _, name := range slices.Sorted(maps.Keys(notified)) {
		if slices.Contains(has, name) {
			continue
		}
		arg := ""
		if root := notified[name]; root != "" {
			arg = pg.Literal(root)
		}
		ns, tbl, _ := strings.Cut(name, ".") // -table-master need not be a synced table
		if _, err := tx.Exec(ctx, fmt.Sprintf("CREATE TRIGGER %s AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON %s FOR EACH STATEMENT EXECUTE FUNCTION %s(%s)",
			pg.Ident(notifyTrigger), pg.QName(ns, tbl), fn, arg)); err != nil {
			return fmt.Errorf("table %s: install the change notification: %w", name, err)
		}
		added++
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if added+removed > 0 {
		w.s.log.Info("change notification triggers updated on the source", "added", added, "removed", removed)
	}
	return nil
}

// announce tells Watch, in this process or another, that tables need a sync.
func announce(ctx context.Context, src *pgx.Conn, names []string) error {
	_, err := src.Exec(ctx, "SELECT pg_notify($1, n) FROM unnest($2::text[]) AS n", notifyChannel, names)
	return err
}

func (w *watcher) open(ctx context.Context) error {
	if err := w.openSource(ctx); err != nil {
		return err
	}
	if w.dst != nil && !w.dst.IsClosed() {
		return nil
	}
	dst, err := pg.Connect(ctx, w.s.cfg.TargetURL)
	if err != nil {
		return fmt.Errorf("connect target: %w", err)
	}
	if err := guardDifferent(ctx, w.src, dst); err != nil {
		dst.Close(context.Background())
		return err
	}
	if err := w.s.ensureMeta(ctx, dst); err != nil {
		dst.Close(context.Background())
		return err
	}
	w.dst = dst
	return nil
}

func (w *watcher) openSource(ctx context.Context) error {
	if w.src != nil && !w.src.IsClosed() {
		return nil
	}
	src, err := pg.Connect(ctx, w.s.cfg.SourceURL)
	if err != nil {
		return fmt.Errorf("connect source: %w", err)
	}
	w.src = src
	return nil
}

func (w *watcher) close() {
	for _, c := range []*pgx.Conn{w.src, w.dst} {
		if c != nil {
			c.Close(context.Background())
		}
	}
	w.src, w.dst = nil, nil
}
