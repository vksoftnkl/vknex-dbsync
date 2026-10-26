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

	"github.com/vksoftwares/vknex-dbsync/internal/pg"
	"github.com/vksoftwares/vknex-dbsync/internal/schema"
)

// With -two-way, the tables of a -two-way-kinds kind (schema.Table.TwoWay) may
// be written on the live database too, and those changes come back to the
// source:
//
//   - On the target, a row trigger (captureTrigger) on every two-way table logs
//     the key of each row inserted, updated or deleted there in live_changes
//     and sends the table's name on notifyChannel. The sync's own writes are
//     not logged: its target transactions set applyingSetting (see begin).
//   - pull copies the logged keys to the source, apart from the pushes (see
//     watchLive), within moments of the change. For every key the source row
//     becomes what the target has now, written or deleted, with its sync column
//     set to the pull time: the row is on both sides, so it is not a source
//     change waiting for the push. A key the source changed too since the last
//     sync (sync column NULL, or its delete waiting in sync_deleted_rows) is a
//     conflict: -two-way-wins local keeps the source's version, which the push
//     then writes to the target; live takes the target's.
//   - A pulled change stays in the log, with the time it was pulled, until a
//     push whose source snapshot has it clears it (saveState). Until then the
//     pushes leave its target row alone and its source row waiting
//     (liveWaiting): a push that read the source before the pull would write
//     the old row back.
const (
	captureTrigger  = "zz_vknex_sync_capture"
	captureFunction = "capture_change"      // in the -meta-schema schema on the target
	liveChangesName = "live_changes"        // the log, in the -meta-schema schema on the target
	applyingSetting = "vknex_sync.applying" // "on" in the sync's own target transactions

	// The notifications of one commit arrive together; this only gathers a
	// save made of several commits. Every round trip delays the change more.
	pullSettle = 50 * time.Millisecond
)

func (s *Syncer) liveChanges() string { return pg.QName(s.cfg.MetaSchema, liveChangesName) }

// waitingSQL is a condition on c, a row of live_changes: the source does not
// have the change in snapshot, because it was pulled later or not yet. A zero
// snapshot asks for the changes not pulled yet.
func waitingSQL(snapshot time.Time) string {
	if snapshot.IsZero() {
		return "c.pulled_at IS NULL"
	}
	return fmt.Sprintf("(c.pulled_at IS NULL OR c.pulled_at >= %s::timestamptz)",
		pg.Literal(snapshot.UTC().Format("2006-01-02 15:04:05.999999")+"+00"))
}

// liveWaiting is a condition on alias, a row holding t's primary key columns,
// for a push that read the source at snapshot: its key has a live change that
// snapshot does not have. "" for one-way tables.
func (s *Syncer) liveWaiting(t *schema.Table, alias string, snapshot time.Time) string {
	if !t.TwoWay {
		return ""
	}
	pairs := make([]string, len(t.PK))
	for i, c := range t.PK {
		pairs[i] = fmt.Sprintf("%s, %s.%s", pg.Literal(c), alias, pg.Ident(c))
	}
	return fmt.Sprintf("EXISTS (SELECT 1 FROM %s c WHERE c.schema_name = %s AND c.table_name = %s AND c.pk = jsonb_build_object(%s) AND %s)",
		s.liveChanges(), pg.Literal(t.Schema), pg.Literal(t.Name), strings.Join(pairs, ", "), waitingSQL(snapshot))
}

// liveKeysSQL selects the keys logged for t whose change snapshot does not
// have (see waitingSQL), typed as its primary key columns.
func (s *Syncer) liveKeysSQL(t *schema.Table, snapshot time.Time) string {
	return fmt.Sprintf("SELECT DISTINCT %s FROM %s c CROSS JOIN LATERAL jsonb_populate_record(NULL::%s, c.pk) k WHERE c.schema_name = %s AND c.table_name = %s AND %s",
		pg.IdentList(t.PK, "k."), s.liveChanges(), t.QName(), pg.Literal(t.Schema), pg.Literal(t.Name), waitingSQL(snapshot))
}

// captureBody is the capture function. Its arguments are the table's schema,
// name and primary key columns; an update that moves a row to another key logs
// both keys.
func (s *Syncer) captureBody() string {
	return fmt.Sprintf(`
DECLARE
  r jsonb;
  old_key jsonb;
  new_key jsonb;
BEGIN
  IF current_setting(%[2]s, true) = 'on' THEN
    RETURN NULL; -- written by the sync itself
  END IF;
  IF TG_OP <> 'INSERT' THEN
    r := to_jsonb(OLD);
    old_key := '{}';
    FOR i IN 2 .. TG_NARGS - 1 LOOP
      old_key := old_key || jsonb_build_object(TG_ARGV[i], r -> TG_ARGV[i]);
    END LOOP;
    INSERT INTO %[1]s (schema_name, table_name, pk) VALUES (TG_ARGV[0], TG_ARGV[1], old_key);
  END IF;
  IF TG_OP <> 'DELETE' THEN
    r := to_jsonb(NEW);
    new_key := '{}';
    FOR i IN 2 .. TG_NARGS - 1 LOOP
      new_key := new_key || jsonb_build_object(TG_ARGV[i], r -> TG_ARGV[i]);
    END LOOP;
    IF new_key IS DISTINCT FROM old_key THEN
      INSERT INTO %[1]s (schema_name, table_name, pk) VALUES (TG_ARGV[0], TG_ARGV[1], new_key);
    END IF;
  END IF;
  PERFORM pg_notify(%[3]s, TG_ARGV[0] || '.' || TG_ARGV[1]);
  RETURN NULL;
END `, s.liveChanges(), pg.Literal(applyingSetting), pg.Literal(notifyChannel))
}

// twoWayOf picks the two-way tables, without touching their FKs (schema.Subset would).
func twoWayOf(tables map[string]*schema.Table) map[string]*schema.Table {
	out := map[string]*schema.Table{}
	for key, t := range tables {
		if t.TwoWay {
			out[key] = t
		}
	}
	return out
}

// ensureCapture makes the capture function current on the target and puts the
// capture trigger on exactly the two-way tables there, in a transaction of its
// own. A table the target does not have yet gets it once an interval run has
// created it. Changes logged for tables that are no longer two-way are
// dropped: the source's rows win there.
func (s *Syncer) ensureCapture(ctx context.Context, dst *pgx.Conn, tables map[string]*schema.Table) error {
	cfg := s.cfg
	tx, err := dst.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	batch := &pgx.Batch{}
	batch.Queue(fmt.Sprintf("SET LOCAL lock_timeout = %d", cfg.LockTimeout.Milliseconds()))
	// An interval run and Watch can both get here at the start: one at a time.
	batch.Queue("SELECT pg_advisory_xact_lock($1)", metaLockKey)
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return err
	}

	fn := pg.QName(cfg.MetaSchema, captureFunction)
	body := s.captureBody()
	var have string
	if err := tx.QueryRow(ctx, "SELECT COALESCE((SELECT prosrc FROM pg_proc WHERE oid = to_regprocedure($1)), '')", fn+"()").Scan(&have); err != nil {
		return err
	}
	if have != body {
		// SECURITY DEFINER: the live application's database user needs no rights on the log.
		if _, err := tx.Exec(ctx, fmt.Sprintf("CREATE OR REPLACE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$%s$$",
			fn, body)); err != nil {
			return fmt.Errorf("install the change capture on the target: %w", err)
		}
	}

	quoted := make([]string, 0, len(tables))
	for _, t := range tables {
		quoted = append(quoted, t.QName())
	}
	rows, err := tx.Query(ctx, "SELECT k FROM unnest($1::text[]) AS k WHERE to_regclass(k) IS NOT NULL", quoted)
	if err != nil {
		return err
	}
	list, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	exists := toSet(list)

	// Partitions carry clones of a partitioned table's trigger (tgparentid <> 0).
	rows, err = tx.Query(ctx, `SELECT n.nspname || '.' || c.relname, t.tgargs FROM pg_trigger t
		JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE t.tgname = $1 AND t.tgparentid = 0`, captureTrigger)
	if err != nil {
		return err
	}
	has := map[string][]string{} // table -> trigger arguments
	for rows.Next() {
		var name string
		var args []byte // each argument ends in a zero byte
		if err := rows.Scan(&name, &args); err != nil {
			rows.Close()
			return err
		}
		has[name] = strings.Split(strings.TrimSuffix(string(args), "\x00"), "\x00")
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	var added, removed int
	var changed []string // the table of each statement in ddl
	ddl := &pgx.Batch{}  // one round trip for every table
	for _, name := range slices.Sorted(maps.Keys(has)) {
		if tables[name] != nil {
			continue
		}
		ns, tbl, _ := strings.Cut(name, ".")
		ddl.Queue(fmt.Sprintf("DROP TRIGGER %s ON %s", pg.Ident(captureTrigger), pg.QName(ns, tbl)))
		changed = append(changed, name)
		removed++
	}
	for _, name := range slices.Sorted(maps.Keys(tables)) {
		t := tables[name]
		args := slices.Concat([]string{t.Schema, t.Name}, t.PK)
		if !exists[t.QName()] || slices.Equal(has[name], args) {
			continue
		}
		lits := make([]string, len(args))
		for i, a := range args {
			lits[i] = pg.Literal(a)
		}
		ddl.Queue(fmt.Sprintf("CREATE OR REPLACE TRIGGER %s AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION %s(%s)",
			pg.Ident(captureTrigger), t.QName(), fn, strings.Join(lits, ", ")))
		changed = append(changed, name)
		added++
	}
	if len(changed) > 0 {
		br := tx.SendBatch(ctx, ddl)
		for _, name := range changed {
			if _, err := br.Exec(); err != nil {
				br.Close()
				return fmt.Errorf("table %s: update the change capture: %w", name, err)
			}
		}
		if err := br.Close(); err != nil {
			return err
		}
	}
	keys := make([]string, 0, len(tables)) // not nil: = ANY(NULL) would keep every row
	for name := range tables {
		keys = append(keys, name)
	}
	tag, err := tx.Exec(ctx, "DELETE FROM "+s.liveChanges()+" WHERE NOT (schema_name || '.' || table_name = ANY($1::text[]))", keys)
	if err != nil {
		return fmt.Errorf("clear %s: %w", s.liveChanges(), err)
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if added+removed > 0 {
		s.log.Info("change capture triggers updated on the target", "added", added, "removed", removed)
	}
	if n := tag.RowsAffected(); n > 0 {
		s.log.Warn("live changes of tables that are no longer synced both ways were dropped; the local rows win", "rows", n)
	}
	return nil
}

// fkRef is a foreign key of any source table that points at a pulled table.
type fkRef struct {
	child      string   // quoted schema.name
	self       bool     // from the table to itself
	cols, refs []string // its columns, and the columns of the pulled table they point at
}

const refsSQL = `
SELECT pn.nspname || '.' || p.relname, format('%I.%I', n.nspname, c.relname), con.conrelid = con.confrelid,
       ARRAY(SELECT a.attname::text FROM unnest(con.conkey) WITH ORDINALITY AS k(num, ord)
             JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.num ORDER BY k.ord),
       ARRAY(SELECT a.attname::text FROM unnest(con.confkey) WITH ORDINALITY AS k(num, ord)
             JOIN pg_attribute a ON a.attrelid = con.confrelid AND a.attnum = k.num ORDER BY k.ord)
FROM pg_constraint con
JOIN pg_class c      ON c.oid = con.conrelid
JOIN pg_namespace n  ON n.oid = c.relnamespace
JOIN pg_class p      ON p.oid = con.confrelid
JOIN pg_namespace pn ON pn.oid = p.relnamespace
WHERE con.contype = 'f' AND con.conparentid = 0 AND pn.nspname || '.' || p.relname = ANY($1::text[])`

func loadRefs(ctx context.Context, q pg.Querier, names []string) (map[string][]fkRef, error) {
	rows, err := q.Query(ctx, refsSQL, names)
	if err != nil {
		return nil, fmt.Errorf("read foreign keys: %w", err)
	}
	defer rows.Close()
	out := map[string][]fkRef{}
	for rows.Next() {
		var parent string
		var r fkRef
		if err := rows.Scan(&parent, &r.child, &r.self, &r.cols, &r.refs); err != nil {
			return nil, err
		}
		out[parent] = append(out[parent], r)
	}
	return out, rows.Err()
}

// goneSQL is a condition on row alias of t: the target deleted it (its key is
// in keys, the target has no row for it in stage).
func goneSQL(t *schema.Table, alias, keys, stage string) string {
	return fmt.Sprintf("(EXISTS (SELECT 1 FROM %s k WHERE %s) AND NOT EXISTS (SELECT 1 FROM %s s WHERE %s))",
		keys, matchCols(t.PK, "k", alias), stage, matchCols(t.PK, "s", alias))
}

// referencedSQL is a condition on row d of t: a source row points at it,
// other than a row of t this pull deletes too. "" when nothing can.
func referencedSQL(t *schema.Table, refs []fkRef, keys, stage string) string {
	var conds []string
	for _, r := range refs {
		match := make([]string, len(r.cols))
		for i := range r.cols {
			match[i] = fmt.Sprintf("c.%s = d.%s", pg.Ident(r.cols[i]), pg.Ident(r.refs[i]))
		}
		q := fmt.Sprintf("EXISTS (SELECT 1 FROM %s c WHERE %s", r.child, strings.Join(match, " AND "))
		if r.self {
			q += " AND NOT " + goneSQL(t, "c", keys, stage)
		}
		conds = append(conds, q+")")
	}
	if len(conds) == 0 {
		return ""
	}
	return "(" + strings.Join(conds, " OR ") + ")"
}

// pull copies the changes logged on the target for the two-way tables, and not
// pulled yet, to the source in one source transaction. While another pull
// holds pullLockKey it returns errBusy. Rows deleted on the target that a
// source row still points at are kept on the source, waiting, so that the push
// writes them back.
func (s *Syncer) pull(ctx context.Context, src, dst *pgx.Conn) error {
	cfg := s.cfg
	started := time.Now()

	// One snapshot of the target for the log and the rows; a change logged
	// after it waits for the next pull. Each round trip to the live DB delays
	// the change, so the transaction begins with the first query and commits
	// with the last statement.
	var locked bool
	var snapshot time.Time
	var names []string
	batch := &pgx.Batch{}
	batch.Queue("BEGIN ISOLATION LEVEL REPEATABLE READ")
	batch.Queue(fmt.Sprintf("SELECT pg_try_advisory_xact_lock($1), now(), ARRAY(SELECT DISTINCT c.schema_name || '.' || c.table_name FROM %s c WHERE %s)",
		s.liveChanges(), waitingSQL(time.Time{})), pullLockKey).QueryRow(func(row pgx.Row) error { return row.Scan(&locked, &snapshot, &names) })
	open := true
	defer func() {
		if open {
			dst.Exec(context.Background(), "ROLLBACK")
		}
	}()
	if err := dst.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("read live changes: %w", err)
	}
	if !locked {
		return errBusy
	}
	if len(names) == 0 {
		return nil
	}
	waiting := toSet(names)

	srcTx, err := src.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin source: %w", err)
	}
	defer srcTx.Rollback(context.Background())
	batch = &pgx.Batch{}
	batch.Queue(fmt.Sprintf("SET LOCAL lock_timeout = %d", cfg.LockTimeout.Milliseconds()))
	// The stages have no statistics, so the planner takes them for thousands of
	// rows and would JIT-compile the key lookups below: 200 ms each pull.
	batch.Queue("SET LOCAL jit = off")
	if err := srcTx.SendBatch(ctx, batch).Close(); err != nil {
		return err
	}
	var all map[string]*schema.Table
	if err := schema.CatalogPath(ctx, srcTx, func() error {
		var err error
		all, err = schema.Load(ctx, srcTx, cfg.Schemas)
		return err
	}); err != nil {
		return fmt.Errorf("source schema: %w", err)
	}
	master, err := schema.LoadMaster(ctx, srcTx, cfg)
	if err != nil {
		return err
	}
	selected, _ := schema.Select(all, cfg, master) // the runs log the warnings
	tables := schema.Subset(selected, func(t *schema.Table) bool { return t.TwoWay && waiting[schema.Key(t.Schema, t.Name)] })
	if len(tables) == 0 {
		return nil
	}
	order, err := s.order(tables)
	if err != nil {
		return err
	}
	refs, err := loadRefs(ctx, srcTx, order)
	if err != nil {
		return err
	}
	var deleteLog bool
	if err := srcTx.QueryRow(ctx, "SELECT to_regclass('public.sync_deleted_rows') IS NOT NULL").Scan(&deleteLog); err != nil {
		return err
	}

	at := time.Now().Truncate(time.Microsecond) // the sync time of every row pulled
	local := cfg.TwoWayWins == "local"
	// guard keeps, with local winning, a source row that changed since the
	// conflict check below (the source does not wait for the pull).
	guard := func(t *schema.Table) string {
		if !local {
			return ""
		}
		return " AND d." + pg.Ident(t.SyncCol.Name) + " IS NOT NULL"
	}
	keysOf, stageOf := map[string]string{}, map[string]string{}
	staged := map[string]int64{}
	var read, written, deleted, conflicts int64

	// 1. stage the logged keys (keysOf) and the target's rows for them
	//    (stageOf), from one copy per table: every key with its row, or with
	//    _found false when the target deleted it
	for i, name := range order {
		t := tables[name]
		both, keys, stage := fmt.Sprintf("_pull_%d", i), fmt.Sprintf("_pull_k_%d", i), fmt.Sprintf("_pull_s_%d", i)
		keysOf[name], stageOf[name] = keys, stage
		cols := t.ColNames()
		from := make([]string, len(cols))
		for j, c := range cols {
			from[j] = "t." + pg.Ident(c)
			if slices.Contains(t.PK, c) {
				from[j] = "k." + pg.Ident(c)
			}
		}
		if _, err := srcTx.Exec(ctx, fmt.Sprintf("CREATE TEMP TABLE %s ON COMMIT DROP AS SELECT %s, true AS _found FROM %s WITH NO DATA",
			both, pg.IdentList(cols, ""), t.QName())); err != nil {
			return fmt.Errorf("table %s: create stage: %w", name, err)
		}
		q := fmt.Sprintf("SELECT %s, t.%s IS NOT NULL FROM (%s) k LEFT JOIN %s t ON %s",
			strings.Join(from, ", "), pg.Ident(t.PK[0]), s.liveKeysSQL(t, time.Time{}), t.QName(), matchCols(t.PK, "t", "k"))
		if _, err := pg.CopyStream(ctx, dst.PgConn(), srcTx.Conn().PgConn(), q, both, append(cols, "_found"), t.TextCopy); err != nil {
			return fmt.Errorf("table %s: read live changes: %w", name, err)
		}
		create := fmt.Sprintf("CREATE TEMP TABLE %s ON COMMIT DROP AS SELECT %s FROM %s WITH NO DATA", stage, pg.IdentList(cols, ""), t.QName())
		if len(t.All) > len(t.Columns) {
			// Keep generated columns, computed on insert, so unique keys that use them can be matched.
			create = fmt.Sprintf("CREATE TEMP TABLE %s (LIKE %s INCLUDING GENERATED) ON COMMIT DROP", stage, t.QName())
		}
		if _, err := srcTx.Exec(ctx, fmt.Sprintf("CREATE TEMP TABLE %s ON COMMIT DROP AS SELECT %s FROM %s", keys, pg.IdentList(t.PK, ""), both)); err != nil {
			return fmt.Errorf("table %s: create stage: %w", name, err)
		}
		if _, err := srcTx.Exec(ctx, create); err != nil {
			return fmt.Errorf("table %s: create stage: %w", name, err)
		}
		tag, err := srcTx.Exec(ctx, fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s WHERE _found",
			stage, pg.IdentList(cols, ""), pg.IdentList(cols, ""), both))
		if err != nil {
			return fmt.Errorf("table %s: stage: %w", name, err)
		}
		staged[name] = tag.RowsAffected()
		read += tag.RowsAffected()
	}

	// 2. conflicts: keys the source changed too since the last sync
	for _, name := range order {
		t := tables[name]
		changed := fmt.Sprintf("EXISTS (SELECT 1 FROM %s l WHERE %s AND l.%s IS NULL)", t.QName(), matchCols(t.PK, "l", "k"), pg.Ident(t.SyncCol.Name))
		if deleteLog {
			changed += fmt.Sprintf(` OR (NOT EXISTS (SELECT 1 FROM %s l WHERE %s) AND EXISTS (SELECT 1 FROM public.sync_deleted_rows x
				CROSS JOIN LATERAL jsonb_populate_record(NULL::%s, x.sdr_pk) l
				WHERE x.sdr_sync_date IS NULL AND x.sdr_schema = %s AND x.sdr_table = %s AND %s))`,
				t.QName(), matchCols(t.PK, "l", "k"), t.QName(), pg.Literal(t.Schema), pg.Literal(t.Name), matchCols(t.PK, "l", "k"))
		}
		q := fmt.Sprintf("SELECT ROW(%s)::text FROM %s k WHERE %s", pg.IdentList(t.PK, "k."), keysOf[name], changed)
		if local {
			q = fmt.Sprintf("DELETE FROM %s k WHERE %s RETURNING ROW(%s)::text", keysOf[name], changed, pg.IdentList(t.PK, "k."))
		}
		keys, err := collectKeys(ctx, srcTx, q)
		if err != nil {
			return fmt.Errorf("table %s: find conflicts: %w", name, err)
		}
		if len(keys) == 0 {
			continue
		}
		conflicts += int64(len(keys))
		s.log.Warn("two-way conflict: changed on both databases since the last sync", "table", name, "rows", len(keys), "kept", cfg.TwoWayWins, "keys", listOf(keys))
		if local {
			if _, err := srcTx.Exec(ctx, fmt.Sprintf("DELETE FROM %s s WHERE NOT EXISTS (SELECT 1 FROM %s k WHERE %s)",
				stageOf[name], keysOf[name], matchCols(t.PK, "k", "s"))); err != nil {
				return fmt.Errorf("table %s: set conflicts aside: %w", name, err)
			}
		}
	}

	// 3. unique keys, children first: a source row the incoming rows do not
	//    replace may hold a value one of them takes
	for i := len(order) - 1; i >= 0; i-- {
		name := order[i]
		t := tables[name]
		if staged[name] == 0 {
			continue
		}
		keys, stage := keysOf[name], stageOf[name]
		ref := referencedSQL(t, refs[name], keys, stage)
		for _, u := range t.Uniques {
			match := uniqueMatchSQL(t, u, stage)
			// Deleted on the target: delete it first (e.g. a code deleted and re-created there).
			tag, err := srcTx.Exec(ctx, fmt.Sprintf("DELETE FROM %s d WHERE %s AND EXISTS (%s)%s%s",
				t.QName(), goneSQL(t, "d", keys, stage), match, andNot(ref), guard(t)))
			if err != nil {
				return fmt.Errorf("table %s: pull: clear unique-key conflicts: %w", name, err)
			}
			deleted += tag.RowsAffected()
			// A row the target never had (or still referenced): one of the two
			// rows has to go. With live winning the source's, unless something
			// points at it; else the incoming one, which the push then deletes.
			if !local {
				gone, err := collectKeys(ctx, srcTx, fmt.Sprintf("DELETE FROM %s d WHERE NOT EXISTS (SELECT 1 FROM %s s0 WHERE %s) AND EXISTS (%s)%s RETURNING ROW(%s)::text",
					t.QName(), stage, matchCols(t.PK, "s0", "d"), match, andNot(ref), pg.IdentList(t.PK, "d.")))
				if err != nil {
					return fmt.Errorf("table %s: pull: clear unique-key conflicts: %w", name, err)
				}
				if len(gone) > 0 {
					conflicts += int64(len(gone))
					deleted += int64(len(gone))
					s.log.Warn("two-way conflict: a local row holds a unique value a live row took; the local row is deleted",
						"table", name, "rows", len(gone), "key", strings.Join(u.Exprs, ","), "keys", listOf(gone))
				}
			}
			dropped, err := collectKeys(ctx, srcTx, fmt.Sprintf(`WITH dropped AS (DELETE FROM %[1]s x WHERE EXISTS (SELECT 1 FROM %[2]s d
					WHERE NOT EXISTS (SELECT 1 FROM %[1]s s0 WHERE %[3]s) AND EXISTS (%[4]s AND %[5]s)) RETURNING %[6]s)
				DELETE FROM %[7]s k USING dropped x WHERE %[8]s RETURNING ROW(%[9]s)::text`,
				stage, t.QName(), matchCols(t.PK, "s0", "d"), match, matchCols(t.PK, "s", "x"), pg.IdentList(t.PK, "x."),
				keys, matchCols(t.PK, "k", "x"), pg.IdentList(t.PK, "k.")))
			if err != nil {
				return fmt.Errorf("table %s: pull: set unique-key conflicts aside: %w", name, err)
			}
			if len(dropped) > 0 {
				conflicts += int64(len(dropped))
				s.log.Warn("two-way conflict: a live row takes a unique value a local row holds; the local row is kept",
					"table", name, "rows", len(dropped), "key", strings.Join(u.Exprs, ","), "keys", listOf(dropped))
			}
		}
	}

	// 4. write the target's rows, parents first (FK-cycle columns as NULL)
	for _, name := range order {
		t := tables[name]
		if staged[name] == 0 {
			continue
		}
		tag, err := srcTx.Exec(ctx, pullUpsertSQL(t, stageOf[name], s.stamp(t.SyncCol), strings.TrimPrefix(guard(t), " AND ")), at)
		if err != nil {
			return fmt.Errorf("table %s: pull: %w", name, err)
		}
		if n := tag.RowsAffected(); n > 0 {
			written += n
			s.log.Info("rows pulled from the live DB", "table", name, "rows", n)
		}
	}

	// 5. FK-cycle columns
	for _, name := range order {
		if t := tables[name]; len(t.Break) > 0 && staged[name] > 0 {
			if _, err := srcTx.Exec(ctx, fixBreakSQL(t, stageOf[name], "")); err != nil {
				return fmt.Errorf("table %s: pull: fill cycle columns: %w", name, err)
			}
		}
	}

	// 6. deletes, children first
	for i := len(order) - 1; i >= 0; i-- {
		name := order[i]
		t := tables[name]
		gone := goneSQL(t, "d", keysOf[name], stageOf[name])
		ref := referencedSQL(t, refs[name], keysOf[name], stageOf[name])
		if ref != "" {
			// Deleting it would fail, or take the rows that point at it along.
			kept, err := collectKeys(ctx, srcTx, fmt.Sprintf("UPDATE %s d SET %s = NULL WHERE %s AND %s%s RETURNING ROW(%s)::text",
				t.QName(), pg.Ident(t.SyncCol.Name), gone, ref, guard(t), pg.IdentList(t.PK, "d.")))
			if err != nil {
				return fmt.Errorf("table %s: pull: keep referenced rows: %w", name, err)
			}
			if len(kept) > 0 {
				s.log.Warn("deleted on the live DB, but local rows still point at them: kept, and written back to the live DB",
					"table", name, "rows", len(kept), "keys", listOf(kept))
			}
		}
		tag, err := srcTx.Exec(ctx, fmt.Sprintf("DELETE FROM %s d WHERE %s%s%s", t.QName(), gone, andNot(ref), guard(t)))
		if err != nil {
			return fmt.Errorf("table %s: pull: delete: %w", name, err)
		}
		if n := tag.RowsAffected(); n > 0 {
			deleted += n
			s.log.Info("rows deleted (deleted on the live DB)", "table", name, "rows", n)
		}
	}

	// Rows the target inserted took ids from its own sequences. Never moved
	// back: the source's own sequence values count too.
	if written > 0 {
		seqs, err := readSourceSequences(ctx, srcTx, tables)
		if err != nil {
			return err
		}
		if err := s.setSequences(ctx, srcTx, tables, seqs, false); err != nil {
			return err
		}
	}
	if err := srcTx.Commit(ctx); err != nil {
		return fmt.Errorf("commit source: %w", err)
	}
	// On the source's clock, like the snapshots of the pushes: one that starts
	// later has these rows (see waitingSQL).
	var pulledAt time.Time
	if err := src.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&pulledAt); err != nil {
		return err
	}

	// The source has them now. If this fails, the next pull reads the same
	// changes again and finds nothing left to write.
	batch = &pgx.Batch{}
	batch.Queue(fmt.Sprintf("UPDATE %s c SET pulled_at = $1 WHERE %s AND c.schema_name || '.' || c.table_name = ANY($2::text[])",
		s.liveChanges(), waitingSQL(time.Time{})), pulledAt, order)
	batch.Queue(fmt.Sprintf(`INSERT INTO %s (mode, snapshot_at, started_at, finished_at, tables, rows_read, rows_written, rows_deleted)
		VALUES ('pull', $1, $2, clock_timestamp(), $3, $4, $5, $6)`, pg.QName(cfg.MetaSchema, "run_log")),
		snapshot, started, len(order), read, written, deleted)
	batch.Queue("COMMIT")
	if err := dst.SendBatch(ctx, batch).Close(); err != nil {
		s.log.Warn("could not mark the changes pulled on the live DB; the next pull reads them again", "err", err)
	} else {
		open = false
	}
	s.log.Info("pull complete",
		"tables", listOf(order),
		"rows_read", read,
		"rows_written", written,
		"rows_deleted", deleted,
		"conflicts", conflicts,
		"took", time.Since(started).Round(time.Millisecond).String())
	return nil
}

// watchLive pulls the live changes to the two-way tables until ctx ends, apart
// from the pushes, so that a long one (Watch catching up after a start, an
// interval run) does not hold them up. It starts over after an error.
func (s *Syncer) watchLive(ctx context.Context) {
	backoff := retryMin
	for {
		err := s.watchLiveOnce(ctx, &backoff)
		if ctx.Err() != nil {
			return
		}
		s.log.Error("two-way pull failed; will retry", "err", err, "in", backoff.String())
		s.notify.Failed("two-way pull", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, retryMax)
	}
}

// watchLiveOnce listens on notifyChannel on the target and pulls after every
// change announced there. It first pulls what was logged while nothing was
// listening. backoff goes back to retryMin once a pull works.
func (s *Syncer) watchLiveOnce(ctx context.Context, backoff *time.Duration) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	listen, err := pg.Connect(ctx, s.cfg.TargetURL)
	if err != nil {
		return fmt.Errorf("connect target: %w", err)
	}
	notes := make(chan string, 1024)
	lost := make(chan error, 1)
	var wg sync.WaitGroup
	var src, dst *pgx.Conn // for the pulls
	defer func() {
		cancel()
		wg.Wait()
		for _, c := range []*pgx.Conn{listen, src, dst} {
			if c != nil {
				c.Close(context.Background())
			}
		}
	}()
	// Listen before the first pull, so that no change falls in between.
	if _, err := listen.Exec(ctx, "LISTEN "+pg.Ident(notifyChannel)); err != nil {
		return fmt.Errorf("listen on target: %w", err)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		relay(ctx, listen, notes, lost)
	}()
	if src, err = pg.Connect(ctx, s.cfg.SourceURL); err != nil {
		return fmt.Errorf("connect source: %w", err)
	}
	if dst, err = pg.Connect(ctx, s.cfg.TargetURL); err != nil {
		return fmt.Errorf("connect target: %w", err)
	}
	if err := guardDifferent(ctx, src, dst); err != nil {
		return err
	}
	if err := s.ensureMeta(ctx, dst); err != nil {
		return err
	}
	s.log.Info("two-way pull listening on the live DB", "two_way_kinds", strings.Join(s.cfg.TwoWayKinds, ","))

	for {
		wait := (<-chan time.Time)(nil) // another sync pulls: try again later
		switch err := s.pull(ctx, src, dst); {
		case err == nil:
			*backoff = retryMin
			s.notify.Recovered("two-way pull")
		case errors.Is(err, errBusy):
			wait = time.After(retryMin)
		default:
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-lost:
			return fmt.Errorf("listen on target: %w", err)
		case <-wait:
			continue
		case <-notes:
		}
		// Gather the rest of the save; the pull reads every table logged.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pullSettle):
		}
		for drained := false; !drained; {
			select {
			case <-notes:
			default:
				drained = true
			}
		}
	}
}

// collectKeys runs q and returns its single text column.
func collectKeys(ctx context.Context, tx pgx.Tx, q string) ([]string, error) {
	rows, err := tx.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// planPull prints, for -dry-run, the live changes a run would pull first.
func (s *Syncer) planPull(ctx context.Context, dst *pgx.Conn, tables map[string]*schema.Table) error {
	var logged bool
	if err := dst.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", s.liveChanges()).Scan(&logged); err != nil {
		return err
	}
	if !logged {
		s.log.Info("DRY RUN: no live changes captured yet; the first real run puts the capture triggers on the live DB")
		return nil
	}
	rows, err := dst.Query(ctx, fmt.Sprintf("SELECT c.schema_name || '.' || c.table_name, count(DISTINCT c.pk) FROM %s c WHERE %s GROUP BY 1 ORDER BY 1",
		s.liveChanges(), waitingSQL(time.Time{})))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var n int64
		if err := rows.Scan(&name, &n); err != nil {
			return err
		}
		if t := tables[name]; t != nil && t.TwoWay {
			s.log.Info("DRY RUN: live changes would be pulled", "table", name, "rows", n)
		}
	}
	return rows.Err()
}
