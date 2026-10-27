package syncer

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/vksoftwares/vknex-dbsync/internal/pg"
	"github.com/vksoftwares/vknex-dbsync/internal/schema"
)

// incremental makes the target identical to the source snapshot without
// truncating anything. The order avoids foreign-key and unique-key errors:
//
//  1. stage changed rows on the target, and find the target rows that are
//     gone from the source: from the source's delete log when that accounts
//     for the difference, else by staging every source key (see look.go)
//  2. remove target rows that are gone from the source AND block an incoming
//     row on a unique key (e.g. a product code deleted and re-created)
//  3. upsert changed rows, parents first (FK-cycle columns written as NULL)
//  4. fill FK-cycle columns now that every referenced row exists
//  5. delete rows that are gone from the source, children first
//
// Target rows of a -two-way table whose key has a live change the source
// snapshot (read at snapshot) does not have are left alone by every step.
func (s *Syncer) incremental(ctx context.Context, src *pgx.Conn, dst pgx.Tx, tables map[string]*schema.Table, order []string, state map[string]time.Time,
	snapshot time.Time, st *runStats) error {
	srcPg := src.PgConn()
	deletes := s.cfg.Deletes
	live := func(t *schema.Table, alias string) string { return s.liveWaiting(t, alias, snapshot) }

	stageOf := map[string]string{} // stage table of the rows to write; none = nothing to write
	goneOf := map[string]string{}  // matches the target rows (alias d) gone from the source; none = nothing to delete
	staged := map[string]int64{}
	notIn := func(t *schema.Table, keys string) string {
		return fmt.Sprintf("NOT EXISTS (SELECT 1 FROM %s k WHERE %s)", keys, matchCols(t.PK, "k", "d"))
	}

	// 1. stage, after a look at what changed (see look.go): a table with
	// nothing to move gets no stage table, and every stage table is created
	// in one batch.
	wheres := map[string]string{} // tables with a watermark: their changed rows
	for _, name := range order {
		t := tables[name]
		st.Tables++
		if since, ok := state[name]; ok && t.UpdatedCol != nil && len(t.PK) > 0 {
			wheres[name] = s.changedSince(t, since.Add(-s.cfg.Overlap))
		}
	}
	changed, probes, logged, err := s.lookSource(ctx, src, tables, order, wheres)
	if err != nil {
		return err
	}
	var creates []string
	for i, name := range order {
		if wheres[name] != "" && changed[name] {
			stageOf[name] = fmt.Sprintf("_sync_s_%d", i)
			creates = append(creates, createStageSQL(tables[name], stageOf[name], tables[name].ColNames()))
		}
	}
	same, err := s.lookTarget(ctx, dst, creates, probes)
	if err != nil {
		return err
	}
	creates = nil
	unsettled := map[string]bool{} // keys differ from the source's: rows were inserted or deleted
	for i, name := range order {
		t := tables[name]
		switch {
		case len(t.PK) == 0 || same[name]:
			// replaced wholesale in step 3 / the target has the same rows (no
			// watermark) or the same keys (no row to delete)
		case wheres[name] == "":
			stageOf[name] = fmt.Sprintf("_sync_s_%d", i)
			creates = append(creates, createStageSQL(t, stageOf[name], t.ColNames()))
			if deletes {
				goneOf[name] = notIn(t, stageOf[name]) // the whole table is staged: its keys are all there
			}
		case deletes:
			unsettled[name] = true
		}
	}
	if err := s.fillStages(ctx, srcPg, dst, tables, order, creates, func(name string) (string, []string, string) {
		return stageOf[name], tables[name].ColNames(), wheres[name]
	}, func(name string, n int64) {
		staged[name] = n
		st.Copied += n
	}); err != nil {
		return err
	}

	// 1b. A table whose keys differ needs its rows to delete. When the staged
	// rows and the rows in the source's delete log account for the difference
	// (the target then holds the source's keys), those are the logged rows;
	// else every source key is staged and the rows missing there are.
	if len(unsettled) > 0 {
		want := map[string]string{}
		for _, p := range probes {
			want[p.name] = p.want
		}
		var checks []probe
		for _, name := range order {
			if !unsettled[name] {
				continue
			}
			t := tables[name]
			if keys := logged[name]; keys != "" && keys != "[]" {
				goneOf[name] = loggedGone(t, keys)
			}
			checks = append(checks, probe{name: name, sql: settledSQL(t, stageOf[name], goneOf[name]), want: want[name]})
		}
		settled, err := s.lookTarget(ctx, dst, nil, checks)
		if err != nil {
			return err
		}
		keysOf := map[string]string{}
		creates = nil
		for i, name := range order {
			if !unsettled[name] || settled[name] {
				continue
			}
			t := tables[name]
			keysOf[name] = fmt.Sprintf("_sync_k_%d", i)
			creates = append(creates, createStageSQL(t, keysOf[name], t.PK))
			goneOf[name] = notIn(t, keysOf[name])
		}
		if err := s.fillStages(ctx, srcPg, dst, tables, order, creates, func(name string) (string, []string, string) {
			return keysOf[name], tables[name].PK, ""
		}, nil); err != nil {
			return err
		}
	}

	// Steps 2 to 5 go to the target in as few round trips as their order
	// allows: their statements are queued, and sent before a table that
	// needs a round trip of its own (no primary key, NOT VALID constraints).
	q := &queue{}
	done := func(name, what string, count func(n int64)) func(pgconn.CommandTag, error) error {
		return func(tag pgconn.CommandTag, err error) error {
			if err != nil {
				return fmt.Errorf("table %s: %s: %w", name, what, hint(err))
			}
			if count != nil {
				count(tag.RowsAffected())
			}
			return nil
		}
	}

	// 2. unique-key swaps
	if deletes {
		for i := len(order) - 1; i >= 0; i-- {
			name := order[i]
			t := tables[name]
			if staged[name] == 0 || len(t.Uniques) == 0 {
				continue
			}
			for _, u := range t.Uniques {
				if goneOf[name] != "" { // else the target has no row the source lacks
					q.add(fmt.Sprintf("DELETE FROM %s d WHERE %s AND EXISTS (%s)%s",
						t.QName(), goneOf[name], uniqueMatchSQL(t, u, stageOf[name]), andNot(live(t, "d"))),
						done(name, "clear unique-key conflicts", func(n int64) {
							if n > 0 {
								st.Deleted += n
								s.log.Info("rows deleted (unique key re-used)", "table", name, "rows", n, "key", strings.Join(u.Exprs, ","))
							}
						}))
				}
				if !s.cfg.DisableTriggers {
					continue
				}
				// Rows both sides have can hold each other's unique values (ids assigned
				// separately on each side); updating them one by one would collide midway.
				// Such a target row is staged, so remove it now and let step 3 write it back.
				// Only without triggers: a cascading foreign key would take its children along.
				q.add(fmt.Sprintf("DELETE FROM %s d WHERE EXISTS (SELECT 1 FROM %s s0 WHERE %s) AND EXISTS (%s AND NOT (%s))%s",
					t.QName(), stageOf[name], matchCols(t.PK, "s0", "d"), uniqueMatchSQL(t, u, stageOf[name]), matchCols(t.PK, "s", "d"),
					andNot(live(t, "d"))),
					done(name, "clear unique-key conflicts", func(n int64) {
						if n > 0 {
							s.log.Info("rows rewritten (unique key moved to another row)", "table", name, "rows", n, "key", strings.Join(u.Exprs, ","))
						}
					}))
			}
		}
	}

	// 3. upsert, parents first
	for _, name := range order {
		t := tables[name]
		switch {
		case len(t.PK) == 0:
			if same[name] {
				continue // the target has the same rows
			}
			if err := q.send(ctx, dst); err != nil {
				return err
			}
			// No primary key: rows cannot be matched one-to-one, so replace the table.
			tag, err := dst.Exec(ctx, "DELETE FROM "+t.QName())
			if err != nil {
				return fmt.Errorf("table %s: clear: %w", name, hint(err))
			}
			if _, err := s.relaxNotValid(ctx, dst, t, "", st); err != nil {
				return fmt.Errorf("table %s: %w", name, err)
			}
			n, err := pg.CopyStream(ctx, srcPg, dst.Conn().PgConn(), selectSQL(t, selectExprs(t, false), ""), t.QName(), t.ColNames(), t.TextCopy)
			if err != nil {
				return fmt.Errorf("table %s: %w", name, hint(err))
			}
			st.Copied += n
			st.Upserted += n
			st.Deleted += tag.RowsAffected()
			s.log.Info("table replaced (no primary key)", "table", name, "rows", n)
		case staged[name] == 0:
		case hasNotValid(t):
			if err := q.send(ctx, dst); err != nil {
				return err
			}
			w, err := s.upsert(ctx, dst, t, stageOf[name], live(t, "s"), st)
			if err != nil {
				return fmt.Errorf("table %s: upsert: %w", name, hint(err))
			}
			if w > 0 {
				st.Upserted += w
				s.log.Info("rows written", "table", name, "rows", w, "scanned", staged[name])
			}
		default:
			q.add(upsertSQL(t, stageOf[name], live(t, "s")), done(name, "upsert", func(w int64) {
				if w > 0 {
					st.Upserted += w
					s.log.Info("rows written", "table", name, "rows", w, "scanned", staged[name])
				}
			}))
		}
	}

	// 4. FK-cycle columns
	for _, name := range order {
		t := tables[name]
		if len(t.Break) == 0 || len(t.PK) == 0 || staged[name] == 0 {
			continue
		}
		q.add(fixBreakSQL(t, stageOf[name], live(t, "s")), done(name, "fill cycle columns", nil))
	}

	// 5. deletes, children first
	if deletes {
		// Rows about to be deleted may point at each other through a cycle column.
		for _, name := range order {
			t := tables[name]
			if len(t.Break) == 0 || goneOf[name] == "" {
				continue
			}
			sets, notNull := make([]string, len(t.Break)), make([]string, len(t.Break))
			for i, c := range t.Break {
				sets[i] = pg.Ident(c) + " = NULL"
				notNull[i] = "d." + pg.Ident(c) + " IS NOT NULL"
			}
			q.add(fmt.Sprintf("UPDATE %s d SET %s WHERE (%s) AND %s%s",
				t.QName(), strings.Join(sets, ", "), strings.Join(notNull, " OR "), goneOf[name], andNot(live(t, "d"))),
				done(name, "detach cycle columns", nil))
		}
		for i := len(order) - 1; i >= 0; i-- {
			name := order[i]
			t := tables[name]
			if goneOf[name] == "" {
				continue // no primary key, or the target has no row the source lacks
			}
			q.add(fmt.Sprintf("DELETE FROM %s d WHERE %s%s", t.QName(), goneOf[name], andNot(live(t, "d"))),
				done(name, "delete removed rows", func(n int64) {
					if n > 0 {
						st.Deleted += n
						s.log.Info("rows deleted", "table", name, "rows", n)
					}
				}))
		}
	}
	if err := q.send(ctx, dst); err != nil {
		return err
	}
	// Stage tables are dropped at commit (ON COMMIT DROP).
	return nil
}

// stage copies selected source columns into a temp table on the target and
// returns the number of rows copied.
func (s *Syncer) stage(ctx context.Context, srcPg *pgconn.PgConn, dst pgx.Tx, t *schema.Table, stage string, cols []string, where string) (int64, error) {
	if _, err := dst.Exec(ctx, createStageSQL(t, stage, cols)); err != nil {
		return 0, fmt.Errorf("table %s: create stage: %w", t.Name, err)
	}
	return s.fillStage(ctx, srcPg, dst, t, stage, cols, where)
}

// createStageSQL creates the temp table stage for cols of t.
func createStageSQL(t *schema.Table, stage string, cols []string) string {
	if len(cols) == len(t.Columns) && len(t.All) > len(t.Columns) {
		// Keep generated columns, computed on COPY, so unique keys that use them can be matched.
		return fmt.Sprintf("CREATE TEMP TABLE %s (LIKE %s INCLUDING GENERATED) ON COMMIT DROP", stage, t.QName())
	}
	return fmt.Sprintf("CREATE TEMP TABLE %s ON COMMIT DROP AS SELECT %s FROM %s WITH NO DATA", stage, pg.IdentList(cols, ""), t.QName())
}

// fillStage copies the source rows matching where (all for "") into stage
// and returns the number of rows copied.
func (s *Syncer) fillStage(ctx context.Context, srcPg *pgconn.PgConn, dst pgx.Tx, t *schema.Table, stage string, cols []string, where string) (int64, error) {
	text := false
	for _, c := range cols {
		text = text || t.Column(c).NeedText
	}
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = pg.Ident(c)
	}
	n, err := pg.CopyStream(ctx, srcPg, dst.Conn().PgConn(), selectSQL(t, quoted, where), stage, cols, text)
	if err != nil {
		return 0, fmt.Errorf("table %s: %w", t.Name, err)
	}
	if n >= 10000 { // small stages plan fine without statistics; skip the round trip
		if _, err := dst.Exec(ctx, "ANALYZE "+stage); err != nil {
			return 0, err
		}
	}
	return n, nil
}

// fillStages creates the stage tables of creates in one batch, then fills,
// in order, the stage each table has (stageOf: its name, columns and the
// rows' condition; "" for none), created now or before. done, if not nil,
// gets each row count.
func (s *Syncer) fillStages(ctx context.Context, srcPg *pgconn.PgConn, dst pgx.Tx, tables map[string]*schema.Table, order, creates []string,
	stageOf func(name string) (stage string, cols []string, where string), done func(name string, n int64)) error {
	if len(creates) > 0 {
		b := &pgx.Batch{}
		for _, q := range creates {
			b.Queue(q)
		}
		if err := dst.SendBatch(ctx, b).Close(); err != nil {
			return fmt.Errorf("create stage tables: %w", err)
		}
	}
	for _, name := range order {
		stage, cols, where := stageOf(name)
		if stage == "" {
			continue
		}
		n, err := s.fillStage(ctx, srcPg, dst, tables[name], stage, cols, where)
		if err != nil {
			return err
		}
		if done != nil {
			done(name, n)
		}
	}
	return nil
}

// upsert writes the staged rows, except those matching skip. On a table with
// NOT VALID constraints it runs in a savepoint: when a source row breaks one
// of them, the target's copy is set aside (see relaxNotValid) and the upsert
// is retried.
func (s *Syncer) upsert(ctx context.Context, dst pgx.Tx, t *schema.Table, stage, skip string, st *runStats) (int64, error) {
	q := upsertSQL(t, stage, skip)
	if !hasNotValid(t) {
		tag, err := dst.Exec(ctx, q)
		return tag.RowsAffected(), err
	}
	for {
		sp, err := dst.Begin(ctx)
		if err != nil {
			return 0, err
		}
		tag, err := sp.Exec(ctx, q)
		if err == nil {
			return tag.RowsAffected(), sp.Commit(ctx)
		}
		if rbErr := sp.Rollback(ctx); rbErr != nil {
			return 0, err
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || (pgErr.Code != "23514" && pgErr.Code != "23503") {
			return 0, err
		}
		relaxed, rErr := s.relaxNotValid(ctx, dst, t, pgErr.ConstraintName, st)
		if rErr != nil {
			return 0, rErr
		}
		if !relaxed {
			return 0, err
		}
	}
}

// hasNotValid reports whether t has a NOT VALID constraint, which a source
// row may break (see upsert).
func hasNotValid(t *schema.Table) bool {
	return slices.ContainsFunc(t.Constraints, func(c schema.Constraint) bool { return strings.HasSuffix(c.Def, " NOT VALID") })
}

// queue collects target statements to send in one round trip, each with what
// to do with its result. The first failure ends the run, as it would one
// statement at a time.
type queue struct {
	b    *pgx.Batch
	then []func(pgconn.CommandTag, error) error
}

func (q *queue) add(sql string, then func(pgconn.CommandTag, error) error) {
	if q.b == nil {
		q.b = &pgx.Batch{}
	}
	q.b.Queue(sql)
	q.then = append(q.then, then)
}

// send runs the queued statements in order and empties the queue.
func (q *queue) send(ctx context.Context, tx pgx.Tx) error {
	if len(q.then) == 0 {
		return nil
	}
	b, then := q.b, q.then
	q.b, q.then = nil, nil
	br := tx.SendBatch(ctx, b)
	for _, f := range then {
		tag, err := br.Exec()
		if err := f(tag, err); err != nil {
			br.Close()
			return err
		}
	}
	return br.Close()
}

// relaxNotValid drops the target's copy of t's NOT VALID constraints (all of
// them, or only the named one) and queues them to be re-added NOT VALID after
// the data. Rows on the source may break such a constraint, and the target has
// to take them as they are. Returns false when there was nothing to drop.
func (s *Syncer) relaxNotValid(ctx context.Context, dst pgx.Tx, t *schema.Table, only string, st *runStats) (bool, error) {
	relaxed := false
	for _, c := range t.Constraints {
		if !strings.HasSuffix(c.Def, " NOT VALID") || (only != "" && c.Name != only) {
			continue
		}
		var exists bool
		if err := dst.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = $1::regclass AND conname = $2)",
			t.QName(), c.Name).Scan(&exists); err != nil {
			return false, err
		}
		if !exists {
			continue
		}
		if _, err := dst.Exec(ctx, fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", t.QName(), pg.Ident(c.Name))); err != nil {
			return false, fmt.Errorf("set aside NOT VALID constraint %s: %w", c.Name, err)
		}
		s.log.Info("NOT VALID constraint set aside for the data copy, re-added after", "table", schema.Key(t.Schema, t.Name), "constraint", c.Name)
		st.ReAdd = append(st.ReAdd, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s", t.QName(), pg.Ident(c.Name), c.Def))
		relaxed = true
	}
	return relaxed, nil
}
