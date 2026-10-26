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
//  1. stage changed rows (and, for deletes, every source key) on the target
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

	stageOf := map[string]string{}
	keysOf := map[string]string{}
	staged := map[string]int64{}

	// 1. stage
	for i, name := range order {
		t := tables[name]
		st.Tables++
		if len(t.PK) == 0 {
			continue // replaced wholesale in step 3
		}
		where := ""
		if since, ok := state[name]; ok && t.UpdatedCol != nil {
			where = s.changedSince(t, since.Add(-s.cfg.Overlap))
		}
		stageOf[name] = fmt.Sprintf("_sync_s_%d", i)
		n, err := s.stage(ctx, srcPg, dst, t, stageOf[name], t.ColNames(), where)
		if err != nil {
			return err
		}
		staged[name] = n
		st.Copied += n

		switch {
		case deletes && where == "":
			keysOf[name] = stageOf[name] // the whole table is staged: its keys are all there
		case deletes:
			keysOf[name] = fmt.Sprintf("_sync_k_%d", i)
			if _, err := s.stage(ctx, srcPg, dst, t, keysOf[name], t.PK, ""); err != nil {
				return err
			}
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
				tag, err := dst.Exec(ctx, fmt.Sprintf("DELETE FROM %s d WHERE NOT EXISTS (SELECT 1 FROM %s k WHERE %s) AND EXISTS (%s)%s",
					t.QName(), keysOf[name], matchCols(t.PK, "k", "d"), uniqueMatchSQL(t, u, stageOf[name]), andNot(live(t, "d"))))
				if err != nil {
					return fmt.Errorf("table %s: clear unique-key conflicts: %w", name, hint(err))
				}
				if n := tag.RowsAffected(); n > 0 {
					st.Deleted += n
					s.log.Info("rows deleted (unique key re-used)", "table", name, "rows", n, "key", strings.Join(u.Exprs, ","))
				}
				if !s.cfg.DisableTriggers {
					continue
				}
				// Rows both sides have can hold each other's unique values (ids assigned
				// separately on each side); updating them one by one would collide midway.
				// Such a target row is staged, so remove it now and let step 3 write it back.
				// Only without triggers: a cascading foreign key would take its children along.
				tag, err = dst.Exec(ctx, fmt.Sprintf("DELETE FROM %s d WHERE EXISTS (SELECT 1 FROM %s s0 WHERE %s) AND EXISTS (%s AND NOT (%s))%s",
					t.QName(), stageOf[name], matchCols(t.PK, "s0", "d"), uniqueMatchSQL(t, u, stageOf[name]), matchCols(t.PK, "s", "d"),
					andNot(live(t, "d"))))
				if err != nil {
					return fmt.Errorf("table %s: clear unique-key conflicts: %w", name, hint(err))
				}
				if n := tag.RowsAffected(); n > 0 {
					s.log.Info("rows rewritten (unique key moved to another row)", "table", name, "rows", n, "key", strings.Join(u.Exprs, ","))
				}
			}
		}
	}

	// 3. upsert, parents first
	for _, name := range order {
		t := tables[name]
		if len(t.PK) == 0 {
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
			continue
		}
		if staged[name] > 0 {
			w, err := s.upsert(ctx, dst, t, stageOf[name], live(t, "s"), st)
			if err != nil {
				return fmt.Errorf("table %s: upsert: %w", name, hint(err))
			}
			if w > 0 {
				st.Upserted += w
				s.log.Info("rows written", "table", name, "rows", w, "scanned", staged[name])
			}
		}
	}

	// 4. FK-cycle columns
	for _, name := range order {
		t := tables[name]
		if len(t.Break) == 0 || len(t.PK) == 0 || staged[name] == 0 {
			continue
		}
		if _, err := dst.Exec(ctx, fixBreakSQL(t, stageOf[name], live(t, "s"))); err != nil {
			return fmt.Errorf("table %s: fill cycle columns: %w", name, hint(err))
		}
	}

	// 5. deletes, children first
	if deletes {
		// Rows about to be deleted may point at each other through a cycle column.
		for _, name := range order {
			t := tables[name]
			if len(t.Break) == 0 || len(t.PK) == 0 {
				continue
			}
			sets, notNull := make([]string, len(t.Break)), make([]string, len(t.Break))
			for i, c := range t.Break {
				sets[i] = pg.Ident(c) + " = NULL"
				notNull[i] = "d." + pg.Ident(c) + " IS NOT NULL"
			}
			if _, err := dst.Exec(ctx, fmt.Sprintf("UPDATE %s d SET %s WHERE (%s) AND NOT EXISTS (SELECT 1 FROM %s k WHERE %s)%s",
				t.QName(), strings.Join(sets, ", "), strings.Join(notNull, " OR "), keysOf[name], matchCols(t.PK, "k", "d"),
				andNot(live(t, "d")))); err != nil {
				return fmt.Errorf("table %s: detach cycle columns: %w", name, hint(err))
			}
		}
		for i := len(order) - 1; i >= 0; i-- {
			name := order[i]
			t := tables[name]
			if len(t.PK) == 0 {
				continue
			}
			tag, err := dst.Exec(ctx, fmt.Sprintf("DELETE FROM %s d WHERE NOT EXISTS (SELECT 1 FROM %s k WHERE %s)%s",
				t.QName(), keysOf[name], matchCols(t.PK, "k", "d"), andNot(live(t, "d"))))
			if err != nil {
				return fmt.Errorf("table %s: delete removed rows: %w", name, hint(err))
			}
			if n := tag.RowsAffected(); n > 0 {
				st.Deleted += n
				s.log.Info("rows deleted", "table", name, "rows", n)
			}
		}
	}
	// Stage tables are dropped at commit (ON COMMIT DROP).
	return nil
}

// stage copies selected source columns into a temp table on the target and
// returns the number of rows copied.
func (s *Syncer) stage(ctx context.Context, srcPg *pgconn.PgConn, dst pgx.Tx, t *schema.Table, stage string, cols []string, where string) (int64, error) {
	text := false
	for _, c := range cols {
		text = text || t.Column(c).NeedText
	}
	create := fmt.Sprintf("CREATE TEMP TABLE %s ON COMMIT DROP AS SELECT %s FROM %s WITH NO DATA", stage, pg.IdentList(cols, ""), t.QName())
	if len(cols) == len(t.Columns) && len(t.All) > len(t.Columns) {
		// Keep generated columns, computed on COPY, so unique keys that use them can be matched.
		create = fmt.Sprintf("CREATE TEMP TABLE %s (LIKE %s INCLUDING GENERATED) ON COMMIT DROP", stage, t.QName())
	}
	if _, err := dst.Exec(ctx, create); err != nil {
		return 0, fmt.Errorf("table %s: create stage: %w", t.Name, err)
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

// upsert writes the staged rows, except those matching skip. On a table with
// NOT VALID constraints it runs in a savepoint: when a source row breaks one
// of them, the target's copy is set aside (see relaxNotValid) and the upsert
// is retried.
func (s *Syncer) upsert(ctx context.Context, dst pgx.Tx, t *schema.Table, stage, skip string, st *runStats) (int64, error) {
	q := upsertSQL(t, stage, skip)
	if !slices.ContainsFunc(t.Constraints, func(c schema.Constraint) bool { return strings.HasSuffix(c.Def, " NOT VALID") }) {
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
