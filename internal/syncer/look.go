package syncer

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/vksoftwares/vknex-dbsync/internal/pg"
	"github.com/vksoftwares/vknex-dbsync/internal/schema"
)

// Before an incremental run stages anything, it looks for the tables that have
// nothing to move: every statement on the target is a round trip, and over a
// slow link the stage tables, their COPYs and the deletes of a table cost
// seconds even when nothing changed. The source answers locally:
//
//   - a table with a watermark: are there changed rows? (EXISTS)
//   - with -deletes, a fingerprint of its keys: when the target's is the same,
//     no row was inserted or deleted, so the keys need not be staged and
//     nothing is deleted.
//   - a table staged whole (no watermark, no change column) or replaced whole
//     (no primary key): a fingerprint of its rows: when the target's is the
//     same, the table is left alone.
//
// The target's fingerprints come back in one batch, together with the stage
// tables already known to be needed.
//
// A table whose keys differ (rows were inserted or deleted) needs the target
// rows that are gone from the source. Staging every source key finds them,
// but sends the whole key list over the link. So first, once the changed rows
// are staged, the target fingerprints the keys it will hold with those rows
// written and the rows in the source's delete log (-mark-synced) deleted
// (settledSQL). When that matches the source's keys, the logged rows are the
// ones to delete (none, when only rows were inserted); else the keys are
// staged. The log is only trusted where the fingerprint confirms it.

// probe is one fingerprint to compare.
type probe struct {
	name string // table key
	sql  string // fingerprintSQL
	want string // the source's fingerprint
}

// fingerprintSQL sums a 128-bit hash of each row's cols, next to the row
// count, so that a table can be compared with its copy without moving its
// rows: equal results mean equal rows. The sum does not depend on the row
// order, so the servers' collations do not matter. Values are hashed as text:
// timestamptz is written in UTC, so that the servers' TimeZone settings do not
// matter, and floats exactly (extra_float_digits, see lookSource). Where the
// servers still write the same value differently (another DateStyle, an
// interval written another way) the results differ and the table is staged as
// before; equal text always means equal values.
func fingerprintSQL(t *schema.Table, cols []string) string {
	return fingerprintFromSQL(t, cols, t.QName())
}

// fingerprintFromSQL is fingerprintSQL over the rows of from, a table or a
// subquery with t's cols.
func fingerprintFromSQL(t *schema.Table, cols []string, from string) string {
	exprs := make([]string, len(cols))
	for i, name := range cols {
		exprs[i] = pg.Ident(name)
		if c := t.AnyColumn(name); c != nil && strings.HasPrefix(c.Type, "timestamp") && strings.HasSuffix(c.Type, "with time zone") {
			exprs[i] = "(" + exprs[i] + " AT TIME ZONE 'UTC')"
		}
	}
	return fmt.Sprintf("SELECT count(*)::text || ':' || COALESCE(sum(('x' || left(h, 16))::bit(64)::bigint::numeric), 0)::text "+
		"|| ':' || COALESCE(sum(('x' || right(h, 16))::bit(64)::bigint::numeric), 0)::text "+
		"FROM (SELECT md5(ROW(%s)::text) AS h FROM %s) f", strings.Join(exprs, ", "), from)
}

// maxLogged bounds the delete log sent to the target as a literal (see
// loggedSQL); a longer one is left out, and the keys are staged.
const maxLogged = 1 << 20

// loggedSQL lists, as a jsonb array, the keys of t's rows that wait in the
// source's delete log (public.sync_deleted_rows, see -mark-synced) and that
// the source does not have (again): the rows deleted since the last sync.
func loggedSQL(t *schema.Table) string {
	return fmt.Sprintf("SELECT COALESCE(jsonb_agg(x.sdr_pk), '[]')::text FROM public.sync_deleted_rows x "+
		"WHERE x.sdr_sync_date IS NULL AND x.sdr_schema = %s AND x.sdr_table = %s "+
		"AND NOT EXISTS (SELECT 1 FROM jsonb_populate_record(NULL::%s, x.sdr_pk) l JOIN %s s ON %s)",
		pg.Literal(t.Schema), pg.Literal(t.Name), t.QName(), t.QName(), matchCols(t.PK, "s", "l"))
}

// loggedGone matches the target rows (alias d) whose keys are in logged, a
// jsonb array from loggedSQL.
func loggedGone(t *schema.Table, logged string) string {
	return fmt.Sprintf("EXISTS (SELECT 1 FROM jsonb_populate_recordset(NULL::%s, %s::jsonb) l WHERE %s)",
		t.QName(), pg.Literal(logged), matchCols(t.PK, "l", "d"))
}

// settledSQL fingerprints the keys the target will hold once the rows in
// stage ("" for none) are written and the rows matching gone (a condition on
// d, "" for none) are deleted. When that matches the source's keys, the keys
// need not be staged to find the rows to delete: they are the gone ones.
func settledSQL(t *schema.Table, stage, gone string) string {
	from := fmt.Sprintf("(SELECT %s FROM %s d", pg.IdentList(t.PK, "d."), t.QName())
	if gone != "" {
		from += " WHERE NOT " + gone
	}
	if stage != "" {
		from += fmt.Sprintf(" UNION SELECT %s FROM %s", pg.IdentList(t.PK, ""), stage)
	}
	return fingerprintFromSQL(t, t.PK, from+") f0")
}

// exactFloats makes float output exact (shortest round-trip form, the default
// since PostgreSQL 12) for the rest of the transaction, in case a server is
// configured otherwise.
const exactFloats = "SET LOCAL extra_float_digits = 1"

// lookSource reads, in the source snapshot, which tables with a watermark
// (wheres) have changed rows, and the fingerprints to compare on the target:
// the rows of tables staged or replaced whole, the keys of the others with
// -deletes. With -mark-synced it also reads the delete log of those (logged,
// see loggedSQL).
func (s *Syncer) lookSource(ctx context.Context, src *pgx.Conn, tables map[string]*schema.Table, order []string, wheres map[string]string) (
	changed map[string]bool, probes []probe, logged map[string]string, err error) {
	changed, logged = map[string]bool{}, map[string]string{}
	useLog := false
	if s.cfg.MarkSynced && s.cfg.Deletes {
		if err := src.QueryRow(ctx, "SELECT to_regclass('public.sync_deleted_rows') IS NOT NULL").Scan(&useLog); err != nil {
			return nil, nil, nil, fmt.Errorf("compare with the target: %w", err)
		}
	}
	b := &pgx.Batch{}
	b.Queue(exactFloats)
	for _, name := range order {
		t := tables[name]
		var p probe
		switch where := wheres[name]; {
		case where == "": // staged or replaced whole
			p = probe{name: name, sql: fingerprintSQL(t, t.ColNames())}
		default:
			b.Queue("SELECT EXISTS (" + selectSQL(t, []string{"1"}, where) + ")").QueryRow(func(row pgx.Row) error {
				var yes bool
				err := row.Scan(&yes)
				changed[name] = yes
				return err
			})
			if !s.cfg.Deletes {
				continue
			}
			if useLog {
				b.Queue(loggedSQL(t)).QueryRow(func(row pgx.Row) error {
					var keys string
					err := row.Scan(&keys)
					if len(keys) <= maxLogged {
						logged[name] = keys
					}
					return err
				})
			}
			p = probe{name: name, sql: fingerprintSQL(t, t.PK)}
		}
		i := len(probes)
		probes = append(probes, p)
		b.Queue(p.sql).QueryRow(func(row pgx.Row) error { return row.Scan(&probes[i].want) })
	}
	if err := src.SendBatch(ctx, b).Close(); err != nil {
		return nil, nil, nil, fmt.Errorf("compare with the target: read the source: %w", err)
	}
	return changed, probes, logged, nil
}

// lookTarget runs the statements of before, then the probes, in one round
// trip, and reports the tables whose fingerprint matches the source's. The
// probes run in a savepoint: when one fails (say md5 is disabled), they are
// rolled back and none matches, so every table is staged as before.
func (s *Syncer) lookTarget(ctx context.Context, dst pgx.Tx, before []string, probes []probe) (map[string]bool, error) {
	same := map[string]bool{}
	b := &pgx.Batch{}
	for _, q := range before {
		b.Queue(q)
	}
	if len(probes) > 0 {
		b.Queue("SAVEPOINT _sync_look")
		b.Queue(exactFloats)
		for _, p := range probes {
			b.Queue(p.sql)
		}
		b.Queue("RELEASE SAVEPOINT _sync_look")
	}
	if b.Len() == 0 {
		return same, nil
	}
	br := dst.SendBatch(ctx, b)
	for _, q := range before {
		if _, err := br.Exec(); err != nil {
			br.Close()
			return nil, fmt.Errorf("%s: %w", q, err)
		}
	}
	if len(probes) == 0 {
		return same, br.Close()
	}
	var failed error
	if _, err := br.Exec(); err != nil { // SAVEPOINT
		br.Close()
		return nil, err
	}
	if _, err := br.Exec(); err != nil {
		failed = err
	}
	for _, p := range probes {
		var got string
		if err := br.QueryRow().Scan(&got); err != nil {
			if failed == nil {
				failed = fmt.Errorf("table %s: %w", p.name, err)
			}
			continue
		}
		same[p.name] = got == p.want
	}
	if _, err := br.Exec(); err != nil && failed == nil { // RELEASE
		failed = err
	}
	if err := br.Close(); err != nil && failed == nil {
		failed = err
	}
	if failed == nil {
		return same, nil
	}
	if _, err := dst.Exec(ctx, "ROLLBACK TO SAVEPOINT _sync_look"); err != nil {
		return nil, fmt.Errorf("compare with the target: %w (after: %v)", err, failed)
	}
	s.log.Warn("could not compare the tables with the target; their rows or keys are staged instead", "err", failed)
	return map[string]bool{}, nil
}
