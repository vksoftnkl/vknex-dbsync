package syncer

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/vksoftwares/vknex-dbsync/internal/pg"
	"github.com/vksoftwares/vknex-dbsync/internal/schema"
)

// With -mark-synced the run follows the source's own change tracking: a row
// whose sync column (e.g. abj_sync_date) is NULL has changes the live DB has
// not seen yet, and deletes wait in public.sync_deleted_rows with
// sdr_sync_date NULL. After a successful run those rows get the sync time on
// both sides. With -mark-txn=false only the tables -instant mirrors (the
// master tables, Table.Txn false) are stamped: the others have no SyncCol
// (schema.Select).

// pendingRows are a table's rows waiting at the snapshot: key and row version.
type pendingRows struct {
	keys, versions []string
}

type syncMarks struct {
	rows        map[string]*pendingRows // table key -> rows waiting
	deleteLog   bool                    // public.sync_deleted_rows exists on the source
	deletesUpTo int64                   // highest waiting sync_deleted_rows id at the snapshot
}

// drop removes the rows whose key is in keys.
func (p *pendingRows) drop(keys map[string]bool) {
	if len(keys) == 0 {
		return
	}
	var ks, vs []string
	for i, k := range p.keys {
		if !keys[k] {
			ks, vs = append(ks, k), append(vs, p.versions[i])
		}
	}
	p.keys, p.versions = ks, vs
}

func (m *syncMarks) count() (n int) {
	for _, p := range m.rows {
		n += len(p.keys)
	}
	return n
}

// readPending lists, inside the source snapshot, the rows waiting to be synced.
func readPending(ctx context.Context, tx pgx.Tx, tables map[string]*schema.Table) (*syncMarks, error) {
	m := &syncMarks{rows: map[string]*pendingRows{}}
	for _, name := range slices.Sorted(maps.Keys(tables)) {
		t := tables[name]
		if t.SyncCol == nil {
			continue
		}
		rows, err := tx.Query(ctx, fmt.Sprintf("SELECT ROW(%s)::text, xmin::text FROM %s WHERE %s IS NULL",
			pg.IdentList(t.PK, ""), t.QName(), pg.Ident(t.SyncCol.Name)))
		if err != nil {
			return nil, fmt.Errorf("table %s: read waiting rows: %w", name, err)
		}
		p := &pendingRows{}
		for rows.Next() {
			var k, v string
			if err := rows.Scan(&k, &v); err != nil {
				rows.Close()
				return nil, err
			}
			p.keys = append(p.keys, k)
			p.versions = append(p.versions, v)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
		if len(p.keys) > 0 {
			m.rows[name] = p
		}
	}

	if err := tx.QueryRow(ctx, "SELECT to_regclass('public.sync_deleted_rows') IS NOT NULL").Scan(&m.deleteLog); err != nil {
		return nil, err
	}
	if m.deleteLog {
		if err := tx.QueryRow(ctx, "SELECT COALESCE(max(sdr_id), 0) FROM public.sync_deleted_rows WHERE sdr_sync_date IS NULL").Scan(&m.deletesUpTo); err != nil {
			return nil, fmt.Errorf("read sync_deleted_rows: %w", err)
		}
	}
	return m, nil
}

// validRows is a WHERE condition leaving out rows that break one of t's NOT
// VALID checks. Any UPDATE of such a row fails, a stamp included, so those rows
// stay waiting (on both sides) until they are corrected.
func validRows(t *schema.Table) string {
	var conds []string
	for _, c := range t.Constraints {
		if c.Type == "c" && strings.HasSuffix(c.Def, " NOT VALID") {
			expr := strings.TrimSuffix(strings.TrimPrefix(c.Def, "CHECK "), " NOT VALID")
			conds = append(conds, " AND "+expr+" IS NOT FALSE")
		}
	}
	return strings.Join(conds, "")
}

// stamp is the value written to a sync column; naive timestamp columns get the
// time in -naive-tz, like the change columns.
func (s *Syncer) stamp(c *schema.Column) string {
	if strings.Contains(c.Type, "without time zone") {
		return fmt.Sprintf("($1::timestamptz AT TIME ZONE %s)", pg.Literal(s.cfg.NaiveTZ))
	}
	return "$1::timestamptz"
}

// markTarget stamps the waiting rows on the target, in the sync transaction.
// After the data step the target matches the snapshot, so its rows with a NULL
// sync column are exactly the rows that were waiting on the source. On a
// -two-way table the push left the rows with a live change waiting alone:
// those are dropped from m, so that they stay waiting on the source too.
func (s *Syncer) markTarget(ctx context.Context, tx pgx.Tx, tables map[string]*schema.Table, m *syncMarks, snapshot, at time.Time) error {
	names := slices.Sorted(maps.Keys(m.rows))
	if len(names) == 0 {
		return nil
	}
	batch := &pgx.Batch{} // one round trip for every table
	for _, name := range names {
		t := tables[name]
		if t.TwoWay {
			batch.Queue(fmt.Sprintf("SELECT ROW(%s)::text FROM (%s) k", pg.IdentList(t.PK, "k."), s.liveKeysSQL(t, snapshot)))
		}
		col := pg.Ident(t.SyncCol.Name)
		batch.Queue(fmt.Sprintf("UPDATE %s SET %s = %s WHERE %s IS NULL%s", t.QName(), col, s.stamp(t.SyncCol), col, validRows(t)), at)
	}
	br := tx.SendBatch(ctx, batch)
	var n int64
	for _, name := range names {
		if tables[name].TwoWay {
			rows, err := br.Query()
			if err != nil {
				br.Close()
				return fmt.Errorf("table %s: read live changes: %w", name, err)
			}
			live, err := pgx.CollectRows(rows, pgx.RowTo[string])
			if err != nil {
				br.Close()
				return fmt.Errorf("table %s: read live changes: %w", name, err)
			}
			m.rows[name].drop(toSet(live))
		}
		tag, err := br.Exec()
		if err != nil {
			br.Close()
			return fmt.Errorf("table %s: set sync column on target: %w", name, err)
		}
		n += tag.RowsAffected()
	}
	if err := br.Close(); err != nil {
		return err
	}
	s.log.Info("sync time set on target", "rows", n, "tables", len(names))
	return nil
}

// markSource stamps the same rows on the source once the target has
// committed. A row edited after the snapshot has a new row version and stays
// waiting for the next run. Triggers are skipped when the source user may do
// so: the stamp is bookkeeping, not a change to log or audit.
func (s *Syncer) markSource(ctx context.Context, src *pgx.Conn, tables map[string]*schema.Table, m *syncMarks, at time.Time) error {
	var logged []string // tables whose logged deletes get the stamp
	if m.deleteLog && m.deletesUpTo > 0 && s.cfg.Deletes {
		for name, t := range tables {
			if s.cfg.Marked(t.Txn) {
				logged = append(logged, name)
			}
		}
	}
	if len(m.rows) == 0 && len(logged) == 0 {
		return nil // e.g. an interval run of -txn-tables with -mark-txn=false
	}

	tx, err := src.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())

	var super bool
	if err := tx.QueryRow(ctx, "SELECT rolsuper FROM pg_roles WHERE rolname = current_user").Scan(&super); err != nil {
		return err
	}
	if super {
		if _, err := tx.Exec(ctx, "SET LOCAL session_replication_role = replica"); err != nil {
			return err
		}
	}

	var marked, changed int64
	for _, name := range slices.Sorted(maps.Keys(m.rows)) {
		t, p := tables[name], m.rows[name]
		col := pg.Ident(t.SyncCol.Name)
		tag, err := tx.Exec(ctx, fmt.Sprintf(`UPDATE %s AS t SET %s = %s FROM unnest($2::text[], $3::text[]) AS p(_sync_key, _sync_version)
			WHERE t.%s IS NULL AND ROW(%s)::text = p._sync_key AND t.xmin::text = p._sync_version%s`,
			t.QName(), col, s.stamp(t.SyncCol), col, pg.IdentList(t.PK, "t."), validRows(t)), at, p.keys, p.versions)
		if err != nil {
			return fmt.Errorf("table %s: set sync column on source: %w", name, err)
		}
		marked += tag.RowsAffected()
		changed += int64(len(p.keys)) - tag.RowsAffected()
	}

	var deletes int64
	if len(logged) > 0 {
		// Every delete logged before the snapshot is on the target now: the run
		// removed all target rows the snapshot does not have.
		tag, err := tx.Exec(ctx, `UPDATE public.sync_deleted_rows SET sdr_sync_date = $1
			WHERE sdr_sync_date IS NULL AND sdr_id <= $2 AND sdr_schema || '.' || sdr_table = ANY($3::text[])`,
			at, m.deletesUpTo, logged)
		if err != nil {
			return fmt.Errorf("set sdr_sync_date: %w", err)
		}
		deletes = tag.RowsAffected()
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	s.log.Info("sync time set on source", "rows", marked, "deletes", deletes, "still_waiting", changed)
	return nil
}
