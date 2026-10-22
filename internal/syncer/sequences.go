package syncer

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/vksoftwares/vknex-dbsync/internal/pg"
	"github.com/vksoftwares/vknex-dbsync/internal/schema"
)

type seqKey struct{ table, col string }

func readSourceSequences(ctx context.Context, tx pgx.Tx, tables map[string]*schema.Table) (map[seqKey]int64, error) {
	out := map[seqKey]int64{}
	for name, t := range tables {
		for _, col := range t.SeqCols {
			var seq string
			if err := tx.QueryRow(ctx, "SELECT pg_get_serial_sequence(format('%I.%I', $1::text, $2::text), $3)", t.Schema, t.Name, col).Scan(&seq); err != nil {
				return nil, fmt.Errorf("sequence of %s.%s: %w", name, col, err)
			}
			var used int64
			if err := tx.QueryRow(ctx, "SELECT CASE WHEN is_called THEN last_value ELSE last_value - 1 END FROM "+seq).Scan(&used); err != nil {
				return nil, fmt.Errorf("read %s: %w", seq, err)
			}
			out[seqKey{name, col}] = used
		}
	}
	return out, nil
}

// setSequences moves each target sequence past both the source sequence and
// the highest id on the target, so new rows on either side never collide.
func setSequences(ctx context.Context, tx pgx.Tx, tables map[string]*schema.Table, srcSeq map[seqKey]int64) error {
	for name, t := range tables {
		for _, col := range t.SeqCols {
			var seq *string
			if err := tx.QueryRow(ctx, "SELECT pg_get_serial_sequence(format('%I.%I', $1::text, $2::text), $3)", t.Schema, t.Name, col).Scan(&seq); err != nil {
				return err
			}
			if seq == nil {
				continue
			}
			q := fmt.Sprintf(`SELECT CASE WHEN m >= 1 THEN setval($1::regclass, m, true) ELSE setval($1::regclass, 1, false) END
				FROM (SELECT GREATEST($2::bigint, COALESCE(max(%s), 0)::bigint) AS m FROM %s) x`, pg.Ident(col), t.QName())
			if _, err := tx.Exec(ctx, q, *seq, srcSeq[seqKey{name, col}]); err != nil {
				return fmt.Errorf("set sequence %s: %w", *seq, err)
			}
		}
	}
	return nil
}
