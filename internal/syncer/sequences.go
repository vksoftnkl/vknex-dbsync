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

// setSequences moves each sequence of tx's database past both the other
// side's sequence (other) and the highest id there, so new rows on either side
// never collide. With -two-way-live-ids the ids of a two-way table are kept
// apart instead: on the target (onTarget) its sequences hand out ids from
// there on, and on the source the ids from there on are the target's.
func (s *Syncer) setSequences(ctx context.Context, tx pgx.Tx, tables map[string]*schema.Table, other map[seqKey]int64, onTarget bool) error {
	batch := &pgx.Batch{} // one round trip for every sequence
	cols := s.queueSequences(batch, tables, other, onTarget)
	if len(cols) == 0 {
		return nil
	}
	br := tx.SendBatch(ctx, batch)
	if err := sequencesDone(br, cols); err != nil {
		br.Close()
		return err
	}
	return br.Close()
}

// queueSequences queues the statements of setSequences on batch and returns
// the column each one is for, to read them back with sequencesDone.
func (s *Syncer) queueSequences(batch *pgx.Batch, tables map[string]*schema.Table, other map[seqKey]int64, onTarget bool) []string {
	var cols []string
	for name, t := range tables {
		for _, col := range t.SeqCols {
			top, floor := "max("+pg.Ident(col)+")", ""
			if live := s.cfg.TwoWayLiveIDs; live > 0 && t.TwoWay {
				if onTarget {
					floor = fmt.Sprintf(", %d", live-1)
				} else {
					top = fmt.Sprintf("max(%s) FILTER (WHERE %s < %d)", pg.Ident(col), pg.Ident(col), live)
				}
			}
			// No row when the column has no sequence.
			batch.Queue(fmt.Sprintf(`SELECT CASE WHEN m >= 1 THEN setval(seq, m, true) ELSE setval(seq, 1, false) END
				FROM (SELECT pg_get_serial_sequence(format('%%I.%%I', $1::text, $2::text), $3)::regclass AS seq,
				             GREATEST($4::bigint, COALESCE(%s, 0)::bigint%s) AS m FROM %s) x
				WHERE seq IS NOT NULL`, top, floor, t.QName()), t.Schema, t.Name, col, other[seqKey{name, col}])
			cols = append(cols, name+"."+col)
		}
	}
	return cols
}

// sequencesDone reads the results of queueSequences from br, leaving it open.
func sequencesDone(br pgx.BatchResults, cols []string) error {
	for _, col := range cols {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("set sequence of %s: %w", col, err)
		}
	}
	return nil
}
