package syncer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/vksoftwares/vknex-dbsync/internal/pg"
	"github.com/vksoftwares/vknex-dbsync/internal/schema"
)

// full truncates every synced table on the target and reloads it.
func (s *Syncer) full(ctx context.Context, src *pgx.Conn, dst pgx.Tx, tables map[string]*schema.Table, order []string, st *runStats) error {
	srcPg, dstPg := src.PgConn(), dst.Conn().PgConn()

	names := make([]string, len(order))
	for i, n := range order {
		names[i] = tables[n].QName()
	}
	if _, err := dst.Exec(ctx, "TRUNCATE "+strings.Join(names, ", ")); err != nil {
		return fmt.Errorf("truncate target tables: %w", hint(err))
	}

	for _, name := range order {
		t := tables[name]
		t0 := time.Now()
		if _, err := s.relaxNotValid(ctx, dst, t, "", st); err != nil {
			return fmt.Errorf("table %s: %w", name, err)
		}
		n, err := pg.CopyStream(ctx, srcPg, dstPg, selectSQL(t, selectExprs(t, true), ""), t.QName(), t.ColNames(), t.TextCopy)
		if err != nil {
			return fmt.Errorf("table %s: %w", name, hint(err))
		}
		st.Copied += n
		st.Upserted += n
		st.Tables++
		s.log.Info("table loaded", "table", name, "rows", n, "took", time.Since(t0).Round(time.Millisecond).String())
	}

	// Second pass for FK-cycle columns that were loaded as NULL.
	for i, name := range order {
		t := tables[name]
		if len(t.Break) == 0 {
			continue
		}
		stage := fmt.Sprintf("_sync_fix_%d", i)
		cols := append(append([]string{}, t.PK...), t.Break...)
		if _, err := s.stage(ctx, srcPg, dst, t, stage, cols, ""); err != nil {
			return err
		}
		if _, err := dst.Exec(ctx, fixBreakSQL(t, stage)); err != nil {
			return fmt.Errorf("table %s: fill cycle columns: %w", name, hint(err))
		}
	}

	for _, name := range order {
		if _, err := dst.Exec(ctx, "ANALYZE "+tables[name].QName()); err != nil {
			return fmt.Errorf("analyze %s: %w", name, err)
		}
	}
	return nil
}
