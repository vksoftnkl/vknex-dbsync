package syncer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/vksoftwares/vknex-dbsync/internal/schema"
)

// plan prints what a run would do, without touching the target.
func (s *Syncer) plan(ctx context.Context, srcTx pgx.Tx, dst *pgx.Conn, tables map[string]*schema.Table, order []string, reset map[string]bool) error {
	state := map[string]time.Time{}
	if s.cfg.Mode == "incremental" {
		var exists bool
		_ = dst.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", s.cfg.MetaSchema+".table_state").Scan(&exists)
		if exists {
			var err error
			if state, err = s.loadState(ctx, dst); err != nil {
				return err
			}
			for name := range reset {
				delete(state, name)
			}
		}
	}

	s.log.Info("DRY RUN: nothing will be written", "mode", s.cfg.Mode, "tables", len(order))
	var total int64
	for i, name := range order {
		t := tables[name]
		how, where := "full table", ""
		switch {
		case s.cfg.Mode == "full":
			how = "truncate + reload"
		case len(t.PK) == 0:
			how = "replace (no primary key)"
		case t.UpdatedCol == nil:
			how = "compare whole table (no updatedAt column)"
		default:
			if since, ok := state[name]; ok {
				where = s.changedSince(t.UpdatedCol, since.Add(-s.cfg.Overlap))
				how = "changed since " + since.Add(-s.cfg.Overlap).Format(time.RFC3339)
			} else {
				how = "first run: all rows"
			}
		}
		var n int64
		q := "SELECT count(*) FROM " + t.QName()
		if where != "" {
			q += " WHERE " + where
		}
		if err := srcTx.QueryRow(ctx, q).Scan(&n); err != nil {
			return fmt.Errorf("count %s: %w", name, err)
		}
		total += n
		if len(t.Break) > 0 {
			how += "; cycle columns " + strings.Join(t.Break, ",") + " filled in a second pass"
		}
		s.log.Info("plan", "step", i+1, "table", name, "rows", n, "how", how, "copy_format", map[bool]string{true: "text", false: "binary"}[t.TextCopy])
	}
	s.log.Info("DRY RUN total", "rows_to_read", total, "deletes", s.cfg.Deletes && s.cfg.Mode == "incremental")
	return nil
}
