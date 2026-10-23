package schema

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/vksoftwares/vknex-dbsync/internal/config"
	"github.com/vksoftwares/vknex-dbsync/internal/pg"
)

// Select applies include/exclude, picks the change-tracking column and sorts
// the tables into instant and interval ones. kinds comes from LoadKinds.
func Select(all map[string]*Table, cfg *config.Config, kinds map[string]string) map[string]*Table {
	out := map[string]*Table{}
	for key, t := range all {
		if !cfg.Wanted(t.Schema, t.Name) {
			continue
		}
		for _, cand := range cfg.UpdatedCols {
			if c := t.Column(cand); c != nil && strings.HasPrefix(c.Type, "timestamp") {
				t.UpdatedCol = c
				break
			}
		}
		out[key] = t
	}
	keepFKs(out)
	classify(out, cfg, kinds)
	for _, t := range out {
		if cfg.Marked(t.Txn) && len(t.PK) > 0 {
			t.SyncCol = syncColumn(t)
		}
	}
	return out
}

// LoadKinds reads the active rows of -table-master (e.g. public.db_table_master)
// as table key -> dtm_kind. Without -table-master it returns nil.
func LoadKinds(ctx context.Context, q pg.Querier, cfg *config.Config) (map[string]string, error) {
	if cfg.TableMaster == "" {
		return nil, nil
	}
	rows, err := q.Query(ctx, "SELECT dtm_schema::text, dtm_table::text, upper(dtm_kind::text) FROM "+
		pg.QName(cfg.TableMasterName())+" WHERE dtm_is_active")
	if err != nil {
		return nil, fmt.Errorf("read -table-master %s (an empty SYNC_TABLE_MASTER picks the instant tables with -txn-tables instead): %w", cfg.TableMaster, err)
	}
	kinds := map[string]string{}
	for rows.Next() {
		var ns, tbl, kind string
		if err := rows.Scan(&ns, &tbl, &kind); err != nil {
			rows.Close()
			return nil, err
		}
		kinds[Key(ns, tbl)] = kind
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read -table-master %s: %w", cfg.TableMaster, err)
	}
	return kinds, nil
}

// classify sets Kind and Txn. Without -table-master the -txn-tables are the
// interval tables, as before. With it, -table-master alone decides (it is
// edited as tables come and go; -txn-tables is ignored): -instant mirrors the
// tables of an -instant-kinds kind; the other kinds, and tables it does not
// list (or lists as inactive), follow -interval or their -kind-intervals entry.
func classify(tables map[string]*Table, cfg *config.Config, kinds map[string]string) {
	for key, t := range tables {
		if kinds == nil {
			t.Kind, t.Txn = "", cfg.TxnListed(t.Schema, t.Name)
		} else {
			t.Kind = kinds[key]
			t.Txn = !slices.Contains(cfg.InstantKinds, t.Kind)
		}
	}
}

// Subset returns the tables keep accepts, for a run that moves only those.
func Subset(tables map[string]*Table, keep func(*Table) bool) map[string]*Table {
	out := map[string]*Table{}
	for key, t := range tables {
		if keep(t) {
			out[key] = t
		}
	}
	keepFKs(out)
	return out
}

// keepFKs keeps only the FK edges inside the set: FKOrder orders by those alone.
func keepFKs(tables map[string]*Table) {
	for _, t := range tables {
		var fks []FK
		for _, fk := range t.FKs {
			if _, ok := tables[fk.Parent]; ok {
				fks = append(fks, fk)
			}
		}
		t.FKs = fks
		t.Break = nil
	}
}

// syncColRe is the rule of the source's public.v_sync_tables view.
var syncColRe = regexp.MustCompile(`(?i)(sync_date|sync_on|synced_on|syncdate)$`)

// syncColumn picks a table's sync column the way v_sync_tables does: a
// timestamp column whose name ends in sync_date, else sync_on, synced_on or
// syncdate, first in column order.
func syncColumn(t *Table) *Column {
	var pick *Column
	for i := range t.Columns {
		c := &t.Columns[i]
		if !strings.HasPrefix(c.Type, "timestamp") || !syncColRe.MatchString(c.Name) {
			continue
		}
		if strings.HasSuffix(strings.ToLower(c.Name), "sync_date") {
			return c
		}
		if pick == nil {
			pick = c
		}
	}
	return pick
}
