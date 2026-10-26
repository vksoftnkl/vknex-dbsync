package schema

import (
	"context"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"

	"github.com/vksoftwares/vknex-dbsync/internal/config"
	"github.com/vksoftwares/vknex-dbsync/internal/pg"
)

// MasterRow is one active row of -table-master (e.g. public.db_table_master):
// what it says about a table.
type MasterRow struct {
	Kind    string // dtm_kind, upper case
	SyncCol string // dtm_sync_col: the column holding the time a row last reached the live DB; "" when empty
}

// Select applies include/exclude, picks the change-tracking column and sorts
// the tables into instant and interval ones. master comes from LoadMaster.
// With -mark-synced every stamped table (config.Marked) gets its sync column:
// the one dtm_sync_col names, else the one its name gives away (syncColumn).
// The warnings name the tables whose dtm_sync_col is not a timestamp column
// of theirs; those use the name rule too. With -two-way, the tables of a
// -two-way-kinds kind are TwoWay when they have a sync column (and so a
// primary key); the warnings name the ones that do not.
func Select(all map[string]*Table, cfg *config.Config, master map[string]MasterRow) (map[string]*Table, []string) {
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
	classify(out, cfg, master)
	var warnings []string
	for _, key := range slices.Sorted(maps.Keys(out)) {
		t := out[key]
		if !cfg.Marked(t.Txn) || len(t.PK) == 0 {
			continue
		}
		t.SyncCol = syncColumn(t)
		name := master[key].SyncCol
		if name == "" {
			continue
		}
		if c := t.Column(name); c != nil && strings.HasPrefix(c.Type, "timestamp") {
			t.SyncCol = c
			continue
		}
		instead := "its rows are not stamped"
		if t.SyncCol != nil {
			instead = "using " + t.SyncCol.Name + " instead"
		}
		warnings = append(warnings, fmt.Sprintf("%s: dtm_sync_col %q is not a timestamp column of the table; %s", key, name, instead))
	}
	for _, key := range slices.Sorted(maps.Keys(out)) {
		t := out[key]
		if !cfg.TwoWay || !slices.Contains(cfg.TwoWayKinds, t.Kind) {
			continue
		}
		switch {
		case len(t.PK) == 0:
			warnings = append(warnings, fmt.Sprintf("%s: %s table without a primary key; synced one way only", key, t.Kind))
		case t.SyncCol == nil:
			warnings = append(warnings, fmt.Sprintf("%s: %s table without a sync column; synced one way only", key, t.Kind))
		default:
			t.TwoWay = true
		}
	}
	return out, warnings
}

// LoadMaster reads the active rows of -table-master (e.g. public.db_table_master)
// by table key: dtm_kind and dtm_sync_col. Without -table-master it returns nil.
func LoadMaster(ctx context.Context, q pg.Querier, cfg *config.Config) (map[string]MasterRow, error) {
	if cfg.TableMaster == "" {
		return nil, nil
	}
	rows, err := q.Query(ctx, "SELECT dtm_schema::text, dtm_table::text, upper(dtm_kind::text), coalesce(dtm_sync_col::text, '') FROM "+
		pg.QName(cfg.TableMasterName())+" WHERE dtm_is_active")
	if err != nil {
		return nil, fmt.Errorf("read -table-master %s (an empty SYNC_TABLE_MASTER picks the instant tables with -txn-tables instead): %w", cfg.TableMaster, err)
	}
	master := map[string]MasterRow{}
	for rows.Next() {
		var ns, tbl string
		var row MasterRow
		if err := rows.Scan(&ns, &tbl, &row.Kind, &row.SyncCol); err != nil {
			rows.Close()
			return nil, err
		}
		row.SyncCol = strings.TrimSpace(row.SyncCol)
		master[Key(ns, tbl)] = row
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read -table-master %s: %w", cfg.TableMaster, err)
	}
	return master, nil
}

// classify sets Kind and Txn. Without -table-master the -txn-tables are the
// interval tables, as before. With it, -table-master alone decides (it is
// edited as tables come and go; -txn-tables is ignored): -instant mirrors the
// tables of an -instant-kinds kind; the other kinds, and tables it does not
// list (or lists as inactive), follow -interval or their -kind-intervals entry.
func classify(tables map[string]*Table, cfg *config.Config, master map[string]MasterRow) {
	for key, t := range tables {
		if master == nil {
			t.Kind, t.Txn = "", cfg.TxnListed(t.Schema, t.Name)
		} else {
			t.Kind = master[key].Kind
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

// syncColumn picks a table's sync column the way v_sync_tables does, for
// tables -table-master does not name one for: a timestamp column whose name
// ends in sync_date, else sync_on, synced_on or syncdate, first in column order.
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
