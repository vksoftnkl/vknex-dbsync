package schema

import (
	"regexp"
	"strings"

	"github.com/vksoftwares/vknex-dbsync/internal/config"
)

// Select applies include/exclude and picks the change-tracking column.
func Select(all map[string]*Table, cfg *config.Config) map[string]*Table {
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
		if cfg.MarkSynced && len(t.PK) > 0 {
			t.SyncCol = syncColumn(t)
		}
		out[key] = t
	}
	keepFKs(out)
	return out
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
