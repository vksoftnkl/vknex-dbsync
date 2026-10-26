package syncer

import (
	"maps"
	"slices"

	"github.com/vksoftwares/vknex-dbsync/internal/schema"
	"github.com/vksoftwares/vknex-dbsync/internal/status"
)

// reportTables tells the status page which tables are synced and how.
func (s *Syncer) reportTables(tables map[string]*schema.Table) {
	if s.Status == nil {
		return
	}
	out := make([]status.Table, 0, len(tables))
	for _, name := range slices.Sorted(maps.Keys(tables)) {
		t := tables[name]
		info := status.Table{
			Key:        name,
			Schema:     t.Schema,
			Name:       t.Name,
			Kind:       t.Kind,
			Schedule:   s.schedule(t),
			Instant:    s.cfg.Instant && !t.Txn,
			HasPK:      len(t.PK) > 0,
			TwoWay:     t.TwoWay,
			Partitions: len(t.Partitions),
		}
		if t.UpdatedCol != nil {
			info.UpdatedCol = t.UpdatedCol.Name
		}
		if t.SyncCol != nil {
			info.SyncCol = t.SyncCol.Name
		}
		out = append(out, info)
	}
	s.Status.SetTables(out)
}
