package syncer

import (
	"strings"
	"testing"
	"time"

	"github.com/vksoftwares/vknex-dbsync/internal/config"
	"github.com/vksoftwares/vknex-dbsync/internal/schema"
)

func twoWayTable() *schema.Table {
	t := &schema.Table{Schema: "sales", Name: "customers", PK: []string{"cus_id"}, TwoWay: true, Columns: []schema.Column{
		{Name: "cus_id", Type: "integer"},
		{Name: "cus_name", Type: "text"},
		{Name: "cus_sync_date", Type: "timestamp with time zone"},
	}}
	t.SyncCol = &t.Columns[2]
	return t
}

func TestUpsertSQLSkip(t *testing.T) {
	tbl := twoWayTable()
	if q := upsertSQL(tbl, "_s", ""); strings.Contains(q, " WHERE NOT ") {
		t.Errorf("no skip, but: %s", q)
	}
	q := upsertSQL(tbl, "_s", "EXISTS (x)")
	if want := `SELECT s."cus_id", s."cus_name", s."cus_sync_date" FROM _s s WHERE NOT EXISTS (x) ON CONFLICT ("cus_id")`; !strings.Contains(q, want) {
		t.Errorf("got  %s\nwant %s", q, want)
	}
}

func TestPullUpsertSQL(t *testing.T) {
	tbl := twoWayTable()
	q := pullUpsertSQL(tbl, "_s", "$1::timestamptz", `d."cus_sync_date" IS NOT NULL`)
	for _, want := range []string{
		`SELECT s."cus_id", s."cus_name", $1::timestamptz FROM _s s ON CONFLICT`,
		`DO UPDATE SET "cus_name" = EXCLUDED."cus_name", "cus_sync_date" = EXCLUDED."cus_sync_date"`,
		// a row that differs in nothing but the sync column is left alone
		`WHERE ROW(d."cus_name") IS DISTINCT FROM ROW(EXCLUDED."cus_name") AND d."cus_sync_date" IS NOT NULL`,
	} {
		if !strings.Contains(q, want) {
			t.Errorf("missing %s\nin %s", want, q)
		}
	}
	// Only the key and the sync column: nothing to update.
	tbl.Columns = []schema.Column{tbl.Columns[0], tbl.Columns[2]}
	tbl.SyncCol = &tbl.Columns[1]
	if q := pullUpsertSQL(tbl, "_s", "$1::timestamptz", ""); !strings.HasSuffix(q, "DO NOTHING") {
		t.Errorf("want DO NOTHING: %s", q)
	}
}

func TestLiveWaiting(t *testing.T) {
	s := New(&config.Config{MetaSchema: "vknex_sync"}, nil, nil)
	tbl := twoWayTable()
	at := time.Date(2026, 10, 24, 14, 0, 0, 123456000, time.FixedZone("IST", 19800))
	want := `EXISTS (SELECT 1 FROM "vknex_sync"."live_changes" c WHERE c.schema_name = 'sales' AND c.table_name = 'customers' AND c.pk = jsonb_build_object('cus_id', d."cus_id")` +
		` AND (c.pulled_at IS NULL OR c.pulled_at >= '2026-10-24 08:30:00.123456+00'::timestamptz))`
	if got := s.liveWaiting(tbl, "d", at); got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
	if got := waitingSQL(time.Time{}); got != "c.pulled_at IS NULL" {
		t.Errorf("not pulled yet: %s", got)
	}
	tbl.TwoWay = false
	if got := s.liveWaiting(tbl, "d", at); got != "" || andNot(got) != "" {
		t.Errorf("one-way table: %q", got)
	}
}
