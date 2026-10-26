package schema

import (
	"slices"
	"strings"
	"testing"

	"github.com/vksoftwares/vknex-dbsync/internal/config"
)

// catalog returns fresh tables (Select changes them): key -> FK parents.
func catalog() map[string]*Table {
	parents := map[string][]string{
		"inventory.item_master":        {"stock.stock_track_preset"},
		"stock.stock_track_preset":     nil,
		"sales.customers":              nil,
		"sales.loyalty_member":         {"sales.customers"},
		"sales.sale_bill":              {"sales.customers"},
		"accounts.acc_voucher_header":  nil,
		"accounts.acc_bill_balance":    {"accounts.acc_voucher_header"}, // instant -> interval
		"stock.stock_ledger_part":      nil,
		"public.print_log_part":        nil,
		"inventory.item_tax_master":    nil,
		"public.not_in_table_master":   nil,
		"public.no_stamp_col":          nil,
		"accounts.acc_voucher_seq":     nil,
		"accounts.acc_voucher_details": {"accounts.acc_voucher_header"},
	}
	all := map[string]*Table{}
	for key, ps := range parents {
		ns, name, _ := strings.Cut(key, ".")
		t := &Table{Schema: ns, Name: name, PK: []string{"id"}, Columns: []Column{
			{Name: "id", Type: "integer"},
			{Name: "x_sync_date", Type: "timestamp with time zone"}, // what the name rule picks
			{Name: "other_stamp", Type: "timestamp with time zone"}, // only dtm_sync_col can pick this one
			{Name: "note", Type: "text"},
		}}
		if name == "no_stamp_col" {
			t.Columns = t.Columns[:1] // nothing the name rule could pick
		}
		for _, p := range ps {
			t.FKs = append(t.FKs, FK{Name: "fk_" + name, Parent: p})
		}
		if strings.HasSuffix(name, "_part") {
			t.PartKey = "RANGE (id)"
		}
		all[key] = t
	}
	return all
}

var master = map[string]MasterRow{
	"inventory.item_master":        {Kind: "MASTER", SyncCol: "other_stamp"}, // dtm_sync_col wins over the name rule
	"stock.stock_track_preset":     {Kind: "SETTING", SyncCol: "x_sync_date"},
	"sales.customers":              {Kind: "MASTER"},                         // no dtm_sync_col: the name rule
	"sales.loyalty_member":         {Kind: "MASTER", SyncCol: "missing_col"}, // not a column: warning, name rule
	"sales.sale_bill":              {Kind: "TRANSACTION", SyncCol: "x_sync_date"},
	"accounts.acc_voucher_header":  {Kind: "TRANSACTION", SyncCol: "x_sync_date"},
	"accounts.acc_bill_balance":    {Kind: "BALANCE", SyncCol: "x_sync_date"},
	"stock.stock_ledger_part":      {Kind: "LEDGER", SyncCol: "x_sync_date"},
	"public.print_log_part":        {Kind: "LOG", SyncCol: "x_sync_date"},
	"public.no_stamp_col":          {Kind: "MASTER", SyncCol: "id"},   // not a timestamp, nothing to fall back on
	"accounts.acc_voucher_seq":     {Kind: "SYSTEM", SyncCol: "note"}, // not a timestamp: warning, name rule
	"accounts.acc_voucher_details": {Kind: "TRANSACTION", SyncCol: "other_stamp"},
	// inventory.item_tax_master: inactive, so LoadMaster leaves it out
}

func syncColName(t *Table) string {
	if t == nil || t.SyncCol == nil {
		return ""
	}
	return t.SyncCol.Name
}

func TestSelectTableMaster(t *testing.T) {
	cfg := &config.Config{
		InstantKinds: []string{"MASTER", "SETTING", "METADATA", "LEDGER", "BALANCE", "SYSTEM"},
		TxnTables:    []string{"loyalty_member"}, // ignored: -table-master decides
		MarkSynced:   true,
	}
	got, warnings := Select(catalog(), cfg, master)

	// The instant tables, each with the column -mark-txn=false stamps on it.
	wantSync := map[string]string{
		"inventory.item_master":     "other_stamp", // dtm_sync_col, not the name rule's x_sync_date
		"stock.stock_track_preset":  "x_sync_date",
		"sales.customers":           "x_sync_date", // empty dtm_sync_col: the name rule
		"sales.loyalty_member":      "x_sync_date", // dtm_sync_col is not a column: the name rule
		"accounts.acc_bill_balance": "x_sync_date", // its parent acc_voucher_header stays on the interval
		"stock.stock_ledger_part":   "x_sync_date", // partitioned tables can be instant
		"accounts.acc_voucher_seq":  "x_sync_date", // dtm_sync_col is not a timestamp: the name rule
		"public.no_stamp_col":       "",            // dtm_sync_col is wrong and no column fits the name rule
	}
	for key, tbl := range got {
		want, instant := wantSync[key]
		if tbl.Txn == instant {
			t.Errorf("%s (%s): Txn = %v, want %v", key, tbl.Kind, tbl.Txn, !instant)
		}
		if name := syncColName(tbl); name != want { // interval tables: none with -mark-txn=false
			t.Errorf("%s: sync column %q, want %q", key, name, want)
		}
		if tbl.Kind != master[key].Kind {
			t.Errorf("%s: Kind = %q, want %q", key, tbl.Kind, master[key].Kind)
		}
	}
	wantWarn := []string{
		`accounts.acc_voucher_seq: dtm_sync_col "note" is not a timestamp column of the table; using x_sync_date instead`,
		`public.no_stamp_col: dtm_sync_col "id" is not a timestamp column of the table; its rows are not stamped`,
		`sales.loyalty_member: dtm_sync_col "missing_col" is not a timestamp column of the table; using x_sync_date instead`,
	}
	if !slices.Equal(warnings, wantWarn) {
		t.Errorf("warnings =\n  %s\nwant\n  %s", strings.Join(warnings, "\n  "), strings.Join(wantWarn, "\n  "))
	}
}

// With -mark-txn the interval tables are stamped too, by the same rules.
func TestSelectMarkTxn(t *testing.T) {
	cfg := &config.Config{InstantKinds: []string{"MASTER"}, MarkSynced: true, MarkTxn: true}
	got, warnings := Select(catalog(), cfg, master)
	for key, want := range map[string]string{
		"accounts.acc_voucher_details": "other_stamp", // dtm_sync_col
		"sales.sale_bill":              "x_sync_date",
		"inventory.item_tax_master":    "x_sync_date", // not listed: the name rule
		"public.not_in_table_master":   "x_sync_date",
	} {
		if name := syncColName(got[key]); name != want {
			t.Errorf("%s: sync column %q, want %q", key, name, want)
		}
	}
	if len(warnings) != 3 {
		t.Errorf("warnings = %q, want the three bad dtm_sync_col rows", warnings)
	}
}

func TestSelectTxnTablesOnly(t *testing.T) {
	cfg := &config.Config{TxnTables: []string{"sales.sale_bill", "loyalty_member"}}
	got, _ := Select(catalog(), cfg, nil)
	for key, tbl := range got {
		want := key == "sales.sale_bill" || key == "sales.loyalty_member"
		if tbl.Txn != want || tbl.Kind != "" {
			t.Errorf("%s: Txn = %v, Kind = %q; want %v, empty", key, tbl.Txn, tbl.Kind, want)
		}
	}
}

// With -two-way, the -two-way-kinds tables that get a sync column are two-way.
func TestSelectTwoWay(t *testing.T) {
	cfg := &config.Config{
		InstantKinds: []string{"MASTER", "SETTING", "BALANCE", "LEDGER", "SYSTEM"},
		MarkSynced:   true,
		TwoWay:       true,
		TwoWayKinds:  []string{"MASTER"},
	}
	all := catalog()
	all["sales.customers"].PK = nil // nothing to match rows by
	got, warnings := Select(all, cfg, master)
	want := map[string]bool{
		"inventory.item_master": true,
		"sales.loyalty_member":  true, // the name rule found its sync column
	}
	for key, tbl := range got {
		if tbl.TwoWay != want[key] {
			t.Errorf("%s (%s): TwoWay = %v, want %v", key, tbl.Kind, tbl.TwoWay, want[key])
		}
	}
	for _, w := range []string{
		"public.no_stamp_col: MASTER table without a sync column; synced one way only",
		"sales.customers: MASTER table without a primary key; synced one way only",
	} {
		if !slices.Contains(warnings, w) {
			t.Errorf("missing warning %q in\n  %s", w, strings.Join(warnings, "\n  "))
		}
	}

	cfg.TwoWay = false
	got, _ = Select(catalog(), cfg, master)
	for key, tbl := range got {
		if tbl.TwoWay {
			t.Errorf("%s: TwoWay without -two-way", key)
		}
	}
}
