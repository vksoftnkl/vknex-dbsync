package schema

import (
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
		"accounts.acc_voucher_seq":     nil,
		"accounts.acc_voucher_details": {"accounts.acc_voucher_header"},
	}
	all := map[string]*Table{}
	for key, ps := range parents {
		ns, name, _ := strings.Cut(key, ".")
		t := &Table{Schema: ns, Name: name, PK: []string{"id"}, Columns: []Column{
			{Name: "id", Type: "integer"},
			{Name: "x_sync_date", Type: "timestamp with time zone"},
		}}
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

var kinds = map[string]string{
	"inventory.item_master":        "MASTER",
	"stock.stock_track_preset":     "SETTING",
	"sales.customers":              "MASTER",
	"sales.loyalty_member":         "MASTER",
	"sales.sale_bill":              "TRANSACTION",
	"accounts.acc_voucher_header":  "TRANSACTION",
	"accounts.acc_bill_balance":    "BALANCE",
	"stock.stock_ledger_part":      "LEDGER",
	"public.print_log_part":        "LOG",
	"accounts.acc_voucher_seq":     "SYSTEM",
	"accounts.acc_voucher_details": "TRANSACTION",
	// inventory.item_tax_master: inactive, so LoadKinds leaves it out
}

func TestSelectTableMaster(t *testing.T) {
	cfg := &config.Config{
		InstantKinds: []string{"MASTER", "SETTING", "METADATA", "LEDGER", "BALANCE", "SYSTEM"},
		TxnTables:    []string{"loyalty_member"}, // ignored: -table-master decides
		MarkSynced:   true,
	}
	got := Select(catalog(), cfg, kinds)

	instant := map[string]bool{
		"inventory.item_master":     true,
		"stock.stock_track_preset":  true,
		"sales.customers":           true,
		"sales.loyalty_member":      true,
		"accounts.acc_bill_balance": true, // its parent acc_voucher_header stays on the interval
		"stock.stock_ledger_part":   true, // partitioned tables can be instant
		"accounts.acc_voucher_seq":  true,
	}
	for key, tbl := range got {
		if tbl.Txn == instant[key] {
			t.Errorf("%s (%s): Txn = %v, want %v", key, tbl.Kind, tbl.Txn, !instant[key])
		}
		if tbl.SyncCol == nil { // -mark-txn=false: only the instant tables are stamped
			if instant[key] {
				t.Errorf("%s: instant table has no sync column", key)
			}
		} else if !instant[key] {
			t.Errorf("%s: interval table has a sync column with -mark-txn=false", key)
		}
		if tbl.Kind != kinds[key] {
			t.Errorf("%s: Kind = %q, want %q", key, tbl.Kind, kinds[key])
		}
	}
}

func TestSelectTxnTablesOnly(t *testing.T) {
	cfg := &config.Config{TxnTables: []string{"sales.sale_bill", "loyalty_member"}}
	for key, tbl := range Select(catalog(), cfg, nil) {
		want := key == "sales.sale_bill" || key == "sales.loyalty_member"
		if tbl.Txn != want || tbl.Kind != "" {
			t.Errorf("%s: Txn = %v, Kind = %q; want %v, empty", key, tbl.Txn, tbl.Kind, want)
		}
	}
}
