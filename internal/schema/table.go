// Package schema models the tables of a database and reads them from the catalog.
package schema

import (
	"github.com/vksoftwares/vknex-dbsync/internal/pg"
)

// Column is one column of a table.
type Column struct {
	Name      string
	Type      string // format_type(), e.g. "timestamp(3) without time zone"
	Identity  string // "a" (ALWAYS), "d" (BY DEFAULT) or ""
	Generated string // "s" (STORED), "v" (VIRTUAL) or ""; Default then holds the expression
	NotNull   bool
	Default   string // DEFAULT expression, "" if none
	NeedText  bool   // array of user type / composite: binary COPY is not portable
	NoEq      bool   // type without "=" operator (json, xml, geometric)
}

// FK is a foreign key to another table.
type FK struct {
	Name     string
	Parent   string
	Cols     []string
	Nullable bool // every FK column is nullable, so the edge can be "broken"
}

// UniqueKey is a UNIQUE index other than the primary key, or an EXCLUDE
// constraint: two rows conflict when every key part matches under its operator.
type UniqueKey struct {
	Cols          []string // column names; nil when a key part is an expression
	Exprs         []string // key parts as printed by pg_get_indexdef (quoted names or expressions)
	Ops           []string // EXCLUDE operators, e.g. OPERATOR(pg_catalog.&&); nil for UNIQUE
	Pred          string   // WHERE clause of a partial index, "" if none
	NullsDistinct bool     // false for NULLS NOT DISTINCT
}

// Plain reports whether rows can be matched on equal key columns alone.
func (u UniqueKey) Plain() bool {
	return u.Cols != nil && u.Ops == nil && u.Pred == "" && u.NullsDistinct
}

// Constraint is a PRIMARY KEY, UNIQUE, CHECK, FOREIGN KEY or EXCLUDE constraint.
type Constraint struct {
	Name string
	Type string // pg_constraint.contype: p, u, c, f or x
	Def  string // pg_get_constraintdef()
}

// Index is a standalone index, i.e. not one that backs a constraint.
type Index struct {
	Name string
	Def  string // pg_get_indexdef()
}

// Partition is one partition below a partitioned table, at any depth.
type Partition struct {
	Key     string // schema.name, unquoted (see Key)
	Name    string // quoted schema.name
	Parent  string // quoted schema.name of the direct parent
	Bound   string // FOR VALUES ... or DEFAULT
	PartKey string // set when the partition is itself partitioned
}

// Table is an ordinary or partitioned table, never a partition.
type Table struct {
	Schema      string
	Name        string
	All         []Column // every column in table order, generated ones included
	Columns     []Column // copyable columns (generated columns excluded)
	PK          []string
	FKs         []FK        // FKs whose parent is inside the synced set
	Uniques     []UniqueKey // UNIQUE indexes other than the PK
	SeqCols     []string    // columns backed by an owned sequence / identity
	Break       []string    // FK columns written as NULL first, filled in later (cycles)
	UpdatedCol  *Column
	SyncCol     *Column // set with -mark-synced: the column holding the last sync time
	Kind        string  // dtm_kind in -table-master; "" when it does not list the table (or no -table-master)
	Txn         bool    // -instant leaves the table to the -interval runs (see classify)
	TextCopy    bool
	PartKey     string // PARTITION BY clause of a partitioned table
	Partitions  []Partition
	Constraints []Constraint
	Indexes     []Index
}

// Key identifies a table across schemas, e.g. "accounts.ledger".
func Key(schema, name string) string { return schema + "." + name }

// QName is the quoted schema.name.
func (t *Table) QName() string { return pg.QName(t.Schema, t.Name) }

// IsBreak reports whether col is an FK-cycle column (see Break).
func (t *Table) IsBreak(col string) bool {
	for _, b := range t.Break {
		if b == col {
			return true
		}
	}
	return false
}

// ColNames lists the copyable columns.
func (t *Table) ColNames() []string {
	out := make([]string, len(t.Columns))
	for i, c := range t.Columns {
		out[i] = c.Name
	}
	return out
}

// HasAlwaysIdentity reports whether a column is GENERATED ALWAYS AS IDENTITY.
func (t *Table) HasAlwaysIdentity() bool {
	for _, c := range t.Columns {
		if c.Identity == "a" {
			return true
		}
	}
	return false
}

// Column finds a copyable column, or returns nil.
func (t *Table) Column(name string) *Column {
	for i := range t.Columns {
		if t.Columns[i].Name == name {
			return &t.Columns[i]
		}
	}
	return nil
}

// AnyColumn also finds generated columns.
func (t *Table) AnyColumn(name string) *Column {
	for i := range t.All {
		if t.All[i].Name == name {
			return &t.All[i]
		}
	}
	return nil
}
