// Package ddl plans the schema changes of -ddl: the target's structure is
// brought in line with the source before any data moves: schemas, enum types, functions, tables, partitions, columns
// (type, default, NOT NULL), constraints and indexes. With -ddl-drop, tables,
// partitions and columns the source no longer has are dropped as well.
// Views, triggers and grants are not touched, and functions are never dropped.
package ddl

import (
	"context"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/vksoftwares/vknex-dbsync/internal/config"
	"github.com/vksoftwares/vknex-dbsync/internal/pg"
	"github.com/vksoftwares/vknex-dbsync/internal/schema"
)

// Plan holds the statements in the order they must run.
type Plan struct {
	EnumValues []string        // ALTER TYPE ... ADD VALUE: committed on their own, before the sync transaction
	Before     []string        // inside the sync transaction, before the data
	After      []string        // after the data: NOT NULL, constraints, indexes
	Reset      map[string]bool // tables whose rows must all be compared again
}

// Count is the number of statements in the plan.
func (p *Plan) Count() int { return len(p.EnumValues) + len(p.Before) + len(p.After) }

// Build plans the schema changes, probing the target in a transaction that
// is always rolled back (see prober).
func Build(ctx context.Context, dst *pgx.Conn, tables, dstAll map[string]*schema.Table, srcInfo, dstInfo *Info, cfg *config.Config) (*Plan, error) {
	tx, err := dst.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, "SET LOCAL search_path = pg_catalog"); err != nil {
		return nil, err
	}
	return build(tables, dstAll, srcInfo, dstInfo, cfg, &prober{ctx: ctx, tx: tx, copies: map[string]string{}})
}

// builder collects statements into groups that run in a fixed order.
type builder struct {
	cfg      *config.Config
	probe    *prober
	problems []string

	schemas, types, funcs, dropFKs, dropOther, dropTables, seqs, creates, alters, addPKs, partitions, owned []string // before the data
	notNull, addOther, indexes, addFKs                                                                      []string // after the data

	enumValues []string
	reset      map[string]bool
	seqSeen    map[string]bool
}

// build works out how to turn the target (dst: every table of the synced
// schemas) into the source (src: the selected tables).
func build(src, dst map[string]*schema.Table, srcInfo, dstInfo *Info, cfg *config.Config, probe *prober) (*Plan, error) {
	b := &builder{cfg: cfg, probe: probe, reset: map[string]bool{}, seqSeen: map[string]bool{}}

	for _, ns := range cfg.Schemas {
		srcHas, dstHas := false, false
		for _, t := range src {
			srcHas = srcHas || t.Schema == ns
		}
		for _, t := range dst {
			dstHas = dstHas || (t.Schema == ns && cfg.Wanted(t.Schema, t.Name))
		}
		if cfg.DDLDrop && !srcHas && dstHas {
			return nil, fmt.Errorf("the source has no tables in schema %q but the target does; refusing to drop them all (check SYNC_SCHEMA and the source URL)", ns)
		}
		if srcInfo.Schemas[ns] && !dstInfo.Schemas[ns] {
			b.schemas = append(b.schemas, "CREATE SCHEMA "+pg.Ident(ns))
		}
	}

	b.enums(srcInfo.Enums, dstInfo.Enums)

	// Constraints, defaults and indexes may call these. Bodies are checked when
	// they run, not now, so they may refer to tables created further down.
	for _, name := range slices.Sorted(maps.Keys(srcInfo.Funcs)) {
		if def := srcInfo.Funcs[name]; dstInfo.Funcs[name] != def {
			if len(b.funcs) == 0 {
				b.funcs = append(b.funcs, "SET LOCAL check_function_bodies = off")
			}
			b.funcs = append(b.funcs, def)
		}
	}

	for _, key := range slices.Sorted(maps.Keys(src)) {
		s := src[key]
		d, ok := dst[key]
		if !ok {
			b.createTable(s)
			continue
		}
		if s.PartKey != d.PartKey {
			b.problems = append(b.problems, fmt.Sprintf("table %s: partitioning differs: source %q, target %q", key, s.PartKey, d.PartKey))
			continue
		}
		b.alterColumns(s, d)
		b.constraints(s, d)
		b.indexesOf(s, d)
		b.partitionsOf(s, d)
	}

	if cfg.DDLDrop {
		for _, key := range slices.Sorted(maps.Keys(dst)) {
			d := dst[key]
			if _, ok := src[key]; !ok && cfg.Wanted(d.Schema, d.Name) {
				b.dropTables = append(b.dropTables, "DROP TABLE "+d.QName())
			}
		}
	}

	if len(b.problems) > 0 {
		sort.Strings(b.problems)
		return nil, fmt.Errorf("the sync cannot change these on the target, fix them by hand first:\n  - %s", strings.Join(b.problems, "\n  - "))
	}
	return &Plan{
		EnumValues: b.enumValues,
		Before: slices.Concat(b.schemas, b.types, b.funcs, b.dropFKs, b.dropOther, b.dropTables, b.seqs,
			b.creates, b.alters, b.addPKs, b.partitions, b.owned),
		After: slices.Concat(b.notNull, b.addOther, b.indexes, b.addFKs),
		Reset: b.reset,
	}, nil
}

// enums creates missing types and adds missing labels in the source's order.
// Labels cannot be removed from an enum, so extra target labels stay.
func (b *builder) enums(src, dst map[string][]string) {
	for _, name := range slices.Sorted(maps.Keys(src)) {
		labels := src[name]
		have, ok := dst[name]
		if !ok {
			quoted := make([]string, len(labels))
			for i, l := range labels {
				quoted[i] = pg.Literal(l)
			}
			b.types = append(b.types, fmt.Sprintf("CREATE TYPE %s AS ENUM (%s)", name, strings.Join(quoted, ", ")))
			continue
		}
		for i, l := range labels {
			if slices.Contains(have, l) {
				continue
			}
			pos := ""
			switch {
			case i > 0:
				pos = " AFTER " + pg.Literal(labels[i-1])
			case len(have) > 0:
				pos = " BEFORE " + pg.Literal(have[0])
			}
			b.enumValues = append(b.enumValues, fmt.Sprintf("ALTER TYPE %s ADD VALUE IF NOT EXISTS %s%s", name, pg.Literal(l), pos))
		}
	}
}

func (b *builder) createTable(s *schema.Table) {
	defs := make([]string, len(s.All))
	for i, c := range s.All {
		b.sequencesOf(c)
		defs[i] = columnDef(c, true)
	}
	q := fmt.Sprintf("CREATE TABLE %s (\n  %s\n)", s.QName(), strings.Join(defs, ",\n  "))
	if s.PartKey != "" {
		q += " PARTITION BY " + s.PartKey
	}
	b.creates = append(b.creates, q)
	for _, c := range s.All {
		b.ownSequence(s, c)
	}
	for _, c := range s.Constraints {
		b.addConstraint(s, c)
	}
	for _, ix := range s.Indexes {
		b.indexes = append(b.indexes, createIndexSQL(ix))
	}
	for _, p := range s.Partitions {
		b.partitions = append(b.partitions, createPartitionSQL(p))
	}
	b.reset[schema.Key(s.Schema, s.Name)] = true
}

func (b *builder) alterColumns(s, d *schema.Table) {
	tn := s.QName()
	key := schema.Key(s.Schema, s.Name)
	var generated []string // added after plain columns: their expressions may use them
	for _, sc := range s.All {
		dc := d.AnyColumn(sc.Name)
		col := pg.Ident(sc.Name)
		switch {
		case dc == nil:
			b.addColumn(s, sc, &generated)
			b.reset[key] = true

		case sc.Generated != "" || dc.Generated != "":
			if sc.Generated != dc.Generated || !b.probe.sameDefault(d, sc, dc.Default) || sc.Type != dc.Type {
				b.alters = append(b.alters, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", tn, col))
				b.addColumn(s, sc, &generated)
				b.reset[key] = true
			}

		default:
			retype := sc.Type != dc.Type
			newDefault := retype || !b.probe.sameDefault(d, sc, dc.Default)
			if dc.Default != "" && newDefault {
				b.alters = append(b.alters, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT", tn, col))
			}
			if !sc.NotNull && dc.NotNull && dc.Identity == "" {
				b.alters = append(b.alters, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL", tn, col))
			}
			if retype {
				b.alters = append(b.alters, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s USING %s::%s", tn, col, sc.Type, col, sc.Type))
				b.reset[key] = true
			}
			if sc.Default != "" && newDefault {
				b.sequencesOf(sc)
				b.alters = append(b.alters, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s", tn, col, sc.Default))
				b.ownSequence(s, sc)
			}
			if sc.NotNull && !dc.NotNull {
				b.notNull = append(b.notNull, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL", tn, col))
			}
		}
	}
	b.alters = append(b.alters, generated...)

	if b.cfg.DDLDrop {
		for _, dc := range d.All {
			if s.AnyColumn(dc.Name) == nil {
				b.alters = append(b.alters, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", tn, pg.Ident(dc.Name)))
			}
		}
	}
}

// addColumn adds a column without NOT NULL; that is set after the data has
// filled it. Generated columns go to *generated, to be added last.
func (b *builder) addColumn(s *schema.Table, c schema.Column, generated *[]string) {
	add := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", s.QName(), columnDef(c, false))
	if c.Generated != "" {
		*generated = append(*generated, add)
		return
	}
	b.sequencesOf(c)
	b.alters = append(b.alters, add)
	b.ownSequence(s, c)
	if c.NotNull && c.Identity == "" {
		b.notNull = append(b.notNull, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL", s.QName(), pg.Ident(c.Name)))
	}
}

// constraints matches by name; a target constraint with the same definition
// under another name is renamed instead of rebuilt.
func (b *builder) constraints(s, d *schema.Table) {
	tn := s.QName()
	srcNames := map[string]bool{}
	for _, c := range s.Constraints {
		srcNames[c.Name] = true
	}
	kept := map[string]bool{}
	for _, sc := range s.Constraints {
		if dc := findConstraint(d.Constraints, sc.Name); dc != nil {
			kept[dc.Name] = true
			if !b.probe.sameConstraint(d, sc.Def, dc.Def) {
				b.dropConstraint(tn, *dc)
				b.addConstraint(s, sc)
			}
			continue
		}
		var renamed bool
		for _, dc := range d.Constraints {
			if !kept[dc.Name] && !srcNames[dc.Name] && dc.Type == sc.Type && sameExpr(dc.Def, sc.Def) {
				kept[dc.Name] = true
				b.dropOther = append(b.dropOther, fmt.Sprintf("ALTER TABLE %s RENAME CONSTRAINT %s TO %s", tn, pg.Ident(dc.Name), pg.Ident(sc.Name)))
				renamed = true
				break
			}
		}
		if !renamed {
			b.addConstraint(s, sc)
		}
	}
	for _, dc := range d.Constraints {
		if !kept[dc.Name] {
			b.dropConstraint(tn, dc)
		}
	}
}

func (b *builder) addConstraint(s *schema.Table, c schema.Constraint) {
	q := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s", s.QName(), pg.Ident(c.Name), c.Def)
	switch c.Type {
	case "p": // the upsert needs it before the data
		b.addPKs = append(b.addPKs, q)
	case "f":
		b.addFKs = append(b.addFKs, q)
	default:
		b.addOther = append(b.addOther, q)
	}
}

func (b *builder) dropConstraint(tn string, c schema.Constraint) {
	q := fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", tn, pg.Ident(c.Name))
	if c.Type == "f" {
		b.dropFKs = append(b.dropFKs, q)
	} else {
		b.dropOther = append(b.dropOther, q)
	}
}

// indexesOf matches indexes by name; one with the same definition under
// another name is renamed.
func (b *builder) indexesOf(s, d *schema.Table) {
	srcNames := map[string]bool{}
	for _, ix := range s.Indexes {
		srcNames[ix.Name] = true
	}
	kept := map[string]bool{}
	for _, si := range s.Indexes {
		if di := findIndex(d.Indexes, si.Name); di != nil {
			kept[di.Name] = true
			if !b.probe.sameIndex(d, si.Def, di.Def) {
				b.dropOther = append(b.dropOther, "DROP INDEX "+pg.QName(d.Schema, di.Name))
				b.indexes = append(b.indexes, createIndexSQL(si))
			}
			continue
		}
		var renamed bool
		for _, di := range d.Indexes {
			if !kept[di.Name] && !srcNames[di.Name] && indexShape(di.Def) == indexShape(si.Def) {
				kept[di.Name] = true
				b.dropOther = append(b.dropOther, fmt.Sprintf("ALTER INDEX %s RENAME TO %s", pg.QName(d.Schema, di.Name), pg.Ident(si.Name)))
				renamed = true
				break
			}
		}
		if !renamed {
			b.indexes = append(b.indexes, createIndexSQL(si))
		}
	}
	for _, di := range d.Indexes {
		if !kept[di.Name] {
			b.dropOther = append(b.dropOther, "DROP INDEX "+pg.QName(d.Schema, di.Name))
		}
	}
}

func (b *builder) partitionsOf(s, d *schema.Table) {
	have := map[string]schema.Partition{}
	for _, p := range d.Partitions {
		have[p.Name] = p
	}
	for _, sp := range s.Partitions {
		dp, ok := have[sp.Name]
		switch {
		case !ok:
			b.partitions = append(b.partitions, createPartitionSQL(sp))
			b.reset[schema.Key(s.Schema, s.Name)] = true
		case dp.Parent != sp.Parent || dp.Bound != sp.Bound || dp.PartKey != sp.PartKey:
			b.problems = append(b.problems, fmt.Sprintf("partition %s differs: source %s %s, target %s %s", sp.Name, sp.Parent, sp.Bound, dp.Parent, dp.Bound))
		}
	}
	if b.cfg.DDLDrop {
		srcHas := map[string]bool{}
		for _, p := range s.Partitions {
			srcHas[p.Name] = true
		}
		for _, dp := range d.Partitions {
			if !srcHas[dp.Name] { // IF EXISTS: dropping a sub-partitioned parent drops its children
				b.dropTables = append(b.dropTables, "DROP TABLE IF EXISTS "+dp.Name)
			}
		}
	}
}

var nextvalRe = regexp.MustCompile(`nextval\('((?:[^']|'')+)'::regclass\)`)

// sequencesOf creates the sequences a column default draws from.
func (b *builder) sequencesOf(c schema.Column) {
	if c.Generated != "" {
		return
	}
	for _, m := range nextvalRe.FindAllStringSubmatch(c.Default, -1) {
		seq := strings.ReplaceAll(m[1], "''", "'")
		if !b.seqSeen[seq] {
			b.seqSeen[seq] = true
			b.seqs = append(b.seqs, "CREATE SEQUENCE IF NOT EXISTS "+seq)
		}
	}
}

// ownSequence ties a serial column's sequence to it, as on the source, so the
// sequence is moved past the copied ids after the sync.
func (b *builder) ownSequence(s *schema.Table, c schema.Column) {
	if c.Identity != "" || !slices.Contains(s.SeqCols, c.Name) {
		return
	}
	if m := nextvalRe.FindStringSubmatch(c.Default); m != nil {
		b.owned = append(b.owned, fmt.Sprintf("ALTER SEQUENCE %s OWNED BY %s.%s", strings.ReplaceAll(m[1], "''", "'"), s.QName(), pg.Ident(c.Name)))
	}
}

func columnDef(c schema.Column, notNull bool) string {
	def := pg.Ident(c.Name) + " " + c.Type
	switch {
	case c.Generated == "s":
		def += " GENERATED ALWAYS AS (" + c.Default + ") STORED"
	case c.Generated == "v":
		def += " GENERATED ALWAYS AS (" + c.Default + ") VIRTUAL"
	case c.Identity == "a":
		def += " GENERATED ALWAYS AS IDENTITY"
	case c.Identity == "d":
		def += " GENERATED BY DEFAULT AS IDENTITY"
	case c.Default != "":
		def += " DEFAULT " + c.Default
	}
	if notNull && c.NotNull && c.Identity == "" {
		def += " NOT NULL"
	}
	return def
}

// createIndexSQL drops ONLY from an index on a partitioned table, so the
// index is built on every partition too.
func createIndexSQL(ix schema.Index) string {
	return strings.Replace(ix.Def, " ON ONLY ", " ON ", 1)
}

// indexShape is an index definition without its name.
func indexShape(def string) string {
	_, rest, _ := strings.Cut(def, " ON ")
	return fmt.Sprint(strings.HasPrefix(def, "CREATE UNIQUE"), normExpr(rest))
}

func createPartitionSQL(p schema.Partition) string {
	q := fmt.Sprintf("CREATE TABLE %s PARTITION OF %s %s", p.Name, p.Parent, p.Bound)
	if p.PartKey != "" {
		q += " PARTITION BY " + p.PartKey
	}
	return q
}

func findConstraint(cs []schema.Constraint, name string) *schema.Constraint {
	for i := range cs {
		if cs[i].Name == name {
			return &cs[i]
		}
	}
	return nil
}

func findIndex(ixs []schema.Index, name string) *schema.Index {
	for i := range ixs {
		if ixs[i].Name == name {
			return &ixs[i]
		}
	}
	return nil
}
