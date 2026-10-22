package ddl

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/vksoftwares/vknex-dbsync/internal/pg"
	"github.com/vksoftwares/vknex-dbsync/internal/schema"
)

// prober decides whether a source definition that is printed differently from
// the target's is still the same. Postgres does not always print a definition
// back the way it was written (nested ANDs are flattened, IN lists become
// casted arrays), so it builds the source definition on an empty temp copy of
// the target table and compares what the target prints. Everything happens in
// a transaction that is rolled back. A nil prober only compares text.
type prober struct {
	ctx    context.Context
	tx     pgx.Tx
	copies map[string]string // target table -> its temp copy
}

func (p *prober) copyOf(d *schema.Table) (string, bool) {
	if name, ok := p.copies[d.QName()]; ok {
		return name, name != ""
	}
	name := fmt.Sprintf("pg_temp._sync_probe_%d", len(p.copies))
	if _, ok := p.try([]string{fmt.Sprintf("CREATE TEMP TABLE %s (LIKE %s) ON COMMIT DROP", name, d.QName())}, "", true); !ok {
		name = ""
	}
	p.copies[d.QName()] = name
	return name, name != ""
}

// try runs stmts and query in a savepoint, which is rolled back unless keep.
func (p *prober) try(stmts []string, query string, keep bool, args ...any) (string, bool) {
	sp, err := p.tx.Begin(p.ctx)
	if err != nil {
		return "", false
	}
	for _, q := range stmts {
		if _, err := sp.Exec(p.ctx, q); err != nil {
			sp.Rollback(p.ctx)
			return "", false
		}
	}
	var out string
	if query != "" {
		if err := sp.QueryRow(p.ctx, query, args...).Scan(&out); err != nil {
			sp.Rollback(p.ctx)
			return "", false
		}
	}
	if keep {
		return out, sp.Commit(p.ctx) == nil
	}
	return out, sp.Rollback(p.ctx) == nil
}

func (p *prober) sameConstraint(d *schema.Table, src, dst string) bool {
	if sameExpr(src, dst) {
		return true
	}
	if p == nil {
		return false
	}
	cp, ok := p.copyOf(d)
	if !ok {
		return false
	}
	got, ok := p.try([]string{fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT _sync_probe_c %s", cp, src)},
		"SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = $1::regclass AND conname = '_sync_probe_c'", false, cp)
	return ok && sameExpr(got, dst)
}

func (p *prober) sameIndex(d *schema.Table, src, dst string) bool {
	if sameExpr(src, dst) {
		return true
	}
	if p == nil {
		return false
	}
	cp, ok := p.copyOf(d)
	if !ok {
		return false
	}
	unique, tail := indexTail(src)
	got, ok := p.try([]string{fmt.Sprintf("CREATE %sINDEX _sync_probe_i ON %s USING %s", unique, cp, tail)},
		"SELECT pg_get_indexdef('pg_temp._sync_probe_i'::regclass)", false)
	if !ok {
		return false
	}
	gu, gt := indexTail(got)
	du, dt := indexTail(dst)
	return gu == du && sameExpr(gt, dt)
}

// sameDefault compares a column default, or a generated column's expression.
func (p *prober) sameDefault(d *schema.Table, src schema.Column, dst string) bool {
	if sameExpr(src.Default, dst) {
		return true
	}
	if p == nil || src.Default == "" || dst == "" {
		return false
	}
	cp, ok := p.copyOf(d)
	if !ok {
		return false
	}
	col, stmt := src.Name, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s", cp, pg.Ident(src.Name), src.Default)
	if src.Generated != "" {
		col = "_sync_probe_g"
		stmt = fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", cp, columnDef(schema.Column{Name: col, Type: src.Type, Generated: src.Generated, Default: src.Default}, false))
	}
	got, ok := p.try([]string{stmt},
		"SELECT pg_get_expr(d.adbin, d.adrelid) FROM pg_attrdef d JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum WHERE d.adrelid = $1::regclass AND a.attname = $2",
		false, cp, col)
	return ok && sameExpr(got, dst)
}

// indexTail splits an index definition into "UNIQUE " (or "") and the part
// after USING, which does not name the index or its table.
func indexTail(def string) (string, string) {
	unique := ""
	if strings.HasPrefix(def, "CREATE UNIQUE") {
		unique = "UNIQUE "
	}
	_, tail, _ := strings.Cut(def, " USING ")
	return unique, tail
}
