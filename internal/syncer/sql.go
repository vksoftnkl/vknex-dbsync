package syncer

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/vksoftwares/vknex-dbsync/internal/pg"
	"github.com/vksoftwares/vknex-dbsync/internal/schema"
)

// upsertSQL inserts new rows and updates only rows whose values actually
// changed, so the overlap window does not rewrite (and bloat) unchanged rows.
func upsertSQL(t *schema.Table, stage string) string {
	pk := toSet(t.PK)

	var cols, vals, sets, oldVals, newVals []string
	for _, c := range t.Columns {
		cols = append(cols, pg.Ident(c.Name))
		if t.IsBreak(c.Name) {
			vals = append(vals, "NULL") // filled in by fixBreakSQL
			continue
		}
		vals = append(vals, "s."+pg.Ident(c.Name))
		// GENERATED ALWAYS identity values can be inserted (OVERRIDING SYSTEM
		// VALUE) but never updated, and they do not change on the source either.
		if pk[c.Name] || c.Identity == "a" {
			continue
		}
		sets = append(sets, fmt.Sprintf("%s = EXCLUDED.%s", pg.Ident(c.Name), pg.Ident(c.Name)))
		cast := ""
		if c.NoEq {
			cast = "::text"
		}
		oldVals = append(oldVals, "d."+pg.Ident(c.Name)+cast)
		newVals = append(newVals, "EXCLUDED."+pg.Ident(c.Name)+cast)
	}

	overriding := ""
	if t.HasAlwaysIdentity() {
		overriding = " OVERRIDING SYSTEM VALUE"
	}

	q := fmt.Sprintf("INSERT INTO %s AS d (%s)%s SELECT %s FROM %s s ON CONFLICT (%s) ",
		t.QName(), strings.Join(cols, ", "), overriding, strings.Join(vals, ", "), stage, pg.IdentList(t.PK, ""))
	if len(sets) == 0 {
		return q + "DO NOTHING"
	}
	return q + fmt.Sprintf("DO UPDATE SET %s WHERE ROW(%s) IS DISTINCT FROM ROW(%s)",
		strings.Join(sets, ", "), strings.Join(oldVals, ", "), strings.Join(newVals, ", "))
}

// fixBreakSQL copies FK-cycle columns from the stage once all rows exist.
func fixBreakSQL(t *schema.Table, stage string) string {
	sets := make([]string, len(t.Break))
	for i, c := range t.Break {
		sets[i] = fmt.Sprintf("%s = s.%s", pg.Ident(c), pg.Ident(c))
	}
	return fmt.Sprintf("UPDATE %s d SET %s FROM %s s WHERE %s AND ROW(%s) IS DISTINCT FROM ROW(%s)",
		t.QName(), strings.Join(sets, ", "), stage, matchCols(t.PK, "s", "d"),
		pg.IdentList(t.Break, "d."), pg.IdentList(t.Break, "s."))
}

// selectExprs lists source columns; with nullBreak, FK-cycle columns become NULL.
func selectExprs(t *schema.Table, nullBreak bool) []string {
	out := make([]string, len(t.Columns))
	for i, c := range t.Columns {
		if nullBreak && t.IsBreak(c.Name) {
			out[i] = fmt.Sprintf("NULL::%s", c.Type)
		} else {
			out[i] = pg.Ident(c.Name)
		}
	}
	return out
}

// uniqueMatchSQL selects the staged rows (alias s, with t's primary key) that
// would collide with target row d of t on unique index u.
func uniqueMatchSQL(t *schema.Table, u schema.UniqueKey, stage string) string {
	if u.Plain() {
		return fmt.Sprintf("SELECT 1 FROM %s s WHERE %s", stage, matchCols(u.Cols, "s", "d"))
	}
	if u.Ops == nil && u.NullsDistinct {
		// Expression keys and partial-index predicates are computed on both
		// sides as the index prints them, then joined on equality, which hashes;
		// row d is found again by its primary key.
		side := func(from string) string {
			keys := make([]string, len(u.Exprs))
			for i, e := range u.Exprs {
				keys[i] = fmt.Sprintf("(%s) AS _k%d", e, i)
			}
			q := fmt.Sprintf("SELECT %s, %s FROM %s", pg.IdentList(t.PK, ""), strings.Join(keys, ", "), from)
			if u.Pred != "" {
				q += " WHERE " + u.Pred
			}
			return q
		}
		conds := make([]string, len(u.Exprs))
		for i := range u.Exprs {
			conds[i] = fmt.Sprintf("s._k%d = dv._k%d", i, i)
		}
		return fmt.Sprintf("SELECT 1 FROM (%s) s JOIN (%s) dv ON %s WHERE %s",
			side(stage), side(t.QName()), strings.Join(conds, " AND "), matchCols(t.PK, "dv", "d"))
	}
	// EXCLUDE operators and NULLS NOT DISTINCT do not hash: evaluate the keys
	// against each pair of rows through a LATERAL (SELECT row.*) instead.
	op := "="
	if !u.NullsDistinct {
		op = "IS NOT DISTINCT FROM"
	}
	keys := make([]string, len(u.Exprs))
	conds := make([]string, len(u.Exprs))
	for i, e := range u.Exprs {
		if u.Ops != nil {
			op = u.Ops[i]
		}
		keys[i] = fmt.Sprintf("(%s) AS k%d", e, i)
		conds[i] = fmt.Sprintf("sv.k%d %s dv.k%d", i, op, i)
	}
	pred := "true"
	if u.Pred != "" {
		pred = "(" + u.Pred + ")"
	}
	side := fmt.Sprintf("(SELECT %s, %s AS p FROM (SELECT %%s.*) x)", strings.Join(keys, ", "), pred)
	return fmt.Sprintf("SELECT 1 FROM %s s, LATERAL %s sv, LATERAL %s dv WHERE sv.p AND dv.p AND %s",
		stage, fmt.Sprintf(side, "s"), fmt.Sprintf(side, "d"), strings.Join(conds, " AND "))
}

func matchCols(cols []string, a, b string) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = fmt.Sprintf("%s.%s = %s.%s", a, pg.Ident(c), b, pg.Ident(c))
	}
	return strings.Join(parts, " AND ")
}

// hint adds a next step to the two errors a sync is most likely to hit.
func hint(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23503":
			return fmt.Errorf("%w\n  hint: rows on the target reference rows the source no longer has; re-run with -disable-triggers (RDS master user) or use -mode full -yes", err)
		case "23505", "23P01":
			return fmt.Errorf("%w\n  hint: a unique key conflicts with a target-only row; re-run with -deletes=true or use -mode full -yes", err)
		}
	}
	return err
}

func (s *Syncer) changedSince(c *schema.Column, since time.Time) string {
	lit := pg.Literal(since.UTC().Format("2006-01-02 15:04:05.999999") + "+00")
	bound := lit + "::timestamptz"
	if strings.Contains(c.Type, "without time zone") {
		bound = fmt.Sprintf("(%s::timestamptz AT TIME ZONE %s)", lit, pg.Literal(s.cfg.NaiveTZ))
	}
	return fmt.Sprintf("(%s >= %s OR %s IS NULL)", pg.Ident(c.Name), bound, pg.Ident(c.Name))
}

func selectSQL(t *schema.Table, exprs []string, where string) string {
	q := fmt.Sprintf("SELECT %s FROM %s", strings.Join(exprs, ", "), t.QName())
	if where != "" {
		q += " WHERE " + where
	}
	return q
}

func toSet(xs []string) map[string]bool {
	m := map[string]bool{}
	for _, x := range xs {
		m[x] = true
	}
	return m
}
