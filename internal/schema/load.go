package schema

import (
	"context"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"

	"github.com/vksoftwares/vknex-dbsync/internal/pg"
)

// Every query takes the list of schemas as $1. Tables are keyed "schema.table".
const columnsSQL = `
SELECT n.nspname, c.relname,
       a.attname,
       format_type(a.atttypid, a.atttypmod),
       a.attgenerated::text,
       a.attidentity::text,
       a.attnotnull,
       COALESCE(pg_get_expr(d.adbin, d.adrelid), ''),
       (t.typtype = 'c' OR (t.typcategory = 'A' AND (et.oid >= 16384 OR et.typtype IN ('c','e','d')))) AS need_text,
       (COALESCE(et.typname, t.typname) IN ('json','xml','point','line','lseg','box','path','polygon','circle')) AS no_eq,
       COALESCE(pg_get_partkeydef(c.oid), '')
FROM pg_class c
JOIN pg_namespace n    ON n.oid = c.relnamespace
JOIN pg_attribute a    ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
JOIN pg_type t         ON t.oid = a.atttypid
LEFT JOIN pg_type et   ON t.typcategory = 'A' AND et.oid = t.typelem
LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE n.nspname::text = ANY($1::text[]) AND c.relkind IN ('r','p') AND NOT c.relispartition
ORDER BY n.nspname, c.relname, a.attnum`

const pkSQL = `
SELECT n.nspname || '.' || c.relname, a.attname
FROM pg_index i
JOIN pg_class c     ON c.oid = i.indrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord) ON true
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
WHERE i.indisprimary AND n.nspname::text = ANY($1::text[])
ORDER BY n.nspname, c.relname, k.ord`

const fkSQL = `
SELECT n.nspname || '.' || c.relname, con.conname, pn.nspname || '.' || p.relname,
       array_agg(a.attname::text ORDER BY k.ord),
       bool_and(NOT a.attnotnull)
FROM pg_constraint con
JOIN pg_class c      ON c.oid = con.conrelid
JOIN pg_namespace n  ON n.oid = c.relnamespace
JOIN pg_class p      ON p.oid = con.confrelid
JOIN pg_namespace pn ON pn.oid = p.relnamespace
JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord) ON true
JOIN pg_attribute a  ON a.attrelid = con.conrelid AND a.attnum = k.attnum
WHERE con.contype = 'f' AND n.nspname::text = ANY($1::text[]) AND pn.nspname::text = ANY($1::text[])
GROUP BY n.nspname, c.relname, con.conname, pn.nspname, p.relname`

const uniqueSQL = `
SELECT n.nspname || '.' || c.relname,
       array_agg(COALESCE(a.attname::text, '') ORDER BY k.ord),
       array_agg(pg_get_indexdef(i.indexrelid, k.ord, true) ORDER BY k.ord),
       NULL::text[],
       max(COALESCE(pg_get_expr(i.indpred, i.indrelid, true), '')),
       bool_and(NOT i.indnullsnotdistinct)
FROM pg_index i
JOIN pg_class c     ON c.oid = i.indrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN LATERAL generate_series(1, i.indnkeyatts::int) AS k(ord)
LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = i.indkey[k.ord - 1] AND a.attnum > 0
WHERE i.indisunique AND NOT i.indisprimary AND n.nspname::text = ANY($1::text[])
GROUP BY n.nspname, c.relname, i.indexrelid
UNION ALL
SELECT n.nspname || '.' || c.relname,
       array_agg(COALESCE(a.attname::text, '') ORDER BY k.ord),
       array_agg(pg_get_indexdef(i.indexrelid, k.ord, true) ORDER BY k.ord),
       array_agg(format('OPERATOR(%I.%s)', opn.nspname, op.oprname) ORDER BY k.ord),
       max(COALESCE(pg_get_expr(i.indpred, i.indrelid, true), '')),
       true
FROM pg_constraint con
JOIN pg_index i      ON i.indexrelid = con.conindid
JOIN pg_class c      ON c.oid = con.conrelid
JOIN pg_namespace n  ON n.oid = c.relnamespace
CROSS JOIN LATERAL generate_series(1, i.indnkeyatts::int) AS k(ord)
JOIN pg_operator op  ON op.oid = con.conexclop[k.ord]
JOIN pg_namespace opn ON opn.oid = op.oprnamespace
LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = i.indkey[k.ord - 1] AND a.attnum > 0
WHERE con.contype = 'x' AND n.nspname::text = ANY($1::text[])
GROUP BY n.nspname, c.relname, con.oid`

const seqSQL = `
SELECT n.nspname || '.' || c.relname, a.attname
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
WHERE n.nspname::text = ANY($1::text[]) AND c.relkind IN ('r','p') AND NOT c.relispartition
  AND pg_get_serial_sequence(format('%I.%I', n.nspname, c.relname), a.attname) IS NOT NULL`

// Constraints cloned onto partitions (conparentid <> 0) and NOT NULL
// constraints (handled as column properties) are left out.
const constraintSQL = `
SELECT n.nspname || '.' || c.relname, con.conname, con.contype::text, pg_get_constraintdef(con.oid)
FROM pg_constraint con
JOIN pg_class c     ON c.oid = con.conrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname::text = ANY($1::text[]) AND c.relkind IN ('r','p') AND NOT c.relispartition
  AND con.contype IN ('p','u','c','f','x') AND con.conislocal AND con.conparentid = 0
ORDER BY 1, 2`

const indexSQL = `
SELECT n.nspname || '.' || c.relname, ic.relname, pg_get_indexdef(i.indexrelid)
FROM pg_index i
JOIN pg_class ic    ON ic.oid = i.indexrelid
JOIN pg_class c     ON c.oid = i.indrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname::text = ANY($1::text[]) AND c.relkind IN ('r','p') AND NOT c.relispartition
  AND NOT EXISTS (SELECT 1 FROM pg_constraint k
                  WHERE k.conindid = i.indexrelid AND k.conrelid = i.indrelid AND k.contype IN ('p','u','x'))
ORDER BY 1, 2`

const partitionSQL = `
SELECT rn.nspname || '.' || r.relname,
       format('%I.%I', n.nspname, c.relname),
       format('%I.%I', pn.nspname, p.relname),
       pg_get_expr(c.relpartbound, c.oid),
       COALESCE(pg_get_partkeydef(c.oid), '')
FROM pg_class r
JOIN pg_namespace rn ON rn.oid = r.relnamespace
CROSS JOIN LATERAL pg_partition_tree(r.oid) t
JOIN pg_class c      ON c.oid = t.relid
JOIN pg_namespace n  ON n.oid = c.relnamespace
JOIN pg_class p      ON p.oid = t.parentrelid
JOIN pg_namespace pn ON pn.oid = p.relnamespace
WHERE r.relkind = 'p' AND NOT r.relispartition AND rn.nspname::text = ANY($1::text[]) AND t.level > 0
ORDER BY 1, t.level, 2`

// Load introspects every table of the given schemas. Run it inside
// CatalogPath so that types and expressions come back schema-qualified.
func Load(ctx context.Context, q pg.Querier, schemas []string) (map[string]*Table, error) {
	tables := map[string]*Table{}

	rows, err := q.Query(ctx, columnsSQL, schemas)
	if err != nil {
		return nil, fmt.Errorf("read columns: %w", err)
	}
	for rows.Next() {
		var schema, tbl, partKey string
		var col Column
		if err := rows.Scan(&schema, &tbl, &col.Name, &col.Type, &col.Generated, &col.Identity, &col.NotNull, &col.Default,
			&col.NeedText, &col.NoEq, &partKey); err != nil {
			rows.Close()
			return nil, err
		}
		key := Key(schema, tbl)
		t := tables[key]
		if t == nil {
			t = &Table{Schema: schema, Name: tbl, PartKey: partKey}
			tables[key] = t
		}
		t.All = append(t.All, col)
		if col.Generated != "" { // GENERATED ALWAYS AS (...): computed on target
			continue
		}
		t.Columns = append(t.Columns, col)
		if col.NeedText {
			t.TextCopy = true
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if err := scanPairs(ctx, q, pkSQL, schemas, func(tbl, col string) {
		if t := tables[tbl]; t != nil {
			t.PK = append(t.PK, col)
		}
	}); err != nil {
		return nil, fmt.Errorf("read primary keys: %w", err)
	}
	fkRows, err := q.Query(ctx, fkSQL, schemas)
	if err != nil {
		return nil, fmt.Errorf("read foreign keys: %w", err)
	}
	for fkRows.Next() {
		var child string
		var fk FK
		if err := fkRows.Scan(&child, &fk.Name, &fk.Parent, &fk.Cols, &fk.Nullable); err != nil {
			fkRows.Close()
			return nil, err
		}
		if t := tables[child]; t != nil && child != fk.Parent { // self-references are fine inside one statement
			t.FKs = append(t.FKs, fk)
		}
	}
	fkRows.Close()
	if err := fkRows.Err(); err != nil {
		return nil, err
	}

	uqRows, err := q.Query(ctx, uniqueSQL, schemas)
	if err != nil {
		return nil, fmt.Errorf("read unique indexes: %w", err)
	}
	for uqRows.Next() {
		var tbl string
		var u UniqueKey
		if err := uqRows.Scan(&tbl, &u.Cols, &u.Exprs, &u.Ops, &u.Pred, &u.NullsDistinct); err != nil {
			uqRows.Close()
			return nil, err
		}
		if slices.Contains(u.Cols, "") {
			u.Cols = nil
		}
		if t := tables[tbl]; t != nil {
			t.Uniques = append(t.Uniques, u)
		}
	}
	uqRows.Close()
	if err := uqRows.Err(); err != nil {
		return nil, err
	}
	if err := scanPairs(ctx, q, seqSQL, schemas, func(tbl, col string) {
		if t := tables[tbl]; t != nil {
			t.SeqCols = append(t.SeqCols, col)
		}
	}); err != nil {
		return nil, fmt.Errorf("read sequences: %w", err)
	}

	conRows, err := q.Query(ctx, constraintSQL, schemas)
	if err != nil {
		return nil, fmt.Errorf("read constraints: %w", err)
	}
	for conRows.Next() {
		var tbl string
		var c Constraint
		if err := conRows.Scan(&tbl, &c.Name, &c.Type, &c.Def); err != nil {
			conRows.Close()
			return nil, err
		}
		if t := tables[tbl]; t != nil {
			t.Constraints = append(t.Constraints, c)
		}
	}
	conRows.Close()
	if err := conRows.Err(); err != nil {
		return nil, err
	}

	ixRows, err := q.Query(ctx, indexSQL, schemas)
	if err != nil {
		return nil, fmt.Errorf("read indexes: %w", err)
	}
	for ixRows.Next() {
		var tbl string
		var ix Index
		if err := ixRows.Scan(&tbl, &ix.Name, &ix.Def); err != nil {
			ixRows.Close()
			return nil, err
		}
		if t := tables[tbl]; t != nil {
			t.Indexes = append(t.Indexes, ix)
		}
	}
	ixRows.Close()
	if err := ixRows.Err(); err != nil {
		return nil, err
	}

	ptRows, err := q.Query(ctx, partitionSQL, schemas)
	if err != nil {
		return nil, fmt.Errorf("read partitions: %w", err)
	}
	for ptRows.Next() {
		var root string
		var p Partition
		if err := ptRows.Scan(&root, &p.Name, &p.Parent, &p.Bound, &p.PartKey); err != nil {
			ptRows.Close()
			return nil, err
		}
		if t := tables[root]; t != nil {
			t.Partitions = append(t.Partitions, p)
		}
	}
	ptRows.Close()
	if err := ptRows.Err(); err != nil {
		return nil, err
	}
	return tables, nil
}

func scanPairs(ctx context.Context, q pg.Querier, sql string, schemas []string, fn func(a, b string)) error {
	rows, err := q.Query(ctx, sql, schemas)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var a, b string
		if err := rows.Scan(&a, &b); err != nil {
			return err
		}
		fn(a, b)
	}
	return rows.Err()
}

// CatalogPath runs fn with search_path = pg_catalog, so that format_type(),
// pg_get_expr() and pg_get_*def() print schema-qualified names that mean the
// same thing on both databases. The previous search_path is restored after.
func CatalogPath(ctx context.Context, tx pgx.Tx, fn func() error) error {
	var old string
	if err := tx.QueryRow(ctx, "SELECT current_setting('search_path')").Scan(&old); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "SELECT set_config('search_path', 'pg_catalog', true)"); err != nil {
		return err
	}
	if err := fn(); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, "SELECT set_config('search_path', $1, true)", old)
	return err
}
