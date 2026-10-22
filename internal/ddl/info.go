package ddl

import (
	"context"
	"fmt"

	"github.com/vksoftwares/vknex-dbsync/internal/pg"
)

// Info is the catalog information beyond tables that -ddl needs.
type Info struct {
	Schemas map[string]bool
	Enums   map[string][]string // quoted schema.name -> labels in order
	Funcs   map[string]string   // schema.name(args) -> CREATE OR REPLACE FUNCTION ...
}

// Functions and procedures that do not belong to an extension.
const funcSQL = `
SELECT format('%I.%I(%s)', n.nspname, p.proname, pg_get_function_identity_arguments(p.oid)), pg_get_functiondef(p.oid)
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname::text = ANY($1::text[]) AND p.prokind IN ('f','p')
  AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_proc'::regclass AND d.objid = p.oid AND d.deptype = 'e')`

const enumSQL = `
SELECT format('%I.%I', n.nspname, t.typname), array_agg(e.enumlabel::text ORDER BY e.enumsortorder)
FROM pg_type t
JOIN pg_namespace n ON n.oid = t.typnamespace
JOIN pg_enum e      ON e.enumtypid = t.oid
WHERE n.nspname::text = ANY($1::text[])
GROUP BY n.nspname, t.typname`

// LoadInfo reads the schemas, enum types and functions of the given schemas.
func LoadInfo(ctx context.Context, q pg.Querier, schemas []string) (*Info, error) {
	info := &Info{Schemas: map[string]bool{}, Enums: map[string][]string{}, Funcs: map[string]string{}}
	rows, err := q.Query(ctx, "SELECT nspname::text FROM pg_namespace WHERE nspname::text = ANY($1::text[])", schemas)
	if err != nil {
		return nil, fmt.Errorf("read schemas: %w", err)
	}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			rows.Close()
			return nil, err
		}
		info.Schemas[n] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = q.Query(ctx, enumSQL, schemas)
	if err != nil {
		return nil, fmt.Errorf("read enum types: %w", err)
	}
	for rows.Next() {
		var name string
		var labels []string
		if err := rows.Scan(&name, &labels); err != nil {
			rows.Close()
			return nil, err
		}
		info.Enums[name] = labels
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = q.Query(ctx, funcSQL, schemas)
	if err != nil {
		return nil, fmt.Errorf("read functions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name, def string
		if err := rows.Scan(&name, &def); err != nil {
			return nil, err
		}
		info.Funcs[name] = def
	}
	return info, rows.Err()
}
