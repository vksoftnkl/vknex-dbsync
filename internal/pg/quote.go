package pg

import (
	"strings"

	"github.com/jackc/pgx/v5"
)

// Ident quotes an identifier, QName a schema-qualified name and Literal a
// string literal.
func Ident(name string) string          { return pgx.Identifier{name}.Sanitize() }
func QName(schema, table string) string { return pgx.Identifier{schema, table}.Sanitize() }
func Literal(s string) string           { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// IdentList renders `"a", "b"` or, with a prefix, `d."a", d."b"`.
func IdentList(cols []string, prefix string) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = prefix + Ident(c)
	}
	return strings.Join(parts, ", ")
}
