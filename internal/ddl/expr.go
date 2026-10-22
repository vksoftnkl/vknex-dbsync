package ddl

import (
	"regexp"
	"strings"
)

// sameExpr compares two printed definitions or expressions.
func sameExpr(a, b string) bool { return a == b || normExpr(a) == normExpr(b) }

// typeNameRe matches a printed type such as text, character varying(20) or
// accounts."GdeStatus".
var typeNameRe = regexp.MustCompile(`^[A-Za-z_"][A-Za-z0-9_ ."]*(\([0-9, ]+\))?$`)

// normExpr rewrites "(ARRAY[a, b])::t[]" as "ARRAY[(a)::t, (b)::t]".
// Postgres prints "col IN ('a', 'b')" in the first form, but parsing that text
// back stores the second, so a copied constraint would otherwise never compare
// equal to the original and would be rebuilt on every run.
func normExpr(s string) string {
	const open = "(ARRAY["
	var out strings.Builder
	for {
		i := strings.Index(s, open)
		if i < 0 {
			out.WriteString(s)
			return out.String()
		}
		end := closingBracket(s, i+len(open))
		rest := ""
		if end >= 0 {
			rest = s[end+1:]
		}
		typ, after, ok := strings.Cut(strings.TrimPrefix(rest, ")::"), "[]")
		if end < 0 || !strings.HasPrefix(rest, ")::") || !ok || !typeNameRe.MatchString(typ) {
			out.WriteString(s[:i+len(open)])
			s = s[i+len(open):]
			continue
		}
		elems := splitTopLevel(s[i+len(open) : end])
		for k, e := range elems {
			elems[k] = "(" + e + ")::" + typ
		}
		out.WriteString(s[:i])
		out.WriteString("ARRAY[" + strings.Join(elems, ", ") + "]")
		s = after
	}
}

// closingBracket returns the index of the "]" that closes a "[" opened just
// before from, skipping quoted literals, or -1.
func closingBracket(s string, from int) int {
	depth, quoted := 1, false
	for j := from; j < len(s); j++ {
		switch c := s[j]; {
		case quoted:
			quoted = c != '\''
		case c == '\'':
			quoted = true
		case c == '[':
			depth++
		case c == ']':
			if depth--; depth == 0 {
				return j
			}
		}
	}
	return -1
}

// splitTopLevel splits on commas outside quotes, parentheses and brackets.
func splitTopLevel(s string) []string {
	var parts []string
	depth, quoted, start := 0, false, 0
	for j := 0; j < len(s); j++ {
		switch c := s[j]; {
		case quoted:
			quoted = c != '\''
		case c == '\'':
			quoted = true
		case c == '(' || c == '[':
			depth++
		case c == ')' || c == ']':
			depth--
		case c == ',' && depth == 0:
			parts = append(parts, strings.TrimSpace(s[start:j]))
			start = j + 1
		}
	}
	return append(parts, strings.TrimSpace(s[start:]))
}
