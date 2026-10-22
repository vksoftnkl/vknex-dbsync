package schema

import (
	"fmt"
	"sort"
	"strings"
)

// Compare makes sure the target can receive the source columns as-is.
func Compare(src, dst map[string]*Table) (problems, warnings []string) {
	for name, s := range src {
		d, ok := dst[name]
		if !ok {
			problems = append(problems, fmt.Sprintf("table %q is missing on target", name))
			continue
		}
		for _, sc := range s.Columns {
			dc := d.Column(sc.Name)
			switch {
			case dc == nil:
				problems = append(problems, fmt.Sprintf("column %s.%s is missing on target", name, sc.Name))
			case dc.Type != sc.Type:
				problems = append(problems, fmt.Sprintf("column %s.%s type differs: source %s, target %s", name, sc.Name, sc.Type, dc.Type))
			}
		}
		for _, dc := range d.Columns {
			if s.Column(dc.Name) == nil {
				warnings = append(warnings, fmt.Sprintf("column %s.%s exists only on target (left at its default)", name, dc.Name))
			}
		}
		if strings.Join(s.PK, ",") != strings.Join(d.PK, ",") {
			problems = append(problems, fmt.Sprintf("primary key of %q differs: source (%s), target (%s)", name, strings.Join(s.PK, ","), strings.Join(d.PK, ",")))
		}
	}
	sort.Strings(problems)
	sort.Strings(warnings)
	return
}
