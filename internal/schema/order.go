package schema

import (
	"sort"
)

// FKOrder returns tables parents-first (Kahn's algorithm). When tables form an
// FK cycle (e.g. Branch.managerId -> Employee, Employee.branchId -> Branch) and
// breakCycles is set, one nullable FK per cycle is "broken": its columns are
// recorded in Table.Break, written as NULL in the first pass and filled in once
// every table is loaded. Cycles that cannot be broken are returned in cyclic.
func FKOrder(tables map[string]*Table, breakCycles bool) (order []string, cyclic []string) {
	type edge struct {
		child string
		fk    FK
		done  bool
	}
	var edges []*edge
	indeg := map[string]int{}
	byParent := map[string][]*edge{}
	for name, t := range tables {
		indeg[name] += 0
		for _, fk := range t.FKs {
			e := &edge{child: name, fk: fk}
			edges = append(edges, e)
			byParent[fk.Parent] = append(byParent[fk.Parent], e)
			indeg[name]++
		}
	}
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].child != edges[j].child {
			return edges[i].child < edges[j].child
		}
		return edges[i].fk.Name < edges[j].fk.Name
	})

	placed := map[string]bool{}
	var ready []string
	push := func(n string) {
		ready = append(ready, n)
		sort.Strings(ready)
	}
	for n, d := range indeg {
		if d == 0 {
			push(n)
		}
	}
	for len(order) < len(indeg) {
		if len(ready) == 0 {
			// Stuck: every remaining table waits on another remaining table.
			var pick *edge
			if breakCycles {
				for _, e := range edges {
					if !e.done && !placed[e.child] && !placed[e.fk.Parent] && e.fk.Nullable {
						pick = e
						break
					}
				}
			}
			if pick == nil {
				for n := range indeg {
					if !placed[n] {
						cyclic = append(cyclic, n)
					}
				}
				sort.Strings(cyclic)
				return append(order, cyclic...), cyclic
			}
			pick.done = true
			t := tables[pick.child]
			for _, c := range pick.fk.Cols {
				if !t.IsBreak(c) {
					t.Break = append(t.Break, c)
				}
			}
			if indeg[pick.child]--; indeg[pick.child] == 0 {
				push(pick.child)
			}
			continue
		}
		n := ready[0]
		ready = ready[1:]
		order = append(order, n)
		placed[n] = true
		for _, e := range byParent[n] {
			if e.done {
				continue
			}
			e.done = true
			if indeg[e.child]--; indeg[e.child] == 0 {
				push(e.child)
			}
		}
	}
	return order, nil
}
