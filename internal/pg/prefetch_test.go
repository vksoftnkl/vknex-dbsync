package pg

import (
	"context"
	"os"
	"testing"
)

// TestPrefetch needs a database: VKNEX_TEST_DB=postgres://... go test ./internal/pg
func TestPrefetch(t *testing.T) {
	url := os.Getenv("VKNEX_TEST_DB")
	if url == "" {
		t.Skip("VKNEX_TEST_DB not set")
	}
	ctx := context.Background()
	conn, err := Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)

	one := func(q Querier, sql string, n int) int {
		var got int
		if err := q.QueryRow(ctx, sql, n).Scan(&got); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return got
	}
	const a, b, c = "SELECT $1::int + 1", "SELECT $1::int + 2", "SELECT $1::int + 3"
	p, err := Prefetch(ctx, tx, []string{"SET LOCAL search_path = pg_catalog"}, []string{a, b, c}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if got := one(p, a, 10); got != 11 {
		t.Errorf("in order: got %d, want 11", got)
	}
	// Out of order: the batch ends, and the rest run one at a time on tx.
	if got := one(p, c, 10); got != 13 {
		t.Errorf("out of order: got %d, want 13", got)
	}
	if got := one(p, b, 20); got != 22 {
		t.Errorf("after the batch: got %d, want 22", got)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	var path string
	if err := tx.QueryRow(ctx, "SHOW search_path").Scan(&path); err != nil || path != "pg_catalog" {
		t.Errorf("search_path %q (%v), want the batch's SET LOCAL to hold in tx", path, err)
	}
	if _, err := Prefetch(ctx, tx, []string{"SELECT nonsense"}, nil); err == nil {
		t.Error("a failing statement before the queries: no error")
	}
}
