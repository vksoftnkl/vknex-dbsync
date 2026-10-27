package schema

import (
	"context"
	"slices"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// recorder is a Querier that notes each query and answers it with no rows.
type recorder struct{ sql []string }

func (r *recorder) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	r.sql = append(r.sql, sql)
	return noRows{}, nil
}

func (r *recorder) QueryRow(context.Context, string, ...any) pgx.Row { panic("Load uses Query only") }

type noRows struct{}

func (noRows) Close()                                       {}
func (noRows) Err() error                                   { return nil }
func (noRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (noRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (noRows) Next() bool                                   { return false }
func (noRows) Scan(...any) error                            { return nil }
func (noRows) Values() ([]any, error)                       { return nil, nil }
func (noRows) RawValues() [][]byte                          { return nil }
func (noRows) Conn() *pgx.Conn                              { return nil }

// LoadQueries must list Load's queries in its order, or the target's catalog
// is read one query at a time again (see pg.Prefetch).
func TestLoadQueries(t *testing.T) {
	r := &recorder{}
	if _, err := Load(context.Background(), r, []string{"public"}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(r.sql, LoadQueries) {
		t.Fatalf("Load ran %d queries, LoadQueries lists %d, or in another order", len(r.sql), len(LoadQueries))
	}
}
