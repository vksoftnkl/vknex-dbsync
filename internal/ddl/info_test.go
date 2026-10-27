package ddl

import (
	"context"
	"slices"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type recorder struct{ sql []string }

func (r *recorder) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	r.sql = append(r.sql, sql)
	return noRows{}, nil
}

func (r *recorder) QueryRow(context.Context, string, ...any) pgx.Row {
	panic("LoadInfo uses Query only")
}

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

// InfoQueries must list LoadInfo's queries in its order (see schema.LoadQueries).
func TestInfoQueries(t *testing.T) {
	r := &recorder{}
	if _, err := LoadInfo(context.Background(), r, []string{"public"}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(r.sql, InfoQueries) {
		t.Fatalf("LoadInfo ran %d queries, InfoQueries lists %d, or in another order", len(r.sql), len(InfoQueries))
	}
}
