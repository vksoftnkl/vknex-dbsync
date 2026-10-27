package pg

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestExplain(t *testing.T) {
	for _, c := range []struct {
		url, code string
		want      string // "" = err unchanged
	}{
		{"postgres://u@127.0.0.1:5432/erp", "3D000", "check the database name and port in the URL: 127.0.0.1:5432 is this machine's own PostgreSQL"},
		{"postgres://u@localhost:15432/erp", "3D000", "check the database name and port in the URL"},
		{"postgres://u@db.example.com:5432/erp", "3D000", "check the database name and port in the URL"},
		{"postgres://u@db.example.com/erp", "28P01", "check the password in the URL"},
		{"postgres://u@db.example.com/erp", "28000", "check the user in the URL and the server's pg_hba.conf"},
		{"postgres://u@db.example.com/erp", "57P03", ""}, // the server is starting up: a retry fixes it
	} {
		cc, err := pgx.ParseConfig(c.url)
		if err != nil {
			t.Fatal(err)
		}
		pgErr := &pgconn.PgError{Severity: "FATAL", Code: c.code, Message: "refused"}
		in := fmt.Errorf("server error: %w", pgErr)
		got := explain(in, cc)
		var back *pgconn.PgError
		if !errors.As(got, &back) || back.Code != c.code {
			t.Errorf("%s %s: the PostgreSQL error is no longer reachable: %v", c.url, c.code, got)
		}
		switch {
		case c.want == "" && got != in:
			t.Errorf("%s %s: got %q, want it unchanged", c.url, c.code, got)
		case c.want != "" && !strings.Contains(got.Error(), "; "+c.want):
			t.Errorf("%s %s: got %q, want the hint %q", c.url, c.code, got, c.want)
		}
		if tunnel := c.url == "postgres://u@127.0.0.1:5432/erp"; strings.Contains(got.Error(), "own PostgreSQL") != tunnel {
			t.Errorf("%s %s: the tunnel hint is only for port 5432 on this machine: %q", c.url, c.code, got)
		}
	}
	if err := errors.New("dial tcp 127.0.0.1:15432: connect: connection refused"); explain(err, &pgx.ConnConfig{}) != err {
		t.Error("a network error got a hint")
	}
}
