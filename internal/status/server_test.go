package status

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/vksoftwares/vknex-dbsync/internal/config"
)

func TestGuard(t *testing.T) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, password := range []string{"", "s3cret"} {
		s := &server{st: New(), cfg: &config.Config{UIPassword: password, Interval: 5 * time.Minute}, log: quiet}
		h := s.handler()
		do := func(method, path, pass string, fromPage bool) int {
			r := httptest.NewRequest(method, path, nil)
			if pass != "" {
				r.SetBasicAuth("anyone", pass)
			}
			if fromPage {
				r.Header.Set(pageHeader, "1")
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			return w.Code
		}
		page := http.StatusOK
		if password != "" {
			page = http.StatusUnauthorized
			if got := do("GET", "/", "wrong", false); got != http.StatusUnauthorized {
				t.Errorf("password %q, wrong password: %d, want 401", password, got)
			}
		}
		for _, c := range []struct {
			method, path, pass string
			fromPage           bool
			want               int
		}{
			{"GET", "/", "", false, page},
			{"GET", "/", password, false, http.StatusOK},
			{"GET", "/health", "", false, http.StatusOK}, // no run yet: starting
			{"POST", "/api/sync", password, false, http.StatusForbidden},
			{"POST", "/api/sync", password, true, http.StatusOK},
		} {
			if got := do(c.method, c.path, c.pass, c.fromPage); got != c.want {
				t.Errorf("password %q: %s %s (password %q, from page %t): %d, want %d", password, c.method, c.path, c.pass, c.fromPage, got, c.want)
			}
		}
	}
}

func TestHealthOf(t *testing.T) {
	now := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	ago := func(d time.Duration) int64 { return now.Add(-d).UnixMilli() }
	ok := &Run{FinishedAt: ago(3 * time.Minute)}
	for _, c := range []struct {
		name string
		snap Snapshot
		want string
	}{
		{"just started", Snapshot{StartedAt: ago(time.Minute)}, "starting"},
		{"no run worked since the start", Snapshot{StartedAt: ago(20 * time.Minute)}, "stalled"},
		{"last run worked", Snapshot{StartedAt: ago(time.Hour), Runs: Runs{LastOK: ok}}, "ok"},
		{"last run failed", Snapshot{StartedAt: ago(time.Hour), Runs: Runs{LastOK: ok, FailingSince: ago(time.Minute)}}, "failing"},
		{"last good run long ago", Snapshot{StartedAt: ago(time.Hour), Runs: Runs{LastOK: &Run{FinishedAt: ago(30 * time.Minute)}}}, "stalled"},
		{"instant down", Snapshot{StartedAt: ago(time.Hour), Runs: Runs{LastOK: ok}, Instant: Instant{Enabled: true, State: "down", Since: ago(time.Minute)}}, "instant-down"},
		{"instant retrying a while", Snapshot{StartedAt: ago(time.Hour), Runs: Runs{LastOK: ok}, Instant: Instant{Enabled: true, State: "retrying", Since: ago(time.Minute)}}, "ok"},
		{"instant retrying too long", Snapshot{StartedAt: ago(time.Hour), Runs: Runs{LastOK: ok}, Instant: Instant{Enabled: true, State: "retrying", Since: ago(20 * time.Minute)}}, "instant-retrying"},
	} {
		h := HealthOf(c.snap, 5*time.Minute, now)
		if h.Status != c.want || h.OK != (c.want == "ok" || c.want == "starting") {
			t.Errorf("%s: got %s (ok %t, %s), want %s", c.name, h.Status, h.OK, h.Reason, c.want)
		}
	}
}
