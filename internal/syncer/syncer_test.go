package syncer

import (
	"testing"
	"time"

	"github.com/vksoftwares/vknex-dbsync/internal/config"
)

func TestDue(t *testing.T) {
	s := New(&config.Config{Interval: 5 * time.Minute, KindIntervals: map[string]time.Duration{"LOG": time.Hour}}, nil, nil)
	if !s.due("LOG") || !s.due("TRANSACTION") || !s.due("") {
		t.Fatal("everything is due before the first run")
	}
	for ago, want := range map[time.Duration]bool{
		5 * time.Minute:  false,
		55 * time.Minute: false,
		58 * time.Minute: true, // the run nearest to the hour
		65 * time.Minute: true,
	} {
		s.slowDone["LOG"] = time.Now().Add(-ago)
		if got := s.due("LOG"); got != want {
			t.Errorf("LOG moved %s ago: due = %v, want %v", ago, got, want)
		}
		if !s.due("TRANSACTION") {
			t.Errorf("TRANSACTION not due %s after a LOG run", ago)
		}
	}
}
