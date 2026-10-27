package logfile

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func read(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestRotate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.log")
	if err := os.WriteFile(path, []byte("before start\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	l, err := open(path, 40, 3, nil) // two lines of 18 bytes fit, a third does not
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	// Another writer on the same file, like systemd's StandardOutput=append:.
	other, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()

	for i := 1; i <= 9; i++ {
		fmt.Fprintf(l, "line %d of the log\n", i)
		if i == 7 {
			other.WriteString("from systemd\n")
		}
	}

	for name, want := range map[string]string{
		"":   "line 8 of the log\nline 9 of the log\n",
		".1": "line 6 of the log\nline 7 of the log\nfrom systemd\n",
		".2": "line 4 of the log\nline 5 of the log\n",
		".3": "line 2 of the log\nline 3 of the log\n", // "before start" and line 1 went beyond keep=3
	} {
		if got := read(t, path+name); got != want {
			t.Errorf("log%s %q, want %q", name, got, want)
		}
	}
	if _, err := os.Stat(path + ".4"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("log.4 kept beyond keep=3: %v", err)
	}
}

func TestRotateFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sync.log")
	if err := os.Mkdir(path+".1", 0o755); err != nil { // path.1 cannot be written
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path+".1", "x"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 10)
	l, err := open(path, 100, 1, func(err error) { errs <- err })
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	line := strings.Repeat("x", 59) + "\n"
	l.Write([]byte(line))
	l.Write([]byte(line)) // past max: the rotation fails, the line is still written
	if got := read(t, path); got != line+line {
		t.Fatalf("log %q, want both lines", got)
	}
	if err := <-errs; err == nil {
		t.Fatal("no error reported")
	}
	l.Write([]byte("short\n")) // within the next tenth: not tried again
	select {
	case err := <-errs:
		t.Fatalf("rotation tried again at once: %v", err)
	default:
	}
}
