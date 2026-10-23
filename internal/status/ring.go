package status

import (
	"bytes"
	"io"
	"os"
	"strings"
	"sync"
)

// Ring is an io.Writer that keeps the last lines written to it, so the status
// page can show the latest log lines without reading the log file.
type Ring struct {
	mu      sync.Mutex
	max     int
	lines   []string
	partial []byte // a line cut across writes
}

// NewRing keeps the last max lines.
func NewRing(max int) *Ring { return &Ring{max: max} }

// Write keeps the complete lines of p; it never fails.
func (r *Ring) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	buf := append(r.partial, p...)
	for {
		i := bytes.IndexByte(buf, '\n')
		if i < 0 {
			break
		}
		r.add(string(buf[:i]))
		buf = buf[i+1:]
	}
	r.partial = append(r.partial[:0], buf...)
	return len(p), nil
}

// Add keeps one line.
func (r *Ring) Add(line string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.add(line)
}

func (r *Ring) add(line string) {
	line = strings.TrimRight(line, "\r")
	if line == "" {
		return
	}
	r.lines = append(r.lines, line)
	if len(r.lines) > r.max {
		r.lines = r.lines[len(r.lines)-r.max:]
	}
}

// Lines returns a copy of the last n lines, oldest first.
func (r *Ring) Lines(n int) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n <= 0 || n > len(r.lines) {
		n = len(r.lines)
	}
	out := make([]string, n)
	copy(out, r.lines[len(r.lines)-n:])
	return out
}

// SeedFromFile adds the last n lines of a log file, so that the page also
// shows what happened before this start. A missing file is fine.
func (r *Ring) SeedFromFile(path string, n int) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	const chunk = 256 << 10
	start := info.Size() - chunk
	if start < 0 {
		start = 0
	}
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		return err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	lines := strings.Split(string(data), "\n")
	if start > 0 && len(lines) > 0 {
		lines = lines[1:] // cut mid-line
	}
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, line := range lines {
		r.add(line)
	}
	return nil
}
