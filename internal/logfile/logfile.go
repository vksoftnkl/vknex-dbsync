// Package logfile appends the log to a file that is rotated by size: when it
// passes MaxSize, its content moves to path.1 (path.1 to path.2, and so on,
// Keep files in all) and the file starts again empty.
//
// The content is copied out and the file truncated, not renamed: the systemd
// unit holds the same file open (StandardOutput=append:) and the desktop's
// "Show log" follows it with tail -f, and both keep working on the same file.
// Every writer appends (O_APPEND), so after the truncation they all write from
// the start again. Windows cannot rename a file that is open; it can do this.
package logfile

import (
	"fmt"
	"io"
	"os"
	"sync"
)

const (
	MaxSize = 10 << 20 // bytes in the file before it is rotated
	Keep    = 5        // rotated files kept: path.1 (newest) to path.5
)

// File is the log file. It is safe for concurrent use.
type File struct {
	path    string
	max     int64
	keep    int
	onError func(error) // a failed rotation, called on its own goroutine

	mu   sync.Mutex
	f    *os.File
	size int64 // written so far, as far as this process knows
	next int64 // size at which the next rotation is tried
}

// Open opens path for appending, creating it if needed. onError, if not nil,
// hears of a rotation that failed; the log then goes on growing and the
// rotation is tried again after another tenth of MaxSize. It is called on a
// goroutine of its own, so it may write to the log.
func Open(path string, onError func(error)) (*File, error) {
	return open(path, MaxSize, Keep, onError)
}

func open(path string, max int64, keep int, onError func(error)) (*File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &File{path: path, max: max, keep: keep, onError: onError, f: f, size: info.Size(), next: max}, nil
}

// Write appends p, rotating the file first when p would take it past MaxSize.
func (l *File) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.size > 0 && l.size+int64(len(p)) > l.next {
		if err := l.rotate(); err != nil {
			l.next = l.size + l.max/10
			if l.onError != nil {
				go l.onError(fmt.Errorf("rotate %s: %w", l.path, err))
			}
		} else {
			l.size, l.next = 0, l.max
		}
	}
	n, err := l.f.Write(p)
	l.size += int64(n)
	return n, err
}

// rotate moves path.1 … path.(keep-1) one up, copies the file to path.1 and
// empties it.
func (l *File) rotate() error {
	for i := l.keep - 1; i >= 1; i-- {
		err := os.Rename(fmt.Sprintf("%s.%d", l.path, i), fmt.Sprintf("%s.%d", l.path, i+1))
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	if err := copyFile(l.path, l.path+".1"); err != nil {
		return err
	}
	return l.f.Truncate(0)
}

func copyFile(from, to string) error {
	src, err := os.Open(from)
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := os.Create(to)
	if err != nil {
		return err
	}
	if _, err := io.Copy(dst, src); err != nil {
		dst.Close()
		return err
	}
	return dst.Close()
}

// Close closes the file.
func (l *File) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f.Close()
}
