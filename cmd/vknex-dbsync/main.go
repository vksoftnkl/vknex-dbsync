// vknex-dbsync mirrors a local PostgreSQL database into the live (RDS) database.
//
// It runs as its own process, separate from the NestJS backend: only this tool
// knows both connection strings, and a long sync can never slow the live API.
//
//	vknex-dbsync -dry-run                    # show what would happen
//	vknex-dbsync                             # one incremental sync
//	vknex-dbsync -interval 5m                # keep syncing every 5 minutes
//	vknex-dbsync -mode full -yes             # truncate + reload every table
//	vknex-dbsync -interval 5m -instant -txn-tables sales.sale_bill,...
//	                                         # those every 5 minutes, all other tables within seconds
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/vksoftwares/vknex-dbsync/internal/config"
	"github.com/vksoftwares/vknex-dbsync/internal/syncer"
)

func main() {
	// The Windows build made with -H=windowsgui has no console when it is
	// double-clicked: it logs to a file and shows startup errors in a message box.
	background := !usable(os.Stdout)
	if background && alreadyRunning() {
		fatal(background, "vknex-dbsync is already running in the background.\n\nTo stop it: Task Manager > Details > vknex-dbsync.exe > End task.", 0)
	}

	cfg, err := config.Load(os.Args[1:])
	if err != nil {
		fatal(background, "config: "+err.Error(), 2)
	}

	out := io.Writer(os.Stdout)
	logFile := cfg.LogFile
	if logFile == "" && background {
		logFile = besideBinary("vknex-dbsync.log")
	}
	if logFile != "" {
		f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			fatal(background, "log file: "+err.Error(), 2)
		}
		out = f
	}

	var handler slog.Handler = slog.NewTextHandler(out, &slog.HandlerOptions{Level: slog.LevelInfo})
	if cfg.LogJSON {
		handler = slog.NewJSONHandler(out, nil)
	}
	log := slog.New(handler)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("vknex-dbsync starting",
		"source", describe(cfg.SourceURL),
		"target", describe(cfg.TargetURL),
		"mode", cfg.Mode,
		"schemas", strings.Join(cfg.Schemas, ","),
		"deletes", cfg.Deletes,
		"ddl", cfg.DDL,
		"ddl_drop", cfg.DDLDrop,
		"mark_synced", cfg.MarkSynced,
		"mark_txn", cfg.MarkTxn,
		"interval", cfg.Interval.String(),
		"instant", cfg.Instant)

	s := syncer.New(cfg, log)

	if cfg.Interval <= 0 {
		if err := s.RunOnce(ctx); err != nil {
			log.Error("sync failed", "err", err)
			os.Exit(1)
		}
		return
	}

	if cfg.Mode == "full" {
		log.Warn("full mode with -interval truncates and reloads the target on every run")
	}
	if cfg.Instant && !cfg.DryRun {
		watched := make(chan struct{})
		go func() {
			defer close(watched)
			s.Watch(ctx)
		}()
		defer func() { <-watched }() // let a running instant sync roll back before exiting
	}
	for {
		runStart := time.Now()
		if err := s.RunOnce(ctx); err != nil {
			if ctx.Err() != nil {
				log.Info("stopped")
				return
			}
			log.Error("sync failed; will retry next interval", "err", err)
		}
		wait := cfg.Interval - time.Since(runStart)
		if wait < time.Second {
			wait = time.Second
		}
		select {
		case <-ctx.Done():
			log.Info("stopped")
			return
		case <-time.After(wait):
		}
	}
}

// usable reports whether f can be written to. A GUI-subsystem process started
// from Explorer has no standard handles.
func usable(f *os.File) bool {
	if f == nil {
		return false
	}
	_, err := f.Stat()
	return err == nil
}

// fatal prints msg, also in a message box when nobody would see stderr, and exits.
func fatal(background bool, msg string, code int) {
	fmt.Fprintln(os.Stderr, msg)
	if background {
		alert(msg)
	}
	os.Exit(code)
}

// besideBinary returns name in the binary's directory, which is not always the
// working directory (Startup folder shortcuts, Task Scheduler).
func besideBinary(name string) string {
	exe, err := os.Executable()
	if err != nil {
		return name
	}
	return filepath.Join(filepath.Dir(exe), name)
}

// describe prints host:port/db without the password.
func describe(url string) string {
	c, err := pgx.ParseConfig(url)
	if err != nil {
		return "(unparseable URL)"
	}
	return fmt.Sprintf("%s@%s:%d/%s", c.User, c.Host, c.Port, c.Database)
}
