// vknex-dbsync mirrors a local PostgreSQL database into the live (RDS) database.
//
// It runs as its own process, separate from the NestJS backend: only this tool
// knows both connection strings, and a long sync can never slow the live API.
//
//	vknex-dbsync -dry-run                    # show what would happen
//	vknex-dbsync                             # one incremental sync
//	vknex-dbsync -interval 5m                # keep syncing every 5 minutes
//	vknex-dbsync -mode full -yes             # truncate + reload every table
//	vknex-dbsync -mail-test                  # check the -mail-to settings with a test mail
//	vknex-dbsync -interval 5m -instant       # by dtm_kind in public.db_table_master: masters, settings,
//	                                         # ledgers, balances within seconds, LOG every hour,
//	                                         # TRANSACTION and the rest every 5 minutes
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/vksoftwares/vknex-dbsync/internal/config"
	"github.com/vksoftwares/vknex-dbsync/internal/notify"
	"github.com/vksoftwares/vknex-dbsync/internal/pg"
	"github.com/vksoftwares/vknex-dbsync/internal/status"
	"github.com/vksoftwares/vknex-dbsync/internal/syncer"
)

func main() {
	// The Windows build made with -H=windowsgui has no console when it is
	// double-clicked: it logs to a file and shows startup errors in a message box.
	background := !usable(os.Stdout)
	cfg, err := config.Load(os.Args[1:])
	if err != nil {
		fatal(background, "config: "+err.Error(), 2)
	}
	if background && alreadyRunning() {
		// A second double-click opens the status page of the running copy.
		if cfg.UI != "" && cfg.Interval > 0 && openBrowser(status.URL(cfg.UI)) == nil {
			return
		}
		fatal(background, "vknex-dbsync is already running in the background.\n\nTo stop it: Task Manager > Details > vknex-dbsync.exe > End task.", 0)
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
	st := status.New() // what the status page (-ui) shows; it also keeps the latest log lines
	if logFile != "" {
		st.Log.SeedFromFile(logFile, 300) // what happened before this start
	}
	out = io.MultiWriter(out, st.Log)

	var handler slog.Handler = slog.NewTextHandler(out, &slog.HandlerOptions{Level: slog.LevelInfo})
	if cfg.LogJSON {
		handler = slog.NewJSONHandler(out, nil)
	}
	log := slog.New(handler)

	n := notify.New(cfg, logFile, log) // nil without -mail-to
	defer mailCrash(log, n)
	if cfg.MailTest {
		if err := n.Test(); err != nil {
			fatal(background, "mail test: "+err.Error(), 2)
		}
		log.Info("test mail sent", "to", strings.Join(cfg.MailTo, ","))
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("vknex-dbsync starting",
		"source", pg.Describe(cfg.SourceURL),
		"target", pg.Describe(cfg.TargetURL),
		"mode", cfg.Mode,
		"schemas", strings.Join(cfg.Schemas, ","),
		"deletes", cfg.Deletes,
		"ddl", cfg.DDL,
		"ddl_drop", cfg.DDLDrop,
		"mark_synced", cfg.MarkSynced,
		"mark_txn", cfg.MarkTxn,
		"interval", cfg.Interval.String(),
		"instant", cfg.Instant,
		"table_master", cfg.TableMaster,
		"instant_kinds", strings.Join(cfg.InstantKinds, ","),
		"kind_intervals", kindIntervals(cfg),
		"two_way", cfg.TwoWay,
		"two_way_kinds", strings.Join(cfg.TwoWayKinds, ","),
		"two_way_wins", cfg.TwoWayWins,
		"two_way_live_ids", cfg.TwoWayLiveIDs,
		"mail_to", strings.Join(cfg.MailTo, ","))

	if cfg.TableMaster != "" && len(cfg.TxnTables) > 0 {
		log.Warn("SYNC_TXN_TABLES is ignored: "+cfg.TableMaster+" decides which tables are instant; remove it from .env", "tables", len(cfg.TxnTables))
	}

	s := syncer.New(cfg, log, n)
	s.Status = st

	if cfg.Interval <= 0 {
		if err := s.RunOnce(ctx); err != nil {
			log.Error("sync failed", "err", err)
			n.Failed("sync", err)
			os.Exit(1)
		}
		return
	}

	if cfg.Mode == "full" {
		log.Warn("full mode with -interval truncates and reloads the target on every run")
	}
	if cfg.UI != "" && !cfg.DryRun {
		if url, err := status.Start(ctx, cfg.UI, st, cfg, logFile, log); err != nil {
			log.Warn("status page not available", "addr", cfg.UI, "err", err)
		} else {
			log.Info("status page", "url", url)
		}
	}
	if cfg.Instant && !cfg.DryRun {
		st.InstantEnabled()
		watched := make(chan struct{})
		go func() {
			defer close(watched)
			defer mailCrash(log, n)
			s.Watch(ctx)
		}()
		// let a running instant sync roll back before exiting; stop() first so
		// that a panic below (ctx still live) does not wait for it forever
		defer func() { stop(); <-watched }()
	}
	stopped := func() {
		log.Info("stopped")
		n.Stopped("It was told to stop: Ctrl-C, systemctl stop, the desktop icon's Stop or Task Manager > End task. Nothing is synced until it is started again.")
	}
	for {
		runStart := time.Now()
		if err := s.RunOnce(ctx); err != nil {
			if ctx.Err() != nil {
				stopped()
				return
			}
			log.Error("sync failed; will retry next interval", "err", err)
			n.Failed("sync", err)
		} else {
			n.Recovered("sync")
		}
		wait := cfg.Interval - time.Since(runStart)
		if wait < time.Second {
			wait = time.Second
		}
		st.NextRun(time.Now().Add(wait))
		select {
		case <-ctx.Done():
			stopped()
			return
		case <-time.After(wait):
		case <-st.Trigger():
			log.Info("sync started from the status page")
		}
	}
}

// mailCrash, deferred in main and in the goroutines it starts, logs and mails
// a panic before ending the program. Exit code 1, not the 2 of an unrecovered
// panic: 2 means a bad .env, and the systemd unit does not restart after it.
func mailCrash(log *slog.Logger, n *notify.Notifier) {
	r := recover()
	if r == nil {
		return
	}
	stack := string(debug.Stack())
	log.Error("crashed", "panic", r, "stack", stack)
	n.Crashed(r, stack)
	os.Exit(1)
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

// kindIntervals prints -kind-intervals, e.g. "LOG:1h0m0s".
func kindIntervals(cfg *config.Config) string {
	var out []string
	for _, kind := range slices.Sorted(maps.Keys(cfg.KindIntervals)) {
		out = append(out, kind+":"+cfg.KindIntervals[kind].String())
	}
	return strings.Join(out, ",")
}
