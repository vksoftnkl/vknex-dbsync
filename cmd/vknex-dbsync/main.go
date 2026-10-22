// vknex-dbsync mirrors a local PostgreSQL database into the live (RDS) database.
//
// It runs as its own process, separate from the NestJS backend: only this tool
// knows both connection strings, and a long sync can never slow the live API.
//
//	vknex-dbsync -dry-run                    # show what would happen
//	vknex-dbsync                             # one incremental sync
//	vknex-dbsync -interval 5m                # keep syncing every 5 minutes
//	vknex-dbsync -mode full -yes             # truncate + reload every table
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/vksoftwares/vknex-dbsync/internal/config"
	"github.com/vksoftwares/vknex-dbsync/internal/syncer"
)

func main() {
	cfg, err := config.Load(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(2)
	}

	var handler slog.Handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	if cfg.LogJSON {
		handler = slog.NewJSONHandler(os.Stdout, nil)
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
		"interval", cfg.Interval.String())

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

// describe prints host:port/db without the password.
func describe(url string) string {
	c, err := pgx.ParseConfig(url)
	if err != nil {
		return "(unparseable URL)"
	}
	return fmt.Sprintf("%s@%s:%d/%s", c.User, c.Host, c.Port, c.Database)
}
