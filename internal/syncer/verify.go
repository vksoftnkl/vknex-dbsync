package syncer

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/vksoftwares/vknex-dbsync/internal/pg"
	"github.com/vksoftwares/vknex-dbsync/internal/schema"
)

// Verify compares every synced table with its copy on the target, by row
// count and a checksum of all its rows (fingerprintSQL), and logs each one
// that differs. It changes nothing, and reads each side in one snapshot with
// one batch of queries. A table with changes the sync has not moved yet
// differs until the next run, and a two-way table while live changes wait to
// be pulled. It returns the number of tables that differ.
func (s *Syncer) Verify(ctx context.Context) (int, error) {
	started := time.Now()
	cfg := s.cfg

	src, err := pg.Connect(ctx, cfg.SourceURL)
	if err != nil {
		return 0, fmt.Errorf("connect source: %w", err)
	}
	defer src.Close(context.Background())
	dst, err := pg.Connect(ctx, cfg.TargetURL)
	if err != nil {
		return 0, fmt.Errorf("connect target: %w", err)
	}
	defer dst.Close(context.Background())
	if err := guardDifferent(ctx, src, dst); err != nil {
		return 0, err
	}

	srcTx, _, err := beginSnapshot(ctx, src)
	if err != nil {
		return 0, err
	}
	defer srcTx.Rollback(context.Background())
	var srcAll map[string]*schema.Table
	if err := schema.CatalogPath(ctx, srcTx, func() error {
		var err error
		srcAll, err = schema.Load(ctx, srcTx, cfg.Schemas)
		return err
	}); err != nil {
		return 0, fmt.Errorf("source schema: %w", err)
	}
	master, err := schema.LoadMaster(ctx, srcTx, cfg)
	if err != nil {
		return 0, err
	}
	tables, _ := schema.Select(srcAll, cfg, master)
	if len(tables) == 0 {
		return 0, errors.New("no tables selected; check -schema / -include / -exclude")
	}
	dstAll, _, err := readTarget(ctx, dst, cfg)
	if err != nil {
		return 0, err
	}

	differ := 0
	var probes []probe
	for _, name := range slices.Sorted(maps.Keys(tables)) {
		t := tables[name]
		if problems, _ := schema.Compare(map[string]*schema.Table{name: t}, dstAll); len(problems) > 0 {
			differ++
			s.log.Warn("table differs", "table", name, "structure", strings.Join(problems, "; "))
			continue
		}
		probes = append(probes, probe{name: name, sql: fingerprintSQL(t, t.ColNames())})
	}

	b := &pgx.Batch{}
	b.Queue(exactFloats)
	for i := range probes {
		b.Queue(probes[i].sql).QueryRow(func(row pgx.Row) error { return row.Scan(&probes[i].want) })
	}
	if err := srcTx.SendBatch(ctx, b).Close(); err != nil {
		return 0, fmt.Errorf("checksum the source: %w", err)
	}

	dstTx, err := dst.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return 0, err
	}
	defer dstTx.Rollback(context.Background())
	got := make([]string, len(probes))
	b = &pgx.Batch{}
	b.Queue(exactFloats)
	for i := range probes {
		b.Queue(probes[i].sql).QueryRow(func(row pgx.Row) error { return row.Scan(&got[i]) })
	}
	if err := dstTx.SendBatch(ctx, b).Close(); err != nil {
		return 0, fmt.Errorf("checksum the target: %w", err)
	}

	for i, p := range probes {
		if got[i] == p.want {
			continue
		}
		differ++
		srcRows, dstRows := rowsOf(p.want), rowsOf(got[i])
		detail := "rows differ"
		if srcRows == dstRows {
			detail = "same row count, different content"
		}
		s.log.Warn("table differs", "table", p.name, "source_rows", srcRows, "target_rows", dstRows, "detail", detail)
	}
	s.log.Info("verify complete", "tables", len(tables), "same", len(tables)-differ, "differ", differ,
		"took", time.Since(started).Round(time.Millisecond).String())
	return differ, nil
}

// rowsOf is the row count in a fingerprint (fingerprintSQL).
func rowsOf(fingerprint string) string {
	n, _, _ := strings.Cut(fingerprint, ":")
	return n
}
