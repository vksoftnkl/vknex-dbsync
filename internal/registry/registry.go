// Package registry keeps this client's row in the client list on the live
// server: public.client_master in the -client-db database, next to the target.
package registry

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/vksoftwares/vknex-dbsync/internal/config"
	"github.com/vksoftwares/vknex-dbsync/internal/pg"
)

// Save adds -client-name with the target's database name to client_master, or
// renames the client when the database is listed under another name. A row
// that is already right is left alone, so its updated_on keeps the last change.
// It returns the database name.
func Save(ctx context.Context, cfg *config.Config) (string, error) {
	tc, err := pgx.ParseConfig(cfg.TargetURL)
	if err != nil {
		return "", err
	}
	conn, err := pg.ConnectTo(ctx, cfg.TargetURL, cfg.ClientDB)
	if err != nil {
		return "", fmt.Errorf("connect %s: %w", cfg.ClientDB, err)
	}
	defer conn.Close(context.Background())

	_, err = conn.Exec(ctx, `
INSERT INTO public.client_master (client_name, db_name) VALUES ($1, $2)
ON CONFLICT (db_name) DO UPDATE SET client_name = EXCLUDED.client_name
WHERE client_master.client_name IS DISTINCT FROM EXCLUDED.client_name`, cfg.ClientName, tc.Database)
	if err != nil {
		return "", fmt.Errorf("save in %s.public.client_master: %w", cfg.ClientDB, err)
	}
	return tc.Database, nil
}
