# vknex-dbsync

One-way sync from the **local** PostgreSQL database (source of truth) to the **live** RDS database, so the live copy ends up identical to local.

It runs as its own small binary, separate from the NestJS backend:

- only this tool holds both connection strings,
- a heavy sync never slows down or crashes the live API,
- it can run once, on a schedule, or as a 24/7 worker,
- with `-instant`, master tables reach the live DB seconds after a change while transaction tables follow the schedule.

## Build

Requires Go 1.23+.

```bash
go mod tidy
go build -o vknex-dbsync ./cmd/vknex-dbsync                              # current OS
GOOS=windows GOARCH=amd64 go build -ldflags "-H=windowsgui" -o vknex-dbsync.exe ./cmd/vknex-dbsync  # Windows, double-click runs it in the background
GOOS=windows GOARCH=amd64 go build -o vknex-dbsync.exe ./cmd/vknex-dbsync  # Windows, console (for -dry-run in a terminal)
GOOS=linux   GOARCH=amd64 go build -o vknex-dbsync ./cmd/vknex-dbsync      # Linux / EC2
go test ./...
```

## Project layout

```
cmd/vknex-dbsync/     main: flags, logging, signal handling, -interval loop
internal/
  config/             Config: flags, environment variables, .env
  pg/                 low-level Postgres: connect, identifier/literal quoting, COPY streaming
  schema/             table model; catalog introspection, table selection, FK ordering, schema compare
  ddl/                -ddl: plans the CREATE/ALTER/DROP statements that make the target match the source
  syncer/             one sync run: snapshot, full / incremental copy, sequences, sync state, -mark-synced
```

Dependencies only point down: `cmd` → `syncer` → `ddl` → `schema` → `pg`, `config`.

## First-time setup

1. **Network**: the machine running the sync must reach RDS on port 5432. Either allow your office IP in the RDS security group, or open an SSH tunnel through your EC2 box and point `SYNC_TARGET_URL` at `localhost`:
   ```bash
   ssh -N -L 15432:<rds-endpoint>:5432 ec2-user@<ec2-ip>
   ```
2. **Schema**: both databases must have the same tables and column types. Either set `SYNC_DDL=true` and let the tool apply the source's structure (see [Schema changes](#schema-changes--ddl)), or run `npx prisma migrate deploy` against the live DB first. Without `-ddl` the tool refuses to run and lists every difference.
3. **Backup**: take an RDS snapshot.
4. Copy `.env.example` to `.env` and fill in both URLs.
5. Check the plan:
   ```bash
   ./vknex-dbsync -dry-run
   ```
6. Initial load (truncates the synced tables on the live DB and reloads them):
   ```bash
   ./vknex-dbsync -mode full -yes
   ```
7. From then on, incremental:
   ```bash
   ./vknex-dbsync                 # once
   ./vknex-dbsync -interval 5m    # keep running
   ```

## How it works

Every run is **one transaction on the target**: it either fully succeeds or changes nothing (Ctrl+C mid-run is safe). The source is read from a single consistent snapshot (`REPEATABLE READ`).

Data is streamed with PostgreSQL `COPY` straight from source to target; rows never sit in memory, so table size does not matter.

**Full mode**: `TRUNCATE` all synced tables, then `COPY` each one, parents before children.

**Incremental mode**:

| Step | What happens |
|---|---|
| 1. Stage | Rows changed since the last run (`updatedAt >= last_sync - overlap`) are copied into temp tables on the target. Tables without `updatedAt` are staged in full. If deletes are on, all source primary keys are staged too. |
| 2. Unique swaps | Target rows that are gone from the source and block an incoming row on a unique key (e.g. product code deleted and re-created) are removed first. |
| 3. Upsert | `INSERT ... ON CONFLICT DO UPDATE`, parents first. Unchanged rows are skipped (`IS DISTINCT FROM`), so re-reading the overlap window costs no writes. |
| 4. FK cycles | For tables that reference each other (e.g. `Branch.managerId` ↔ `Employee.branchId`), one nullable FK column is written as NULL in step 3 and filled in here. |
| 5. Deletes | Target rows whose primary key no longer exists in the source are deleted, children first. |

After the data, every serial / identity sequence on the target is moved past both the source sequence and the highest id, so new inserts never collide.

Sync state lives on the target in schema `vknex_sync`:

```sql
SELECT * FROM vknex_sync.run_log ORDER BY id DESC LIMIT 20;   -- history of runs
SELECT * FROM vknex_sync.table_state;                         -- watermark per table
```

Only one sync can write to a target at a time (advisory lock); a second process skips its run.

## Options

Every flag also reads an environment variable (or `.env`).

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `-source` | `SYNC_SOURCE_URL` | | Local DB URL |
| `-target` | `SYNC_TARGET_URL` | | Live DB URL (`?sslmode=require` for RDS) |
| `-mode` | `SYNC_MODE` | `incremental` | `incremental` or `full` |
| `-yes` | `SYNC_YES` | `false` | Required for `-mode full` |
| `-dry-run` | `SYNC_DRY_RUN` | `false` | Print plan and row counts only |
| `-interval` | `SYNC_INTERVAL` | `0` | Repeat every interval (`5m`, `1h`); `0` = run once |
| `-schema` | `SYNC_SCHEMA` | `public` | Comma-separated schemas to sync, e.g. `public,accounts,sales` |
| `-include` | `SYNC_INCLUDE` | all | Comma-separated tables to sync (`table` or `schema.table`) |
| `-exclude` | `SYNC_EXCLUDE` | `_prisma_migrations` | Comma-separated tables to skip (`table` or `schema.table`) |
| `-ddl` | `SYNC_DDL` | `false` | Apply the source's structure to the target first (see below) |
| `-ddl-drop` | `SYNC_DDL_DROP` | `false` | With `-ddl`: also drop target tables, partitions and columns the source no longer has |
| `-mark-synced` | `SYNC_MARK_SYNCED` | `false` | Set each table's sync column (`…_sync_date`) on both sides after a run (see below) |
| `-mark-txn` | `SYNC_MARK_TXN` | `true` | With `-mark-synced`, also set it on the `-txn-tables`; `false` = only the master tables |
| `-instant` | `SYNC_INSTANT` | `false` | Mirror every table not in `-txn-tables` within seconds of a change; needs `-interval` (see below) |
| `-txn-tables` | `SYNC_TXN_TABLES` | | With `-instant`: comma-separated tables (`table` or `schema.table`) synced only every `-interval` |
| `-updated-cols` | `SYNC_UPDATED_COLS` | `updatedAt,updated_at` | Change-tracking column names |
| `-overlap` | `SYNC_OVERLAP` | `5m` | Safety window re-read on each run |
| `-deletes` | `SYNC_DELETES` | `true` | Mirror deletes |
| `-disable-triggers` | `SYNC_DISABLE_TRIGGERS` | `false` | `session_replication_role = replica` on the target: skips FK checks and triggers (needs the RDS master user) |
| `-naive-tz` | `SYNC_NAIVE_TZ` | `UTC` | Time zone of `timestamp without time zone` change columns (Prisma stores UTC) |
| `-lock-timeout` | `SYNC_LOCK_TIMEOUT` | `30s` | Give up if a live table stays locked this long |
| `-meta-schema` | `SYNC_META_SCHEMA` | `vknex_sync` | Schema for sync state on the target |
| `-log-json` | `SYNC_LOG_JSON` | `false` | JSON log lines |
| `-log-file` | `SYNC_LOG_FILE` | stdout | Append the logs to this file; without a console (Windows background build) `vknex-dbsync.log` next to the binary |

## Schema changes (`-ddl`)

With `-ddl`, every run first makes the target's structure match the source, in the same transaction as the data:

| Object | What happens on the target |
|---|---|
| Schemas, enum types | Created; new enum labels added in the source's order |
| Functions / procedures | Created or replaced when the definition differs (never dropped) |
| Tables, partitions | Created, with columns, defaults, identity, generated columns and the primary key |
| Columns | Added, type changed (`USING col::newtype`), default and `NOT NULL` aligned |
| Constraints, indexes | Created, rebuilt when the definition differs, renamed when only the name differs, dropped when the source has none |
| With `-ddl-drop` | Tables, partitions and columns that are gone from the source are dropped, with their data |

Order: new structure first, then the data, then `NOT NULL`, constraints, indexes and foreign keys, so existing target rows cannot block them. Tables that were created or got new/retyped columns have all their rows compared on that run. New enum labels are committed on their own just before the run (PostgreSQL cannot use a label in the transaction that adds it).

Postgres does not always print a definition back the way it was written (`IN (...)` lists, nested `AND`s), so a definition that only *looks* different is rebuilt on an empty temp copy of the target table, inside a transaction that is rolled back, and compared there. A second run right after a sync plans no changes.

Not handled: views, triggers, grants, comments, domains and composite types. Triggers are left out on purpose: a source trigger that writes to a local-only table would break writes on the live DB.

Safety:

- `-dry-run` prints every statement it would run.
- The target user must **own** the tables it alters or drops. On a server where the tables belong to `postgres`, connect as `postgres` (or the owner).
- `-ddl-drop` refuses to run when a schema has tables on the target but none on the source (wrong source URL or schema name).
- Tables excluded with `-exclude` are never created, changed or dropped.

## Sync time (`-mark-synced`)

The source tracks its own changes: each table has a sync column (the first timestamp column ending in `sync_date`, `sync_on`, `synced_on` or `syncdate`, the same rule as the `public.v_sync_tables` view). A `zz_sync_mark_dirty` trigger empties it whenever a row changes, so `NULL` means "not on the live DB yet". Deletes are logged in `public.sync_deleted_rows` with `sdr_sync_date` `NULL`.

With `-mark-synced`, after the data:

1. On the target, in the sync transaction, every row with an empty sync column gets the sync time.
2. After the target commits, the same rows on the source get the **same** time — unless the row was edited after the run read it (its row version changed); it stays waiting for the next run. The `sync_deleted_rows` entries of synced tables, up to the run's snapshot, get `sdr_sync_date`.
3. Rows that break one of their table's `NOT VALID` checks are left waiting on both sides: PostgreSQL refuses any update of them until they are corrected.

With `-mark-txn=false` only the master tables (those not in `-txn-tables`) get the sync time, together with their `sync_deleted_rows` entries; the transaction tables keep `NULL` on both sides. With `-instant` that means every instant sync of a master table stamps its rows on the live DB in the same transaction and on the local DB right after, while the interval runs of the transaction tables stamp nothing.

The source stamp runs with triggers off (`session_replication_role = replica`) when the source user is a superuser, so it is not recorded as a change in the audit log. Only turn this on when the target is the real live database: on a test copy it would mark local rows as sent to live when they are not.

## Instant mirroring (`-instant`)

With `-instant` (and `-interval`), the synced tables are split in two:

- **Transaction tables**, listed in `-txn-tables` (bills, vouchers, stock movements, logs): synced by the interval runs, e.g. every 5 minutes.
- **Every other table** (masters, settings): inserts, updates and deletes reach the target a few seconds after they are committed locally.

How it works:

1. On start, the tool puts a statement trigger `zz_vknex_sync_notify` on every instant table of the **source**, calling `vknex_sync.notify_change()`. When a transaction that changed the table commits, the trigger sends the table's name on the `vknex_sync` channel (`pg_notify`). Tables that are moved to `-txn-tables` or excluded lose the trigger.
2. A listener on that channel waits 0.3 s for the rest of the save, then runs an incremental sync of just the tables named, in one transaction on the target. It has its own advisory lock, so it runs alongside an interval run.
3. On start, and after losing the source connection, every instant table is synced once to catch up on changes made while nothing was listening.
4. Schema changes (`-ddl`) stay with the interval runs. An interval run that has schema changes to apply moves **every** table, not just the transaction tables, so that the constraints, indexes and `NOT NULL` it adds after the data find every table current. Instant syncs pause while it runs (`instant sync paused` … `resumed`). An instant table whose structure changed locally waits for that run (logged as a warning). New tables get the trigger after it.
5. When one table's data makes a run fail, the tables are tried one at a time, in between new changes, so one table cannot hold up the others. Connection errors, lock timeouts and a pause retry the whole set: after 5 s, doubling up to 1 minute (a pause keeps checking every 5 s).

A change takes 0.3 s plus one sync of its table, about 9 round trips: well under a second on a local network, about 5.5 s over a 576 ms link.

Things to know:

- With `-disable-triggers` the target does not check foreign keys. Deleting a master row locally (after its transactions) removes it from the live DB at once, while the live transaction rows that point at it stay until the next interval run deletes them. Without `-disable-triggers` that delete fails and is retried until the interval run has removed them.
- Statement triggers on a partitioned table do not fire for statements on a partition, so keep partitioned tables in `-txn-tables`.
- `vknex_sync.run_log` gets a row for every instant run (`mode = 'instant'`).
- To take the triggers off the local database: `DROP SCHEMA vknex_sync CASCADE;` on the source.

## Running it permanently

**Linux (systemd)**: `/etc/systemd/system/vknex-dbsync.service`

```ini
[Unit]
Description=VK Nex local -> live DB sync
After=network-online.target

[Service]
WorkingDirectory=/opt/vknex-dbsync
ExecStart=/opt/vknex-dbsync/vknex-dbsync -interval 5m
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
```

**Linux desktop, click to start**: run `deploy/linux/install.sh` once. It builds the binary, adds a systemd user service (`~/.config/systemd/user/vknex-dbsync.service`, settings from `.env`, logs appended to `vknex-dbsync.log`, restarted 30 s after a crash) and a "VK Nex DB Sync" icon on the desktop and in the app grid. Click the icon to start the sync in the background; right-click it for Stop and Show log. `systemctl --user enable vknex-dbsync.service` also starts it at every login.

When the live database is only reachable through SSH, pass the `~/.ssh/config` host whose `LocalForward` opens the tunnel: `deploy/linux/install.sh erp-vps`. That adds `vknex-dbsync-tunnel.service` (`ssh -N erp-vps`, reconnected 10 s after it drops), which starts and stops with the sync. Point `SYNC_TARGET_URL` at the forwarded local port (e.g. `127.0.0.1:5433`), not at `127.0.0.1:5432`: that is this machine's own Postgres, and the sync fails with `database "erp_rehearsal" does not exist`.

**Windows, double-click**: build with `-ldflags "-H=windowsgui"` (see Build) and put `vknex-dbsync.exe` and `.env` (with `SYNC_INTERVAL=5m`) in one folder. Double-clicking the exe starts it in the background with no window:

- logs go to `vknex-dbsync.log` in that folder; startup errors (bad `.env`) pop up in a message box,
- a second double-click only reports that it is already running,
- to start it with Windows: `Win+R` → `shell:startup` → put a shortcut to the exe there,
- to stop it: Task Manager → Details → `vknex-dbsync.exe` → End task, or `taskkill /IM vknex-dbsync.exe /F`. An open sync transaction is rolled back by the server.

**Windows, other ways**: Task Scheduler running `vknex-dbsync.exe` every 5 minutes (without `-interval`), or wrap the console build `vknex-dbsync.exe -interval 5m` as a service with NSSM.

**PM2** (if you already use it): `pm2 start ./vknex-dbsync --name dbsync -- -interval 5m`

## Things to know

- **One-way only.** Anything written directly on the live DB to a synced table is overwritten or deleted on the next run. If live users also enter data, exclude those tables or use a different design.
- **Changes are found through `updatedAt`.** Prisma's `@updatedAt` sets it on every update through Prisma. Raw SQL updates that don't touch `updatedAt` are not picked up until the next full run. Tables without an `updatedAt` column are compared in full every run.
- **Delete detection reads every primary key** of every table on each run. That is cheap for most tables; for very large ones run with `-deletes=false` on the frequent schedule and a second job with deletes on, e.g. hourly.
- **Tables without a primary key** are deleted and reloaded on every run.
- **`GENERATED ALWAYS AS IDENTITY`** values are copied on insert but never updated (PostgreSQL forbids it). `GENERATED ... STORED` columns are skipped and recomputed by the target.
- **Triggers on the target fire** during the sync unless `-disable-triggers` is set. Triggers that check one row against others (e.g. "name already used") can fire on a half-applied state; use `-disable-triggers` for a true mirror.
- **`NOT VALID` constraints**: source rows may legitimately break them. When a written row does, the target's copy is dropped for the run and re-added `NOT VALID` after the data.
- **Same records under different ids.** When the live DB was filled separately (seed data, ids from its own sequences), its rows collide with the source's on unique keys and `EXCLUDE` constraints (including partial, expression and `NULLS NOT DISTINCT` indexes). Target rows that are gone from the source are removed first; with `-disable-triggers`, rows both sides have that hold each other's unique values are deleted and written back in the same step.
- **Network latency dominates.** Every statement is one round trip; over a 260 ms link a run over ~190 tables takes 5–7 minutes even when little changed.
- The first incremental run without a previous watermark stages every row in temp tables on RDS. Use `-mode full -yes` for the initial load instead.

## Tested scenarios

Checked against two PostgreSQL 16 databases with a Prisma-style schema (quoted camelCase names, enums, enum arrays, `jsonb`, `json`, `numeric`, `text[]`, serial and identity columns, a stored generated column, composite primary key, self-referencing FK, a two-table FK cycle, a table without a primary key, a table without `updatedAt`), ~450k rows. After each run, per-table checksums of source and target matched for:

- initial sync onto a target holding stale and extra rows
- updates, inserts, deletes with `ON DELETE CASCADE`
- product code deleted and re-created under a new id (unique-key swap)
- children moved to a new category, then the old category deleted
- new rows inside an FK cycle, and deletion of rows inside the cycle
- full reload over a drifted target, and `-disable-triggers`

`-ddl -ddl-drop` was checked on PostgreSQL 18 against an older copy of the VK Nex ERP database (8 schemas, 188 tables, 51 partitioned tables with ~1,900 partitions, enums, generated columns, ~4,000 constraints): one run applied 592 changes (new tables and partitions, added and retyped columns, rebuilt checks and indexes, dropped extra tables, columns and indexes, a renamed index), after which tables, columns, defaults, constraints, indexes, partitions, enums, functions and sequences matched the source exactly, the data of all 188 tables matched, and a second run planned no changes.
- Ctrl+C during a full reload (target unchanged), schema drift and same-database protection, interval loop with clean shutdown
- `-instant` on a copy of the VK Nex ERP database (117 instant, 71 transaction tables): master inserts, updates and deletes (including `item_master`, 10k rows with four expression unique indexes) arrived in 0.5–1.1 s on localhost, a `sale_bill` edit waited for the interval run, a column added to a master table and a new master table were applied by the interval run and then filled by the instant sync, the first start on a target without `vknex_sync`, and Ctrl+C with no sessions left behind. A full run afterwards found nothing to write.
