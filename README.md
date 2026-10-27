# vknex-dbsync

One-way sync from the **local** PostgreSQL database (source of truth) to the **live** RDS database, so the live copy ends up identical to local.

It runs as its own small binary, separate from the NestJS backend:

- only this tool holds both connection strings,
- a heavy sync never slows down or crashes the live API,
- it can run once, on a schedule, or as a 24/7 worker,
- with `-instant`, masters, settings, ledgers and balances reach the live DB seconds after a change, transactions every 5 minutes and logs every hour; `public.db_table_master` decides which table is which,
- with `-two-way`, the master tables may be edited on the live DB too: those changes come back to the local DB.

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
  notify/             -mail-to: mails when a sync fails, works again or the program stops
  registry/           -client-name: this client's row in the client list on the live server
  status/             -ui: what the runs report, the status page (embedded HTML) and its small HTTP API
  syncer/             one sync run: snapshot, full / incremental copy, sequences, sync state, -mark-synced, -two-way pull
```

Dependencies only point down: `cmd` → `syncer` → `ddl` → `schema` → `pg`, `config`; `notify`, `registry` and `status` sit beside `ddl` and need only `pg` and `config` (`syncer` reports into `status`, `cmd` serves it).

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
| `-mark-synced` | `SYNC_MARK_SYNCED` | `false` | Set each table's sync column (`dtm_sync_col` in `db_table_master`) on both sides after a run (see below) |
| `-mark-txn` | `SYNC_MARK_TXN` | `true` | With `-mark-synced`, also set it on the interval tables; `false` = only the instant (master) tables |
| `-instant` | `SYNC_INSTANT` | `false` | Mirror the `-instant-kinds` tables within seconds of a change; needs `-interval` (see below) |
| `-table-master` | `SYNC_TABLE_MASTER` | `public.db_table_master` | Source table listing the tables by `dtm_kind`, with their sync column in `dtm_sync_col`, read on every run (see below); empty = use `-txn-tables` |
| `-instant-kinds` | `SYNC_INSTANT_KINDS` | `MASTER,SETTING,METADATA,LEDGER,BALANCE,SYSTEM` | `dtm_kind` values mirrored within seconds |
| `-kind-intervals` | `SYNC_KIND_INTERVALS` | `LOG:1h` | `kind:interval` for kinds the interval runs move less often than `-interval` |
| `-txn-tables` | `SYNC_TXN_TABLES` | | Only with an empty `-table-master`: comma-separated tables (`table` or `schema.table`) synced every `-interval`, all others instant |
| `-two-way` | `SYNC_TWO_WAY` | `false` | Also bring rows inserted, changed or deleted on the live DB in the `-two-way-kinds` tables back to the local DB; needs `-mark-synced` (see [Two-way master tables](#two-way-master-tables--two-way)) |
| `-two-way-kinds` | `SYNC_TWO_WAY_KINDS` | `MASTER` | `dtm_kind` values synced both ways |
| `-two-way-wins` | `SYNC_TWO_WAY_WINS` | `local` | A row changed on both sides since the last sync: `local` keeps the local version, `live` takes the live one |
| `-two-way-live-ids` | `SYNC_TWO_WAY_LIVE_IDS` | `0` | Rows created on the live DB in the two-way tables get ids from this number on (e.g. `1000000000`), so they never collide with local ids; `0` = same sequences as local |
| `-updated-cols` | `SYNC_UPDATED_COLS` | `updatedAt,updated_at` | Change-tracking column names |
| `-overlap` | `SYNC_OVERLAP` | `5m` | Safety window re-read on each run |
| `-deletes` | `SYNC_DELETES` | `true` | Mirror deletes |
| `-disable-triggers` | `SYNC_DISABLE_TRIGGERS` | `false` | `session_replication_role = replica` on the target: skips FK checks and triggers (needs the RDS master user) |
| `-naive-tz` | `SYNC_NAIVE_TZ` | `UTC` | Time zone of `timestamp without time zone` change columns (Prisma stores UTC) |
| `-lock-timeout` | `SYNC_LOCK_TIMEOUT` | `30s` | Give up if a live table stays locked this long |
| `-meta-schema` | `SYNC_META_SCHEMA` | `vknex_sync` | Schema for sync state on the target |
| `-log-json` | `SYNC_LOG_JSON` | `false` | JSON log lines |
| `-log-file` | `SYNC_LOG_FILE` | stdout | Append the logs to this file; without a console (Windows background build) `vknex-dbsync.log` next to the binary |
| `-ui` | `SYNC_UI` | `127.0.0.1:7645` | With `-interval`: serve the [status page](#status-page--ui) at this `host:port`; empty = none |
| `-mail-to` | `SYNC_MAIL_TO` | | Comma-separated addresses mailed when a sync fails, works again or the program stops (see below); empty = no mail |
| `-smtp` | `SYNC_SMTP` | | Mail server `host:port`, e.g. `smtp.gmail.com:587` |
| `-smtp-user` | `SYNC_SMTP_USER` | | Mail server login; empty = none |
| `-smtp-password` | `SYNC_SMTP_PASSWORD` | | Mail server password (Gmail: an app password) |
| `-mail-from` | `SYNC_MAIL_FROM` | `-smtp-user` | Sender address |
| `-mail-repeat` | `SYNC_MAIL_REPEAT` | `1h` | While a sync keeps failing, mail again at most this often; `0` = once per outage |
| `-mail-test` | `SYNC_MAIL_TEST` | `false` | Send a test mail to `-mail-to` and exit |
| `-client-name` | `SYNC_CLIENT_NAME` | | On start, save this client in the client list on the live server (see [below](#client-list--client-name)); empty = not saved |
| `-client-db` | `SYNC_CLIENT_DB` | `vknex_clients` | Database on the live server holding the client list (`public.client_master`) |

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

The source tracks its own changes: each table has a sync column, named in `dtm_sync_col` of `public.db_table_master` (e.g. `abj_sync_date` for `accounts.acc_bill_adjustment`). A `zz_sync_mark_dirty` trigger empties it whenever a row changes, so `NULL` means "not on the live DB yet". Deletes are logged in `public.sync_deleted_rows` with `sdr_sync_date` `NULL`.

The tool reads `dtm_sync_col` in every run's snapshot, next to `dtm_kind`, so a column filled in or changed there is used from the next run on, without a restart. A table the master does not list, or lists with an empty `dtm_sync_col`, falls back to the rule of the `public.v_sync_tables` view: the first timestamp column ending in `sync_date`, else `sync_on`, `synced_on` or `syncdate`; with `-table-master ""` every table does. A `dtm_sync_col` that is not a timestamp column of its table is logged as a warning (`msg="sync column"`) and that rule is used instead. `-dry-run` shows each table's column as `stamps`.

With `-mark-synced`, an incremental run also copies every row whose sync column is empty, even when its change column (`updated_at`) did not move: a row edited without touching `updated_at` still reaches the live DB before it is stamped. After the data:

1. On the target, in the sync transaction, every row with an empty sync column gets the sync time.
2. After the target commits, the same rows on the source get the **same** time — unless the row was edited after the run read it (its row version changed); it stays waiting for the next run. The `sync_deleted_rows` entries of synced tables, up to the run's snapshot, get `sdr_sync_date`.
3. Rows that break one of their table's `NOT VALID` checks are left waiting on both sides: PostgreSQL refuses any update of them until they are corrected.

With `-mark-txn=false` only the master tables (the instant ones, see [Instant mirroring](#instant-mirroring--instant)) get the sync time, together with their `sync_deleted_rows` entries; the transaction tables keep `NULL` on both sides. With `-instant` that means every instant sync of a master table stamps its rows on the live DB in the same transaction and on the local DB right after, while the interval runs of the transaction tables stamp nothing.

The source stamp runs with triggers off (`session_replication_role = replica`) when the source user is a superuser, so it is not recorded as a change in the audit log. Only turn this on when the target is the real live database: on a test copy it would mark local rows as sent to live when they are not.

## Instant mirroring (`-instant`)

With `-instant` (and `-interval`), each table is synced on the schedule of its kind:

- **Instant tables**: inserts, updates and deletes reach the target a few seconds after they are committed locally.
- **Interval tables**: synced by the interval runs, e.g. every 5 minutes, or less often for a kind with its own interval.

### Which table is synced when: `public.db_table_master`

The schedule comes from `dtm_kind` in `public.db_table_master` on the source, not from a list in `.env`. It is read in every run's snapshot, so a table added or re-categorised there is picked up without touching the configuration. The defaults:

| `dtm_kind` | Synced |
|---|---|
| `MASTER`, `SETTING`, `METADATA`, `LEDGER`, `BALANCE`, `SYSTEM` (`-instant-kinds`) | instantly, partitioned tables included |
| `TRANSACTION`, any other kind, inactive rows (`dtm_is_active = false`), tables missing from `db_table_master` | every `-interval` (5 minutes) |
| `LOG` (`-kind-intervals LOG:1h`) | by the interval run nearest to each hour; after a restart, by the first run |

`dtm_sync_col` in the same row names the table's sync column, the one `-mark-synced` stamps (see [Sync time](#sync-time--mark-synced)).

`db_table_master` carries the notify trigger too: adding or changing a row there (say a `dtm_kind` from `TRANSACTION` to `MASTER`) re-sorts the tables within seconds, without a restart; a table that becomes instant is synced at once (`instant tables changed` in the log). A new table is created on the live DB by the next interval run (`-ddl`) and mirrored instantly from then on. `-dry-run` shows each table's `kind` and `sync` (`instant`, `every 5m0s`, `every 1h0m0s`); the interval runs that move the `LOG` tables log `with_kinds=LOG`.

A foreign key from an instant table to an interval table (e.g. `accounts.acc_bill_balance` → `accounts.acc_voucher_header`) means a new instant row can reach the live DB before the row it points at, until the next interval run moves that one. With `-disable-triggers` the live DB accepts it in the meantime; without, the instant sync of that table fails and is retried until then. Such references are listed in a warning when the instant sync starts.

`-table-master ""` (an empty `SYNC_TABLE_MASTER=`) turns `db_table_master` off: every table is then instant except those listed in `-txn-tables`, which is ignored otherwise.

How it works:

1. On start, the tool puts a statement trigger `zz_vknex_sync_notify` on every instant table of the **source**, on each of its partitions and on `-table-master`, calling `vknex_sync.notify_change()`. When a transaction that changed the table commits, the trigger sends the table's name on the `vknex_sync` channel (`pg_notify`); a partition's trigger sends its partitioned table's name. Tables that are moved to the interval runs or excluded lose the trigger.
2. A listener on that channel waits 0.3 s for the rest of the save, then runs an incremental sync of just the tables named, in one transaction on the target. It has its own advisory lock, so it runs alongside an interval run.
3. On start, and after losing the source connection, every instant table is synced once to catch up on changes made while nothing was listening.
4. Schema changes (`-ddl`) stay with the interval runs. An interval run that has schema changes to apply moves **every** table, not just the transaction tables, so that the constraints, indexes and `NOT NULL` it adds after the data find every table current. Instant syncs pause while it runs (`instant sync paused` … `resumed`). An instant table whose structure changed locally waits for that run (logged as a warning). New tables get the trigger after it.
5. When one table's data makes a run fail, the tables are tried one at a time, in between new changes, so one table cannot hold up the others. Connection errors, lock timeouts and a pause retry the whole set: after 5 s, doubling up to 1 minute (a pause keeps checking every 5 s).

A change takes 0.3 s plus one sync of its table, about 9 round trips: well under a second on a local network, about 5.5 s over a 576 ms link.

Things to know:

- With `-disable-triggers` the target does not check foreign keys. Deleting a master row locally (after its transactions) removes it from the live DB at once, while the live transaction rows that point at it stay until the next interval run deletes them. Without `-disable-triggers` that delete fails and is retried until the interval run has removed them.
- A statement trigger on a partitioned table does not fire for a statement that names one of its partitions; that is why the partitions carry the trigger too. A partition created later (the next fiscal year) gets it on the next start or `db_table_master` change; until then, changes made through the partitioned table itself (what the application does) are still noticed.
- `vknex_sync.run_log` gets a row for every instant run (`mode = 'instant'`).
- To take the triggers off the local database: `DROP SCHEMA vknex_sync CASCADE;` on the source.

## Two-way master tables (`-two-way`)

With `-two-way`, the tables of the `-two-way-kinds` kinds (default `MASTER` in `db_table_master`) may also be edited on the live DB: rows inserted, changed or deleted there come back to the local DB. Every other table stays one-way. It needs `-mark-synced`: an empty sync column is how a local row that changed since the last sync is told apart. A two-way table without a primary key or a sync column stays one-way, named in a warning (`msg="sync column"`). `-dry-run` shows `two_way=true` on the two-way tables and the live changes waiting to be pulled.

How it works:

1. On every two-way table of the **live** DB the tool puts a row trigger `zz_vknex_sync_capture`. It logs the key of each row a live user inserts, updates or deletes in `vknex_sync.live_changes` and notifies the sync. The sync's own writes are not logged (its transactions set `vknex_sync.applying`), so nothing echoes back. The trigger function runs as its owner (`SECURITY DEFINER`): the live application's database user needs no rights on `vknex_sync`.
2. Before a two-way table is pushed, its logged keys are **pulled**: in one local transaction, each local row becomes what the live DB has now, written or deleted, with its sync column set to the pull time. With `-instant` a live change reaches the local DB within a second (0.6 s on localhost); a two-way table that is not instant is pulled at the start of every interval run, as is every table without `-instant`.
3. Until a key is pulled, the push leaves that live row alone and the local row waiting, so a push that started before the pull saw a live change never overwrites or deletes it. When a pull fails (logged and mailed as `two-way pull failed`), the push goes on without those rows.
4. Pulled rows are ordinary local writes: local triggers fire (audit log, `sync_deleted_rows`, …), foreign keys are checked, and sequences are moved past the pulled ids.

**Conflicts.** A row changed on both sides since the last sync (its local sync column is empty, or its local delete waits in `sync_deleted_rows`):

| `-two-way-wins` | Result |
|---|---|
| `local` (default) | The local version stays and the push writes it to the live DB; the live change is dropped |
| `live` | The live version is written locally |

Each conflict is logged as a warning (`two-way conflict`) with the keys. Unique keys work the same way: when a live row takes a value a local row holds (e.g. a customer code), one of them has to go: the live one with `local`, the local one with `live`.

A row deleted on the live DB that local rows still point at (e.g. a customer whose bills are not on the live DB yet) is never deleted locally: that would fail, or cascade into those rows. It is kept and written back to the live DB (warning `deleted on the live DB, but local rows still point at them`).

**Ids of new rows (`-two-way-live-ids`).** Both databases hand out ids from sequences that the sync keeps in step. Rows created on both sides between two syncs, or while the tunnel is down, can get the **same id**: two different rows then meet as one conflict, and one of them is lost. Set `-two-way-live-ids` (e.g. `1000000000`) whenever new master rows are created on both sides: the two-way tables' sequences on the live DB then hand out ids from that number on, and local ids stay below it. It has to fit the id column (`integer` ends at 2147483647).

Things to know:

- Only row changes are captured: a `TRUNCATE` on the live DB is not, and neither are changes made before the trigger went on.
- `-mode full` reloads the live DB from the local one: live changes not pulled by then are lost.
- A table that stops being two-way loses the trigger; its live changes not pulled yet are dropped, and the local rows win.
- With `-instant` the sync also listens on the live DB; when that connection drops, the instant sync starts over a minute later and catches up.
- Pulls are recorded in `vknex_sync.run_log` on the live DB with `mode = 'pull'`.
- To take it off the live DB: `DROP FUNCTION vknex_sync.capture_change() CASCADE; DROP TABLE vknex_sync.live_changes;`

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

**Linux desktop, click to open**: run `deploy/linux/install.sh` once. It builds the binary, adds a systemd user service (`~/.config/systemd/user/vknex-dbsync.service`, settings from `.env`, logs appended to `vknex-dbsync.log`, restarted 30 s after a crash) and a "VK Nex DB Sync" icon on the desktop and in the app grid. Click the icon to open the [status page](#status-page--ui) in the browser; when the sync is not running, it is started first. Right-click the icon for Start in the background (no browser), Stop and Show log. Run the installer again after changing the code or moving the project folder: it restarts a running sync so that the new build is used.

The installer also makes it start whenever the computer boots, before anyone logs in: it enables the service and turns on lingering for the user (`loginctl enable-linger`), which runs the user's services from boot instead of from the first login. Until the local Postgres, the network and the tunnel are up, the runs fail and are retried (instant sync within a minute, the interval run on the next interval). To stop starting it automatically: `systemctl --user disable vknex-dbsync.service`. If the installer warns that lingering could not be enabled, it starts at login only until you run `sudo loginctl enable-linger $USER`.

When the live database is only reachable through SSH, pass the `~/.ssh/config` host whose `LocalForward` opens the tunnel: `deploy/linux/install.sh erp-vps`. That adds `vknex-dbsync-tunnel.service` (`ssh -N erp-vps`, reconnected 10 s after it drops), which starts and stops with the sync. At boot nobody is logged in, so the host's key must work without a passphrase or the desktop's ssh-agent. Point `SYNC_TARGET_URL` at the forwarded local port (e.g. `127.0.0.1:5433`), not at `127.0.0.1:5432`: that is this machine's own Postgres, and the sync fails with `database "erp_rehearsal" does not exist`.

**Windows, double-click**: build with `-ldflags "-H=windowsgui"` (see Build) and put `vknex-dbsync.exe` and `.env` (with `SYNC_INTERVAL=5m`) in one folder. Double-clicking the exe starts it in the background with no window:

- logs go to `vknex-dbsync.log` in that folder; startup errors (bad `.env`) pop up in a message box,
- a second double-click opens the [status page](#status-page--ui) in the browser (with `SYNC_UI=` empty it only reports that it is already running),
- to start it with Windows: `Win+R` → `shell:startup` → put a shortcut to the exe there,
- to stop it: Task Manager → Details → `vknex-dbsync.exe` → End task, or `taskkill /IM vknex-dbsync.exe /F`. An open sync transaction is rolled back by the server.

**Windows, other ways**: Task Scheduler running `vknex-dbsync.exe` every 5 minutes (without `-interval`), or wrap the console build `vknex-dbsync.exe -interval 5m` as a service with NSSM.

**PM2** (if you already use it): `pm2 start ./vknex-dbsync --name dbsync -- -interval 5m`

## Status page (`-ui`)

While it runs with `-interval`, the sync serves a small web page at `-ui` (default `http://127.0.0.1:7645/`, reachable from this machine only; `SYNC_UI=` turns it off). On Linux the desktop icon opens it; on Windows a second double-click on the running exe does. It refreshes itself every 2 seconds and shows:

- **Status**: running, a run in progress, the last run failed, the instant sync paused, retrying or down, each with the error text.
- **Last run / next run**: when, how long it took, tables and rows written and deleted, and a countdown to the next interval run. **Sync now** starts an interval run right away instead of waiting.
- **Instant sync**: listening, syncing or retrying, how many tables are instant and how many follow the interval, the last change synced and the tables waiting for a retry.
- **Tables**: every synced table with its `dtm_kind`, when it is synced (instant, every 5m, every 1h), its last-synced time on the live database (`vknex_sync.table_state`) and notes such as "no primary key: replaced on each run", with a filter.
- **Run history**: the last 100 rows of `vknex_sync.run_log` on the live database.
- **Log**: the last 300 log lines, with a switch for warnings and errors only.
- **Settings**: the configuration the process runs with, without passwords.

The page reads `table_state` and `run_log` from the live database at most every 5 seconds; everything else comes from the process itself. Nothing on it changes data: the only action is "Sync now". `-dry-run` and a single run (no `-interval`) serve no page. To reach it from another machine, set `SYNC_UI=0.0.0.0:7645` and open the port in the firewall, keeping in mind that the page has no login.

## Mail when it fails or stops (`-mail-to`)

With `-mail-to` (comma-separated addresses) the sync sends a mail, subject `[vknex-dbsync] … on <host>`, from "VK Nex DB Sync". It is a styled HTML card (a coloured badge for the event, the error in a monospace box, a table with host, times, source, target and log file) with a plain-text version for clients that prefer one:

- **sync failed** / **instant sync failed**: a run failed (tunnel down, Postgres restarting, a constraint, …). One mail when it starts failing, then one every `-mail-repeat` (default `1h`, `0` = none) while it keeps failing, so a dead tunnel does not mail on every retry.
- **sync recovered** / **instant sync recovered**: the first run that works again after a failure mail. Only with `-interval`: a run-once process (Task Scheduler) mails every failed run and nothing else.
- **stopped**: the program ended. Ctrl-C, `systemctl stop`, the desktop icon's Stop, Task Manager > End task, or a crash (the mail carries the stack trace; the Linux service starts it again after 30 s). A `kill -9`, a power cut or a machine that dies cannot send anything.

The mails go through `-smtp` (`host:port`) as `-smtp-user` / `-smtp-password`, from `-mail-from` (default: the user). Port 465 is TLS from the start, any other port uses STARTTLS when the server offers it. Gmail: `smtp.gmail.com:587`, the Gmail address as user and an **app password** (Google account → Security → 2-Step Verification → App passwords), not the account password. After filling in `.env`, `vknex-dbsync -mail-test` sends a test mail and exits. Incomplete mail settings are a configuration error and the program does not start: `-mail-to` needs `-smtp`, and `-mail-from` when the user is not an address. `-dry-run` never mails.

## Client list (`-client-name`)

The live server keeps one list of its clients and the database each one's data is in, in the database `vknex_clients`:

```sql
SELECT * FROM public.client_master ORDER BY client_id;   -- client_id, client_name, db_name, created_on, updated_on
```

With `-client-name` the sync saves its own row there when it starts: the client name with the database of `-target` as `db_name` (`erp_rehearsal` for `.../erp_rehearsal`). A database not listed yet gets a new row and `client_id`; a listed one gets the new name when it changed, otherwise it is left alone, so `updated_on` (set by a trigger) is the time of the last real change. The list is on the same server as the target and is reached with the same user and password; `-client-db` names another database for it. The sync does not depend on the list: when it cannot be written (the database or table is missing, the tunnel is down) the sync logs a warning and tries again before the next run. `-dry-run` writes nothing there.

## Things to know

- **One-way, except the `-two-way` tables.** Anything written directly on the live DB to any other synced table is overwritten or deleted on the next run. If live users also enter data there, exclude those tables, make them two-way, or use a different design.
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
