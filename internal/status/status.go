// Package status keeps what the running sync is doing and shows it on a web
// page (-ui, see Start): the interval runs, the instant sync, the synced
// tables, the run history on the target and the latest log lines.
//
// The syncer reports into a *Status. Every method is safe on a nil *Status,
// so a run without a status page (tests, -dry-run, a single run) reports
// into nothing.
package status

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"
)

// Run is one interval run (syncer.RunOnce). Times are Unix milliseconds; 0 = none.
type Run struct {
	Mode          string   `json:"mode"`
	StartedAt     int64    `json:"startedAt"`
	FinishedAt    int64    `json:"finishedAt"`
	TookMs        int64    `json:"tookMs"`
	Tables        int      `json:"tables"`
	RowsRead      int64    `json:"rowsRead"`
	RowsWritten   int64    `json:"rowsWritten"`
	RowsDeleted   int64    `json:"rowsDeleted"`
	SchemaChanges int      `json:"schemaChanges"`
	WithKinds     []string `json:"withKinds"` // -kind-intervals kinds the run moved
	Skipped       bool     `json:"skipped"`   // another sync held the target's lock
	Error         string   `json:"error"`
}

// Runs is the state of the interval runs.
type Runs struct {
	Running      bool  `json:"running"`
	RunningSince int64 `json:"runningSince"`
	NextAt       int64 `json:"nextAt"`
	Requested    bool  `json:"requested"` // SyncNow was called; the run has not started yet
	Last         *Run  `json:"last"`
	LastOK       *Run  `json:"lastOk"`
	Count        int   `json:"count"`
	Failed       int   `json:"failed"`
	FailingSince int64 `json:"failingSince"` // first failure of the current streak; 0 when the last run worked
	RowsWritten  int64 `json:"rowsWritten"`
	RowsDeleted  int64 `json:"rowsDeleted"`
}

// InstantRun is one run of the instant sync (syncer.Watch).
type InstantRun struct {
	Tables      []string `json:"tables"`
	At          int64    `json:"at"`
	TookMs      int64    `json:"tookMs"`
	RowsWritten int64    `json:"rowsWritten"`
	RowsDeleted int64    `json:"rowsDeleted"`
}

// Instant is the state of the instant sync.
type Instant struct {
	Enabled         bool        `json:"enabled"`
	State           string      `json:"state"` // off, starting, listening, syncing, paused, retrying, down
	Since           int64       `json:"since"` // when State was entered
	InstantTables   int         `json:"instantTables"`
	IntervalTables  int         `json:"intervalTables"`
	Syncing         []string    `json:"syncing"` // tables of the run in progress
	Waiting         []string    `json:"waiting"` // tables whose sync failed; tried again at RetryAt
	RetryAt         int64       `json:"retryAt"`
	Last            *InstantRun `json:"last"`
	LastError       string      `json:"lastError"`
	LastErrorAt     int64       `json:"lastErrorAt"`
	LastErrorTables []string    `json:"lastErrorTables"`
	Count           int         `json:"count"`
	Failures        int         `json:"failures"`
	RowsWritten     int64       `json:"rowsWritten"`
	RowsDeleted     int64       `json:"rowsDeleted"`
}

// Table is one synced table, as the last run saw it.
type Table struct {
	Key          string `json:"key"`
	Schema       string `json:"schema"`
	Name         string `json:"name"`
	Kind         string `json:"kind"`     // dtm_kind, "" when -table-master does not list it
	Schedule     string `json:"schedule"` // "instant", "every 5m0s", "each run"
	Instant      bool   `json:"instant"`
	HasPK        bool   `json:"hasPk"`
	UpdatedCol   string `json:"updatedCol"`
	SyncCol      string `json:"syncCol"`
	TwoWay       bool   `json:"twoWay"` // -two-way: live changes come back to the local database
	Partitions   int    `json:"partitions"`
	LastSyncedAt int64  `json:"lastSyncedAt"` // from the target's table_state, filled in by the server
}

// Snapshot is a copy of the state at one moment.
type Snapshot struct {
	StartedAt  int64   `json:"startedAt"`
	Runs       Runs    `json:"runs"`
	Instant    Instant `json:"instant"`
	TableCount int     `json:"tableCount"`
	TablesAt   int64   `json:"tablesAt"`
}

// Status collects the reports. Slices stored in it are never changed in
// place, so a Snapshot can share them.
type Status struct {
	Log *Ring // the latest log lines

	mu        sync.Mutex
	startedAt time.Time
	runs      Runs
	instant   Instant
	tables    []Table
	tablesAt  time.Time
	trigger   chan struct{}
}

// New returns an empty Status that keeps the last 1000 log lines.
func New() *Status {
	return &Status{
		Log:       NewRing(1000),
		startedAt: time.Now(),
		instant:   Instant{State: "off"},
		trigger:   make(chan struct{}, 1),
	}
}

func ms(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

// Snapshot copies the current state.
func (st *Status) Snapshot() Snapshot {
	if st == nil {
		return Snapshot{}
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	return Snapshot{StartedAt: ms(st.startedAt), Runs: st.runs, Instant: st.instant, TableCount: len(st.tables), TablesAt: ms(st.tablesAt)}
}

// Tables copies the table list and says when it was read.
func (st *Status) Tables() ([]Table, time.Time) {
	if st == nil {
		return nil, time.Time{}
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	return slices.Clone(st.tables), st.tablesAt
}

// SetTables records the tables a run selected.
func (st *Status) SetTables(tables []Table) {
	if st == nil {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	st.tables, st.tablesAt = tables, time.Now()
}

// RunStarted records that an interval run began at `at`.
func (st *Status) RunStarted(at time.Time) {
	if st == nil {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	st.runs.Running, st.runs.RunningSince, st.runs.NextAt, st.runs.Requested = true, ms(at), 0, false
}

// RunFinished records how the interval run ended; err is RunOnce's result.
func (st *Status) RunFinished(r Run, err error) {
	if st == nil {
		return
	}
	r.FinishedAt = ms(time.Now())
	if r.StartedAt > 0 {
		r.TookMs = r.FinishedAt - r.StartedAt
	}
	if err != nil {
		r.Error = err.Error()
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	st.runs.Running, st.runs.RunningSince = false, 0
	if errors.Is(err, context.Canceled) {
		return // stopping, not a failure
	}
	st.runs.Count++
	st.runs.Last = &r
	switch {
	case err != nil:
		st.runs.Failed++
		if st.runs.FailingSince == 0 {
			st.runs.FailingSince = r.FinishedAt
		}
	case r.Skipped:
	default:
		st.runs.LastOK = &r
		st.runs.FailingSince = 0
		st.runs.RowsWritten += r.RowsWritten
		st.runs.RowsDeleted += r.RowsDeleted
	}
}

// NextRun records when the next interval run is due.
func (st *Status) NextRun(at time.Time) {
	if st == nil {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	st.runs.NextAt = ms(at)
}

// Trigger is ready when SyncNow asked for a run. Without a Status it is nil,
// which never becomes ready in a select.
func (st *Status) Trigger() <-chan struct{} {
	if st == nil {
		return nil
	}
	return st.trigger
}

// SyncNow asks the interval loop (which waits on Trigger) to run right away.
// It reports false when a run is in progress or already requested.
func (st *Status) SyncNow() bool {
	if st == nil {
		return false
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.runs.Running || st.runs.Requested {
		return false
	}
	select {
	case st.trigger <- struct{}{}:
		st.runs.Requested = true
		return true
	default:
		return false
	}
}

// InstantEnabled records that the instant sync is configured and starting.
func (st *Status) InstantEnabled() {
	if st == nil {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	st.instant.Enabled = true
	st.setInstantState("starting")
}

// setInstantState moves to state, keeping Since when it is unchanged.
func (st *Status) setInstantState(state string) {
	if st.instant.State != state {
		st.instant.State, st.instant.Since = state, ms(time.Now())
	}
}

// InstantStarted records that the instant sync listens for changes, with the
// number of instant and interval tables.
func (st *Status) InstantStarted(instant, interval int) {
	if st == nil {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	st.instant.Enabled = true
	st.instant.InstantTables, st.instant.IntervalTables = instant, interval
	st.instant.Syncing, st.instant.Waiting, st.instant.RetryAt = nil, nil, 0
	st.setInstantState("listening")
}

// InstantSyncing records that an instant run of tables began.
func (st *Status) InstantSyncing(tables []string) {
	if st == nil {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	st.instant.Syncing = slices.Clone(tables)
	st.setInstantState("syncing")
}

// InstantSynced records a completed instant run.
func (st *Status) InstantSynced(r InstantRun) {
	if st == nil {
		return
	}
	r.Tables = slices.Clone(r.Tables)
	st.mu.Lock()
	defer st.mu.Unlock()
	st.instant.Last = &r
	st.instant.Count++
	st.instant.RowsWritten += r.RowsWritten
	st.instant.RowsDeleted += r.RowsDeleted
}

// InstantSettled records the outcome of an instant run of tried: err (nil
// when it worked), the tables still waiting for a retry at retryAt, and
// whether the instant sync is paused for an interval run.
func (st *Status) InstantSettled(tried []string, err error, waiting []string, retryAt time.Time, paused bool) {
	if st == nil {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	st.instant.Syncing = nil
	st.instant.Waiting = slices.Clone(waiting)
	st.instant.RetryAt = ms(retryAt)
	switch {
	case paused:
		st.setInstantState("paused")
	case len(waiting) > 0:
		st.setInstantState("retrying")
	default:
		st.setInstantState("listening")
	}
	if err != nil && !paused && !errors.Is(err, context.Canceled) {
		st.instant.Failures++
		st.instant.LastError, st.instant.LastErrorAt, st.instant.LastErrorTables = err.Error(), ms(time.Now()), slices.Clone(tried)
	}
}

// InstantDown records that the instant sync stopped with err and restarts in retryIn.
func (st *Status) InstantDown(err error, retryIn time.Duration) {
	if st == nil {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	st.instant.Syncing = nil
	st.instant.RetryAt = ms(time.Now().Add(retryIn))
	st.setInstantState("down")
	if err != nil && !errors.Is(err, context.Canceled) {
		st.instant.Failures++
		st.instant.LastError, st.instant.LastErrorAt, st.instant.LastErrorTables = err.Error(), ms(time.Now()), nil
	}
}
