package pgotask

import "errors"

// Database
/// Transactions

var ErrTxCreation = errors.New("couldn't create Tx")
var ErrTxCommit = errors.New("couldn't commit Tx")

/// Queries

var ErrExecQuery = errors.New("couldn't execute query")
var ErrScanRow = errors.New("couldn't scan row")

// Scheduler
/// Fatal

var ErrAlreadyRunning = errors.New("scheduler already running")
var ErrNotRunning = errors.New("scheduler is not running")
var ErrInitSchema = errors.New("couldn't init db schema")
var ErrQueryPending = errors.New("couldn't query pending tasks")
var ErrQueryLock = errors.New("couldn't lock tasks")

/// Dispatch

var ErrAbortDispatch = errors.New("dispatch aborted")
var ErrUnhandledTaskType = errors.New("handler for task type not found")
var ErrPushFailure = errors.New("couldn't push failure")
var ErrRetryCooldown = errors.New("couldn't set retry cooldown")
var ErrMarkCompleted = errors.New("couldn't mark completed task")

/// Schedule task

var ErrScheduleFailed = errors.New("couldn't schedule task")
