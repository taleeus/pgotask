package pgotask

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
)

const VERSION = "v2"

const COOLDOWN_DEFAULT = time.Duration(time.Minute)
const RETRY_COOLDOWN_DEFAULT = time.Duration(5 * time.Minute)
const TASK_DEADLINE_DEFAULT = time.Minute
const TASK_RETRIES_DEFAULT = 5

type DB interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type HandlerFn func(context.Context, DB, json.RawMessage) error

type Scheduler struct {
	running bool

	db       *sql.DB
	handlers map[string]HandlerFn

	cooldown      time.Duration
	retryCooldown time.Duration
	taskDeadline  time.Duration
	retries       int
	version       string
}

// NewScheduler returns an initialized scheduler.
// Further configuration can be done in fluent-API style.
//
// To see default configuration, check constants with `_DEFAULT` postfix.
func NewScheduler(db *sql.DB) *Scheduler {
	return &Scheduler{
		db:       db,
		handlers: make(map[string]HandlerFn),

		cooldown:      COOLDOWN_DEFAULT,
		retryCooldown: RETRY_COOLDOWN_DEFAULT,
		taskDeadline:  TASK_DEADLINE_DEFAULT,
		retries:       TASK_RETRIES_DEFAULT,
	}
}

// Cooldown overrides the default cooldown between loops
func (s *Scheduler) Cooldown(cooldown time.Duration) *Scheduler {
	s.cooldown = cooldown
	return s
}

// RetryAfter overrides the default retry cooldown set on tasks after failure
func (s *Scheduler) RetryAfter(retryCooldown time.Duration) *Scheduler {
	s.retryCooldown = retryCooldown
	return s
}

// TaskDeadline overrides the default task deadline
func (s *Scheduler) TaskDeadline(deadline time.Duration) *Scheduler {
	s.taskDeadline = deadline
	return s
}

// Version sets the filter for versioned tasks.
// The idiomatic way to use this is to pass the current Go package version.
func (s *Scheduler) Version(version string) *Scheduler {
	s.version = version
	return s
}

// Retries overrides the default task retries
func (s *Scheduler) Retries(retries int) *Scheduler {
	s.retries = retries
	return s
}

// Handler registers a callback for the given task type.
//
// All task types should be handled by an application.
//
// Handlers can check context cancellation to know if an error happened
// during the dispatch loop on some other task.
func (s *Scheduler) Handler(taskType string, handler HandlerFn) *Scheduler {
	s.handlers[taskType] = handler
	return s
}

// Run launches the scheduler.
// If the scheduler is already running or the database schema fails to initialize,
// the method exits with an error immediately; otherwise, the dispatch loop starts in the background.
//
// To stop the loop manually, you need to cancel the context.
func (s *Scheduler) Run(ctx context.Context) error {
	if s.running {
		slog.WarnContext(ctx, "Scheduler already running")
		return ErrAlreadyRunning
	}

	slog.DebugContext(ctx, "Initializing schema")
	if err := initSchema(ctx, s.db); err != nil {
		return errors.Join(ErrInitSchema, err)
	}

	s.running = true
	go s.dispatchLoop(ctx)

	return nil
}

type TaskArgs struct {
	TaskType      string          `json:"taskType"`
	Payload       json.RawMessage `json:"payload"`
	Idempotent    bool            `json:"idempotent"`
	DispatchAfter time.Duration   `json:"dispatchAfter"`
}

// ScheduleTask schedules a task (duh)
func (s *Scheduler) ScheduleTask(ctx context.Context, task TaskArgs) error {
	var version sql.NullString
	if s.version != "" {
		version = sql.NullString{String: s.version, Valid: true}
	}

	if err := scheduleTask(ctx, s.db,
		task.TaskType,
		version,
		task.Payload,
		task.Idempotent,
		task.DispatchAfter,
	); err != nil {
		slog.ErrorContext(ctx, "Task scheduling failed",
			slog.String("err", err.Error()),
			slog.Any("task", task),
		)

		return errors.Join(ErrScheduleFailed, err)
	}

	return nil
}

func (s *Scheduler) dispatchLoop(ctx context.Context) {
	slog.DebugContext(ctx, "First dispatch")
	if err := s.dispatch(ctx); err != nil {
		slog.ErrorContext(ctx, "First dispatch encountered errors",
			slog.String("err", err.Error()),
		)
	}

	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	for s.running {
		slog.DebugContext(ctx, "Entered dispatch loop; waiting for event")
		select {
		case <-time.After(s.cooldown):
			slog.DebugContext(ctx, "Dispatch fired by cooldown expiration",
				slog.Duration("cooldown", s.cooldown),
			)

			if err := s.dispatch(ctx); err != nil {
				slog.ErrorContext(ctx, "Dispatch ended with errors",
					slog.String("err", err.Error()),
				)
			}

		case <-ctx.Done():
			slog.InfoContext(ctx, "Scheduler stopped by context cancellation",
				slog.String("cause", context.Cause(ctx).Error()),
			)

			s.running = false

		case sig := <-sigint:
			slog.InfoContext(ctx, "Scheduler stopped by interruption signal",
				slog.String("signal", sig.String()),
			)

			s.running = false
		}
	}
}

func (s Scheduler) dispatch(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Join(ErrTxCreation, err)
	}
	defer tx.Rollback()

	if err := lockTasks(ctx, tx); err != nil {
		return errors.Join(ErrQueryLock, err)
	}

	var version sql.NullString
	if s.version != "" {
		version = sql.NullString{String: s.version, Valid: true}
	}

	tasks, err := findPendingTasks(ctx, tx, version)
	if err != nil {
		return errors.Join(ErrQueryPending, err)
	}
	slog.DebugContext(ctx, "Fetched pending tasks",
		slog.Any("tasks", tasks),
	)

	tasks = slices.CompactFunc(tasks, func(t1, t2 TaskScheduled) bool {
		if !t1.Idempotent || !t2.Idempotent {
			return false
		}

		return t1.Type == t2.Type && bytes.Equal(t1.Payload, t2.Payload)
	})
	slog.DebugContext(ctx, "Filtered tasks",
		slog.Any("tasks", tasks),
	)

	var dispatchGroup errgroup.Group
	for _, task := range tasks {
		slog.DebugContext(ctx, "Dispatching",
			slog.Any("task", task),
		)

		dispatchGroup.Go(func() error {
			handler, ok := s.handlers[task.Type]
			if !ok {
				return fmt.Errorf("%w (%s)", ErrUnhandledTaskType, task.Type)
			}

			deadlineCtx, cancel := context.WithTimeoutCause(ctx, s.taskDeadline, ErrExcededTimeline)
			defer cancel()

			if err := handler(deadlineCtx, tx, task.Payload); err != nil {
				slog.DebugContext(ctx, "Handler failed task",
					slog.Any("task", task),
					slog.String("err", err.Error()),
					slog.Duration("retryCooldown", s.retryCooldown),
				)

				switch {
				case task.Retries >= s.retries:
					if err := pushFailure(ctx, tx, task.Task, err.Error()); err != nil {
						return fmt.Errorf("%w (id: %s)", ErrPushFailure, task.ID)
					}

					if err := deleteTask(ctx, tx, task.ID); err != nil {
						return fmt.Errorf("%w (id: %s)", ErrDeleteScheduled, task.ID)
					}

				default:
					if err := setRetryCooldown(ctx, tx, task.ID, s.retryCooldown); err != nil {
						return fmt.Errorf("%w (id: %s)", ErrRetryCooldown, task.ID)
					}
				}
			} else {
				slog.DebugContext(ctx, "Handler completed task",
					slog.Any("task", task),
				)

				if err := markCompleted(ctx, tx, task.Task); err != nil {
					return errors.Join(ErrAbortDispatch,
						fmt.Errorf("%w (id: %s)", ErrMarkCompleted, task.ID),
						err,
					)
				}

				if err := deleteTask(ctx, tx, task.ID); err != nil {
					return errors.Join(ErrAbortDispatch,
						fmt.Errorf("%w (id: %s)", ErrDeleteScheduled, task.ID),
						err,
					)
				}

				if task.Idempotent {
					slog.DebugContext(ctx, "Task is idempotent: deleting duplicate tasks")
					if err := deleteIdempotent(ctx, tx,
						task.Type,
						task.Payload,
					); err != nil {
						return fmt.Errorf("%w (id: %s)", ErrDeleteDuplicates, task.ID)
					}
				}
			}

			return nil
		})
	}

	dispatchErr := dispatchGroup.Wait()
	if errors.Is(dispatchErr, ErrAbortDispatch) {
		slog.WarnContext(ctx, "Dispatch aborted")
		return dispatchErr
	}

	if err := tx.Commit(); err != nil {
		return errors.Join(dispatchErr, ErrTxCommit, err)
	}

	return dispatchErr
}
