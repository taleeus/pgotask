package pgotask

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
)

const VERSION = "v1"

const COOLDOWN_DEFAULT = time.Duration(time.Minute)
const RETRY_COOLDOWN_DEFAULT = time.Duration(5 * time.Minute)

type HandlerFn func(context.Context, *sql.Tx, json.RawMessage) error

type scheduler struct {
	running bool

	db       *sql.DB
	handlers map[string]HandlerFn

	cooldown      time.Duration
	retryCooldown time.Duration
}

// NewScheduler returns an initialized scheduler.
// Further configuration can be done in fluent-API style.
//
// To see default configuration, check constants with `_DEFAULT` postfix.
func NewScheduler(db *sql.DB) *scheduler {
	return &scheduler{
		db:       db,
		handlers: make(map[string]HandlerFn),

		cooldown:      COOLDOWN_DEFAULT,
		retryCooldown: RETRY_COOLDOWN_DEFAULT,
	}
}

// Cooldown overrides the default cooldown between loops
func (s *scheduler) Cooldown(cooldown time.Duration) *scheduler {
	s.cooldown = cooldown
	return s
}

// RetryAfter overrides the default retry cooldown set on tasks after failure
func (s *scheduler) RetryAfter(retryCooldown time.Duration) *scheduler {
	s.retryCooldown = retryCooldown
	return s
}

// Handler registers a callback for the given task type.
//
// All task types should be handled by an application.
//
// Handlers can check context cancellation to know if an error happened
// during the dispatch loop on some other task.
func (s *scheduler) Handler(taskType string, handler HandlerFn) *scheduler {
	s.handlers[taskType] = handler
	return s
}

// Run launches the scheduler.
// If the scheduler is already running or the database schema fails to initialize,
// the method exits with an error immediately; otherwise, the dispatch loop starts in the background.
//
// To stop the loop manually, you need to cancel the context.
func (s *scheduler) Run(ctx context.Context) error {
	if s.running {
		slog.WarnContext(ctx, "Scheduler already running")
		return ErrAlreadyRunning
	}

	slog.DebugContext(ctx, "Initializing schema")
	if err := initSchema(ctx, s.db); err != nil {
		return errors.Join(ErrInitSchema, err)
	}

	go s.dispatchLoop(ctx)
	s.running = true

	return nil
}

type TaskArgs struct {
	TaskType        string          `json:"taskType"`
	TaskTypeVersion int             `json:"taskTypeVersion"`
	Payload         json.RawMessage `json:"payload"`
	DispatchAfter   time.Duration   `json:"dispatchAfter"`
}

// ScheduleTask schedules a task (duh)
func (s *scheduler) ScheduleTask(ctx context.Context, task TaskArgs) error {
	if !s.running {
		slog.WarnContext(ctx, "Scheduler is not running")
		return ErrNotRunning
	}

	if err := scheduleTask(ctx, s.db,
		task.TaskType,
		task.TaskTypeVersion,
		task.Payload,
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

func (s *scheduler) dispatchLoop(ctx context.Context) {
	slog.DebugContext(ctx, "First dispatch")
	if err := s.dispatch(ctx); err != nil {
		slog.ErrorContext(ctx, "First dispatch encountered errors",
			slog.String("err", err.Error()),
		)
	}

	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	for {
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
			return

		case sig := <-sigint:
			slog.InfoContext(ctx, "Scheduler stopped by interruption signal",
				slog.String("signal", sig.String()),
			)

			s.running = false
			return
		}
	}
}

func (s scheduler) dispatch(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Join(ErrTxCreation, err)
	}
	defer tx.Rollback()

	if err := lockTasks(ctx, tx); err != nil {
		return errors.Join(ErrQueryLock, err)
	}

	tasks, err := findPendingTasks(ctx, tx)
	if err != nil {
		return errors.Join(ErrQueryPending, err)
	}

	slog.DebugContext(ctx, "Fetched pending tasks",
		slog.Any("tasks", tasks),
	)

	dispatchGroup, dispatchCtx := errgroup.WithContext(ctx)
	for _, task := range tasks {
		slog.DebugContext(dispatchCtx, "Dispatching",
			slog.Any("task", task),
		)

		dispatchGroup.Go(func() error {
			handler, ok := s.handlers[task.Type]
			if !ok {
				return fmt.Errorf("%w (%s)", ErrUnhandledTaskType, task.Type)
			}

			if err := handler(dispatchCtx, tx, task.Payload); err != nil {
				slog.DebugContext(dispatchCtx, "Handler failed task",
					slog.Any("task", task),
					slog.String("err", err.Error()),
					slog.Duration("retryCooldown", s.retryCooldown),
				)

				if err := pushFailure(dispatchCtx, tx, task.ID, err.Error()); err != nil {
					return fmt.Errorf("%w (id: %s)", ErrPushFailure, task.ID)
				}

				if err := setRetryCooldown(dispatchCtx, tx, task.ID, s.retryCooldown); err != nil {
					return fmt.Errorf("%w (id: %s)", ErrRetryCooldown, task.ID)
				}
			} else {
				slog.DebugContext(dispatchCtx, "Handler completed task",
					slog.Any("task", task),
				)

				if err := markCompleted(dispatchCtx, tx, task.ID); err != nil {
					return errors.Join(ErrAbortDispatch,
						fmt.Errorf("%w (id: %s)", ErrMarkCompleted, task.ID),
						err,
					)
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
