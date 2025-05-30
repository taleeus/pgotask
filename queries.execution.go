package pgotask

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

var pushFailureQuery = `
INSERT INTO task_dead_v2 (
	id,
	type,
	version,
	idempotent,
	payload,
	created_at,
	updated_at,
	error
)
VALUES (
	$1,
	$2,
	$3,
	$4,
	$5,
	$6,
	$7,
	$8
)
`

var deleteTaskQuery = `
DELETE FROM task_scheduled_v2
WHERE id = $1
`

func pushFailure(ctx context.Context, tx *sql.Tx, task Task, message string) error {
	slog.DebugContext(ctx, "Executing query",
		slog.String("query", pushFailureQuery),
		slog.Any("task", task),
		slog.String("message", message),
	)

	if _, err := tx.ExecContext(ctx, pushFailureQuery,
		task.ID,
		task.Type,
		task.Version,
		task.Idempotent,
		task.Payload,
		task.CreatedAt,
		task.UpdatedAt,
		message,
	); err != nil {
		return errors.Join(ErrExecQuery, err)
	}

	return nil
}

func deleteTask(ctx context.Context, tx *sql.Tx, id uuid.UUID) error {
	slog.DebugContext(ctx, "Executing query",
		slog.String("query", deleteTaskQuery),
		slog.String("id", id.String()),
	)

	if _, err := tx.ExecContext(ctx, deleteTaskQuery, id); err != nil {
		return errors.Join(ErrExecQuery, err)
	}

	return nil
}

var setRetryCooldownQuery = `
UPDATE task_scheduled_v2
SET
	dispatch_after = $2,
	retries = retries + 1
WHERE id = $1
`

func setRetryCooldown(ctx context.Context, tx *sql.Tx, taskID uuid.UUID, cooldown time.Duration) error {
	dispatchAfter := time.Now().Add(cooldown)
	slog.DebugContext(ctx, "Executing query",
		slog.String("query", setRetryCooldownQuery),
		slog.String("$1", taskID.String()),
		slog.Time("$2", dispatchAfter),
	)

	if _, err := tx.ExecContext(ctx, setRetryCooldownQuery, taskID, dispatchAfter); err != nil {
		return errors.Join(ErrExecQuery, err)
	}

	return nil
}

var markCompletedQuery = `
INSERT INTO task_completed_v2 (
	id,
	type,
	version,
	idempotent,
	payload,
	created_at,
	updated_at
) VALUES (
	$1,
	$2,
	$3,
	$4,
	$5,
	$6,
	$7
)
`

func markCompleted(ctx context.Context, tx *sql.Tx, task Task) error {
	slog.DebugContext(ctx, "Executing query",
		slog.String("query", markCompletedQuery),
		slog.Any("task", task),
	)

	if _, err := tx.ExecContext(ctx, markCompletedQuery,
		task.ID,
		task.Type,
		task.Version,
		task.Idempotent,
		task.Payload,
		task.CreatedAt,
		task.UpdatedAt,
	); err != nil {
		return errors.Join(ErrExecQuery, err)
	}

	return nil
}

var scheduleTaskQuery = `
INSERT INTO task_scheduled_v2 (
	type,
	version,
	payload,
	idempotent,
	dispatch_after
)
VALUES ($1, $2, $3, $4, $5)
`

func scheduleTask(ctx context.Context, db *sql.DB,
	taskType string,
	version string,
	payload json.RawMessage,
	idempotent bool,
	dispatchAfter time.Duration,
) error {
	dispatchTimestamp := time.Now().Add(dispatchAfter)
	slog.DebugContext(ctx, "Executing query",
		slog.String("query", scheduleTaskQuery),
		slog.Duration("dispatchAfter", dispatchAfter),
		slog.String("$1", taskType),
		slog.String("$2", version),
		slog.String("$3", string(payload)),
		slog.Bool("$4", idempotent),
		slog.Time("$5", dispatchTimestamp),
	)

	if _, err := db.ExecContext(ctx, scheduleTaskQuery,
		taskType,
		version,
		payload,
		idempotent,
		dispatchTimestamp,
	); err != nil {
		return errors.Join(ErrExecQuery, err)
	}

	return nil
}

var deleteIdempotentQuery = `
DELETE FROM task_scheduled_v2
WHERE
	type = $1 AND
	payload = $2::JSONB AND
	idempotent = TRUE
`

func deleteIdempotent(ctx context.Context, tx *sql.Tx,
	typ string,
	payload json.RawMessage,
) error {
	slog.DebugContext(ctx, "Executing query",
		slog.String("query", deleteIdempotentQuery),
		slog.String("$1", typ),
		slog.String("$2", string(payload)),
	)

	if _, err := tx.ExecContext(ctx, deleteIdempotentQuery,
		typ,
		payload,
	); err != nil {
		return errors.Join(ErrExecQuery, err)
	}

	return nil
}
