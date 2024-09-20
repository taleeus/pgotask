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
INSERT INTO ` + table[TaskFailure]() + ` (` + join(columns[TaskFailure](false,
	"task_id",
	"message",
)) + `)
VALUES ($1, $2)
`

func pushFailure(ctx context.Context, tx *sql.Tx, taskID uuid.UUID, message string) error {
	slog.DebugContext(ctx, "Executing query",
		slog.String("query", pushFailureQuery),
		slog.String("$1", taskID.String()),
		slog.String("$2", message),
	)

	if _, err := tx.ExecContext(ctx, pushFailureQuery, taskID, message); err != nil {
		return errors.Join(ErrExecQuery, err)
	}

	return nil
}

var setRetryCooldownQuery = `
UPDATE ` + table[Task]() + `
SET ` + column[Task]("dispatch_after") + ` = $2
WHERE ` + column[Task]("id") + ` = $1
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
UPDATE ` + table[Task]() + `
SET ` + column[Task]("completed_at") + ` = NOW()
WHERE ` + column[Task]("id") + ` = $1
`

func markCompleted(ctx context.Context, tx *sql.Tx, taskID uuid.UUID) error {
	slog.DebugContext(ctx, "Executing query",
		slog.String("query", markCompletedQuery),
		slog.String("$1", taskID.String()),
	)

	if _, err := tx.ExecContext(ctx, markCompletedQuery, taskID); err != nil {
		return errors.Join(ErrExecQuery, err)
	}

	return nil
}

var scheduleTaskQuery = `
INSERT INTO ` + table[Task]() + ` (` + join(columns[Task](false,
	"type",
	"type_version",
	"payload",
	"dispatch_after",
)) + `)
VALUES ($1, $2, $3, $4)
`

func scheduleTask(ctx context.Context, db *sql.DB,
	taskType string,
	taskTypeVersion int,
	payload json.RawMessage,
	dispatchAfter time.Duration,
) error {
	dispatchTimestamp := time.Now().Add(dispatchAfter)
	slog.DebugContext(ctx, "Executing query",
		slog.String("query", scheduleTaskQuery),
		slog.Duration("dispatchAfter", dispatchAfter),
		slog.String("$1", taskType),
		slog.Int("$2", taskTypeVersion),
		slog.String("$3", string(payload)),
		slog.Time("$4", dispatchTimestamp),
	)

	if _, err := db.ExecContext(ctx, scheduleTaskQuery,
		taskType,
		taskTypeVersion,
		payload,
		dispatchTimestamp,
	); err != nil {
		return errors.Join(ErrExecQuery, err)
	}

	return nil
}
