package pgotask

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

var setLockTimeoutQuery = /* sql */ `SET LOCAL lock_timeout = '%dmin'`
var lockTasksQuery = /* sql */ `LOCK task_scheduled_v2 IN ROW EXCLUSIVE MODE`

func lockTasks(ctx context.Context, tx *sql.Tx, lockTimeoutMin uint) error {
	slog.DebugContext(ctx, "Executing query",
		slog.String("query", setLockTimeoutQuery),
		slog.Uint64("$1", uint64(lockTimeoutMin)),
	)

	if _, err := tx.ExecContext(ctx, fmt.Sprintf(setLockTimeoutQuery, lockTimeoutMin)); err != nil {
		return errors.Join(ErrExecQuery, err)
	}

	slog.DebugContext(ctx, "Executing query",
		slog.String("query", lockTasksQuery),
	)

	if _, err := tx.ExecContext(ctx, lockTasksQuery); err != nil {
		return errors.Join(ErrExecQuery, err)
	}

	return nil
}

var findPendingTasksQuery = /* sql */ `
SELECT *
FROM task_scheduled_v2
WHERE
	dispatch_after <= $1 AND
	(version <= $2 OR version IS NULL)
ORDER BY
	priority DESC,
	dispatch_after ASC
LIMIT $3
FOR UPDATE NOWAIT
`

func findPendingTasks(ctx context.Context, tx *sql.Tx, version sql.NullString, limit uint) ([]TaskScheduled, error) {
	now := time.Now().UTC()
	slog.DebugContext(ctx, "Executing query",
		slog.String("query", findPendingTasksQuery),
		slog.Time("$1", now),
		slog.String("$2", version.String),
		slog.Uint64("$3", uint64(limit)),
	)

	rows, err := tx.QueryContext(ctx, findPendingTasksQuery, now, version, limit)
	if err != nil {
		return nil, errors.Join(ErrExecQuery, err)
	}
	defer rows.Close()

	tasks := make([]TaskScheduled, 0)
	for rows.Next() {
		var task TaskScheduled
		if err := rows.Scan(
			&task.ID,
			&task.Type,
			&task.Version,
			&task.Idempotent,
			&task.Payload,
			&task.CreatedAt,
			&task.UpdatedAt,
			&task.Priority,
			&task.Retries,
			&task.DispatchAfter,
		); err != nil {
			return nil, errors.Join(ErrScanRow, err)
		}

		tasks = append(tasks, task)
	}

	return tasks, nil
}
