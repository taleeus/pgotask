package pgotask

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"
)

var lockTasksQuery = `LOCK task_scheduled_v2`

func lockTasks(ctx context.Context, tx *sql.Tx) error {
	slog.DebugContext(ctx, "Executing query",
		slog.String("query", lockTasksQuery),
	)

	if _, err := tx.ExecContext(ctx, lockTasksQuery); err != nil {
		return errors.Join(ErrExecQuery, err)
	}

	return nil
}

var findPendingTasksQuery = `
SELECT *
FROM task_scheduled_v2
WHERE
	dispatch_after <= $1 AND
	(version >= $2 OR version IS NULL)
ORDER BY
	priority,
	dispatch_after
`

func findPendingTasks(ctx context.Context, tx *sql.Tx, version sql.NullString) ([]TaskScheduled, error) {
	now := time.Now()
	slog.DebugContext(ctx, "Executing query",
		slog.String("query", findPendingTasksQuery),
		slog.Time("$1", now),
		slog.String("$2", version.String),
	)

	rows, err := tx.QueryContext(ctx, findPendingTasksQuery, now, version)
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
