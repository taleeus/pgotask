package pgotask

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"
)

var lockTasksQuery = `LOCK ` + table[Task]()

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
SELECT
	` + join(columns[Task](false)) + `
FROM ` + table[Task]() + `
WHERE
	` + column[Task]("completed_at") + ` IS NULL AND
	` + column[Task]("dispatch_after") + ` <= $1
ORDER BY ` + column[Task]("dispatch_after")

func findPendingTasks(ctx context.Context, tx *sql.Tx) ([]Task, error) {
	now := time.Now()
	slog.DebugContext(ctx, "Executing query",
		slog.String("query", findPendingTasksQuery),
		slog.Time("$1", now),
	)

	rows, err := tx.QueryContext(ctx, findPendingTasksQuery, now)
	if err != nil {
		return nil, errors.Join(ErrExecQuery, err)
	}
	defer rows.Close()

	tasks := make([]Task, 0)
	for rows.Next() {
		var task Task
		if err := rows.Scan(
			&task.ID,
			&task.Type,
			&task.TypeVersion,
			&task.Payload,
			&task.DispatchAfter,
			&task.CompletedAt,
			&task.CreatedAt,
			&task.UpdatedAt,
		); err != nil {
			return nil, errors.Join(ErrScanRow, err)
		}

		tasks = append(tasks, task)
	}

	return tasks, nil
}
