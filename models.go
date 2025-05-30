package pgotask

import (
	"database/sql"
	"time"

	"github.com/google/uuid"
)

type Task struct {
	ID         uuid.UUID      `db:"id"`
	Type       string         `db:"type"`
	Version    sql.NullString `db:"version"`
	Idempotent bool           `db:"idempotent"`
	Payload    []byte         `db:"payload"`
	CreatedAt  time.Time      `db:"created_at"`
	UpdatedAt  time.Time      `db:"updated_at"`
}

type TaskScheduled struct {
	Task
	DispatchAfter time.Time `db:"dispatch_after"`
	Priority      int       `db:"priority"`
	Retries       int       `db:"retries"`
}

func (TaskScheduled) ModelName() string {
	return "task_scheduled_" + VERSION
}

type TaskCompleted struct {
	Task
	CompletedAt time.Time `db:"completed_at"`
}

func (TaskCompleted) ModelName() string {
	return "task_completed_" + VERSION
}

type TaskDead struct {
	Task
	Error    string    `db:"error"`
	FailedAt time.Time `db:"failed_at"`
}

func (TaskDead) ModelName() string {
	return "task_dead_" + VERSION
}
