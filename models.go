package pgotask

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Task struct {
	ID            uuid.UUID       `db:"id"`
	Type          string          `db:"type"`
	TypeVersion   int             `db:"type_version"`
	Payload       json.RawMessage `db:"payload"`
	Idempotent    bool            `db:"idempotent"`
	DispatchAfter sql.NullTime    `db:"dispatch_after"`
	CompletedAt   sql.NullTime    `db:"completed_at"`
	CreatedAt     time.Time       `db:"created_at"`
	UpdatedAt     time.Time       `db:"updated_at"`
}

func (Task) TableName() string {
	return "task_" + VERSION
}

type TaskFailure struct {
	TaskID    uuid.UUID `db:"task_id"`
	Message   string    `db:"message"`
	CreatedAt time.Time `db:"created_at"`
}

func (TaskFailure) TableName() string {
	return "task_failure_" + VERSION
}
