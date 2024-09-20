package pgotask

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strings"
)

var initExtensionsQuery = `
CREATE EXTENSION IF NOT EXISTS "moddatetime";
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
`

var initTaskTableQuery = `
CREATE TABLE IF NOT EXISTS ` + table[Task]() + ` (
	` + column[Task]("id") + ` UUID NOT NULL PRIMARY KEY DEFAULT uuid_generate_v4(),
	` + column[Task]("type") + ` TEXT NOT NULL,
	` + column[Task]("type_version") + ` INT DEFAULT 0,
	` + column[Task]("payload") + ` JSON,
	` + column[Task]("dispatch_after") + ` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	` + column[Task]("completed_at") + ` TIMESTAMP,
	` + column[Task]("created_at") + ` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	` + column[Task]("updated_at") + ` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_` + table[Task]() + `_pending ON ` + table[Task]() + ` (` + join(columns[Task](false,
	"type",
	"type_version",
	"dispatch_after",
)) + `)
WHERE
	` + column[Task]("completed_at") + ` IS NULL AND
	` + column[Task]("dispatch_after") + ` IS NOT NULL;

CREATE OR REPLACE TRIGGER mdt_` + table[Task]() + `
	BEFORE UPDATE ON ` + table[Task]() + `
	FOR EACH ROW
	EXECUTE PROCEDURE moddatetime (` + column[Task]("updated_at") + `);
`

var initTaskFailureTableQuery = `
CREATE TABLE IF NOT EXISTS ` + table[TaskFailure]() + ` (
	` + column[TaskFailure]("task_id") + ` UUID NOT NULL REFERENCES ` + table[Task]() + `(` + column[Task]("id") + `),
	` + column[TaskFailure]("message") + ` TEXT NOT NULL,
	` + column[TaskFailure]("created_at") + ` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

	PRIMARY KEY (` + column[TaskFailure]("task_id") + `, ` + column[TaskFailure]("created_at") + `)
);
`

func initSchema(ctx context.Context, db *sql.DB) error {
	query := initExtensionsQuery + initTaskTableQuery + initTaskFailureTableQuery
	for _, stmt := range strings.Split(query, ";") {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			slog.ErrorContext(ctx, "Statement failed",
				slog.String("stmt", stmt),
				slog.String("err", err.Error()),
			)

			if strings.Contains(err.Error(), "42710") {
				slog.WarnContext(ctx, "Unique constraint violation; continuing")
				continue
			}

			if strings.Contains(err.Error(), "23505") {
				slog.WarnContext(ctx, "Type already exists; continuing")
				continue
			}

			return errors.Join(ErrExecQuery, err)
		}
	}

	return nil
}
