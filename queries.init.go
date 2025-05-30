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

var initTaskScheduledTableQuery = `
CREATE COLLATION IF NOT EXISTS en_natural (
  LOCALE = 'en-US-u-kn-true',
  PROVIDER = 'icu'
);

CREATE TABLE IF NOT EXISTS task_scheduled_v2 (
	id 				UUID 		NOT NULL 	DEFAULT uuid_generate_v4()	PRIMARY KEY,
	type 			TEXT 		NOT NULL,
	version			TEXT 					COLLATE en_natural,
	idempotent 		BOOLEAN 	NOT NULL 	DEFAULT FALSE,
	payload 		JSONB,
	created_at 		TIMESTAMP	NOT NULL 	DEFAULT CURRENT_TIMESTAMP,
	updated_at 		TIMESTAMP 	NOT NULL 	DEFAULT CURRENT_TIMESTAMP,

	priority		INT 		NOT NULL 	DEFAULT 0,
	retries			INT 		NOT NULL 	DEFAULT 0,
	dispatch_after 	TIMESTAMP	NOT NULL 	DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_task_scheduled_v2_pending
ON task_scheduled_v2 (
	type,
	version,
	dispatch_after
);

CREATE OR REPLACE TRIGGER mdt_task_scheduled_v2
	BEFORE UPDATE ON task_scheduled_v2
	FOR EACH ROW
	EXECUTE PROCEDURE moddatetime (updated_at);
`

var initTaskDeadTableQuery = `
CREATE TABLE IF NOT EXISTS task_dead_v2 (
	id 				UUID 		NOT NULL 	PRIMARY KEY,
	type 			TEXT 		NOT NULL,
	version			TEXT 					COLLATE en_natural,
	idempotent 		BOOLEAN 	NOT NULL,
	payload 		JSON,
	created_at 		TIMESTAMP	NOT NULL,
	updated_at 		TIMESTAMP 	NOT NULL,

	error 			TEXT 		NOT NULL,
	failed_at 		TIMESTAMP 	NOT NULL 	DEFAULT CURRENT_TIMESTAMP
);
`

var initTaskCompletedTableQuery = `
CREATE TABLE IF NOT EXISTS task_completed_v2 (
	id 				UUID 		NOT NULL	PRIMARY KEY,
	type 			TEXT 		NOT NULL,
	version			TEXT 					COLLATE en_natural,
	idempotent 		BOOLEAN 	NOT NULL,
	payload 		JSON,
	created_at 		TIMESTAMP	NOT NULL,
	updated_at 		TIMESTAMP 	NOT NULL,

	completed_at 	TIMESTAMP 	NOT NULL 	DEFAULT CURRENT_TIMESTAMP
);
`

func initSchema(ctx context.Context, db *sql.DB) error {
	query := initExtensionsQuery + initTaskScheduledTableQuery + initTaskDeadTableQuery + initTaskCompletedTableQuery
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

			if strings.Contains(err.Error(), "42P07") {
				slog.WarnContext(ctx, "Relation already exists; continuing")
				continue
			}

			return errors.Join(ErrExecQuery, err)
		}
	}

	return nil
}
