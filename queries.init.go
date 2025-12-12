package pgotask

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strings"
)

var initExtensionsQuery = /* sql */ `
CREATE EXTENSION "moddatetime";
CREATE EXTENSION "uuid-ossp";
`

var initTaskScheduledTableQuery = /* sql */ `
CREATE COLLATION en_natural (
  LOCALE = 'en-US-u-kn-true',
  PROVIDER = 'icu'
);

CREATE TABLE task_scheduled_v2 (
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

CREATE INDEX idx_task_scheduled_v2_pending
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

var initTaskDeadTableQuery = /* sql */ `
CREATE TABLE task_dead_v2 (
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

var initTaskCompletedTableQuery = /* sql */ `
CREATE TABLE task_completed_v2 (
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

var migrations = []string{
	initExtensionsQuery,
	initTaskScheduledTableQuery,
	initTaskDeadTableQuery,
	initTaskCompletedTableQuery,
}

var initTaskMigrationsQuery = /* sql */ `
CREATE TABLE task_migrations_v2 (
    version INT NOT NULL
);

INSERT INTO task_migrations_v2 VALUES (-1);
`

var checkMigrationsVersionQuery = /* sql */ `
SELECT version
FROM task_migrations_v2
LIMIT 1
FOR UPDATE NOWAIT
`

var setMigrationVersionQuery = /* sql */ `
UPDATE task_migrations_v2
SET version = $1
`

func initSchema(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, initTaskMigrationsQuery); err != nil && !strings.Contains(err.Error(), "42P07") {
		slog.ErrorContext(ctx, "Ensuring migration table failed",
			slog.String("stmt", initTaskMigrationsQuery),
			slog.String("err", err.Error()),
		)

		return errors.Join(ErrExecQuery, err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		slog.ErrorContext(ctx, "Init transaction creation failed",
			slog.String("err", err.Error()),
		)

		return errors.Join(ErrTxCreation, err)
	}
	defer tx.Rollback()

	var version int
	versionRow := tx.QueryRowContext(ctx, checkMigrationsVersionQuery)
	if err := versionRow.Scan(&version); err != nil {
		slog.ErrorContext(ctx, "Migration version fetching failed",
			slog.String("stmt", checkMigrationsVersionQuery),
			slog.String("err", err.Error()),
		)

		return errors.Join(ErrExecQuery, err)
	}

	for migrationVersion, migration := range migrations {
		if migrationVersion <= version {
			continue
		}

		if _, err := tx.ExecContext(ctx, migration); err != nil {
			slog.ErrorContext(ctx, "Migration failed",
				slog.String("migration", migration),
				slog.String("err", err.Error()),
				slog.Int("migrationVersion", migrationVersion),
			)

			return errors.Join(ErrExecQuery, err)
		}
	}

	newVersion := len(migrations) - 1
	if _, err := tx.ExecContext(ctx, setMigrationVersionQuery, newVersion); err != nil {
		slog.ErrorContext(ctx, "Migration version flagging failed",
			slog.String("stmt", setMigrationVersionQuery),
			slog.String("err", err.Error()),
			slog.Int("$1", newVersion),
		)

		return errors.Join(ErrExecQuery, err)
	}

	if err := tx.Commit(); err != nil {
		slog.ErrorContext(ctx, "Transaction failed",
			slog.String("err", err.Error()),
		)

		return errors.Join(ErrTxCommit, err)
	}

	return nil
}
