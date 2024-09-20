package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/taleeus/pgotask"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/sync/errgroup"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

var _COUNTER = atomic.Int32{}

var TASK_TYPE = "COUNTER"

type counterPayload struct {
	Increment int `json:"increment"`
}

func incrementCounter(_ context.Context, _ *sql.Tx, data json.RawMessage) error {
	var payload counterPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}

	_COUNTER.Add(int32(payload.Increment))
	return nil
}

func Test(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	pgContainer, err := postgres.Run(ctx,
		"postgres:15.3-alpine",
		postgres.WithDatabase("test-db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second),
		),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate pgContainer: %v", err)
		}
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}

	pool, err := pgxpool.New(context.Background(), connStr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
	})

	s1 := pgotask.NewScheduler(stdlib.OpenDBFromPool(pool)).
		Cooldown(time.Second).
		Handler(TASK_TYPE, incrementCounter)
	s2 := pgotask.NewScheduler(stdlib.OpenDBFromPool(pool)).
		Cooldown(time.Second).
		Handler(TASK_TYPE, incrementCounter)
	s3 := pgotask.NewScheduler(stdlib.OpenDBFromPool(pool)).
		Cooldown(time.Second).
		Handler(TASK_TYPE, incrementCounter)
	s4 := pgotask.NewScheduler(stdlib.OpenDBFromPool(pool)).
		Cooldown(time.Second).
		Handler(TASK_TYPE, incrementCounter)

	var runGroup errgroup.Group
	runGroup.Go(func() error {
		return s1.Run(ctx)
	})
	runGroup.Go(func() error {
		return s2.Run(ctx)
	})
	runGroup.Go(func() error {
		return s3.Run(ctx)
	})
	runGroup.Go(func() error {
		return s4.Run(ctx)
	})

	if err := runGroup.Wait(); err != nil {
		t.Fatal(err)
	}

	tasksNum := 5
	for i := range tasksNum {
		payload := counterPayload{1}
		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}

		if err := s1.ScheduleTask(ctx, pgotask.TaskArgs{
			TaskType:      TASK_TYPE,
			Payload:       data,
			DispatchAfter: time.Second * time.Duration(i+1),
		}); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(time.Second * time.Duration(tasksNum+2))
	if val := _COUNTER.Load(); int(val) != tasksNum {
		t.Errorf("counter is not %d, but %d", tasksNum, val)
		rows, err := pool.Query(ctx, "SELECT * FROM task_v1")
		if err != nil {
			t.Fatal("tasks dump failed", err)
		}
		defer rows.Close()

		tasks := make([]pgotask.Task, 0)
		for rows.Next() {
			var task pgotask.Task
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
				t.Fatal("task scan failed", err)
			}

			tasks = append(tasks, task)
		}

		tasksJson, err := json.Marshal(tasks)
		if err != nil {
			t.Fatal("tasks marshaling failed", err)
		}

		slog.DebugContext(ctx, "Tasks dump",
			slog.String("tasks", string(tasksJson)),
		)
	}
}
