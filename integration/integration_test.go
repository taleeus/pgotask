package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"log/slog"
	"os"
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
var _COUNTER_IDEMPOTENT = atomic.Int32{}

const TASK_TYPE = "COUNTER"
const TASK_TYPE_IDEMPOTENT = TASK_TYPE + ".IDEMPOTENT"

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

func incrementCounterIdempotent(_ context.Context, _ *sql.Tx, data json.RawMessage) error {
	var payload counterPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}

	_COUNTER_IDEMPOTENT.Add(int32(payload.Increment))
	return nil
}

var ctx = context.Background()
var scheduler *pgotask.Scheduler

func TestMain(m *testing.M) {
	// init
	slog.SetLogLoggerLevel(slog.LevelDebug)
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
		log.Fatal(err)
	}

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	pool, err := pgxpool.New(context.Background(), connStr)
	if err != nil {
		log.Fatal(err)
	}

	s1 := pgotask.NewScheduler(stdlib.OpenDBFromPool(pool)).
		Cooldown(time.Second).
		Handler(TASK_TYPE, incrementCounter).
		Handler(TASK_TYPE_IDEMPOTENT, incrementCounterIdempotent)
	s2 := pgotask.NewScheduler(stdlib.OpenDBFromPool(pool)).
		Cooldown(time.Second).
		Handler(TASK_TYPE, incrementCounter).
		Handler(TASK_TYPE_IDEMPOTENT, incrementCounterIdempotent)
	s3 := pgotask.NewScheduler(stdlib.OpenDBFromPool(pool)).
		Cooldown(time.Second).
		Handler(TASK_TYPE, incrementCounter).
		Handler(TASK_TYPE_IDEMPOTENT, incrementCounterIdempotent)
	s4 := pgotask.NewScheduler(stdlib.OpenDBFromPool(pool)).
		Cooldown(time.Second).
		Handler(TASK_TYPE, incrementCounter).
		Handler(TASK_TYPE_IDEMPOTENT, incrementCounterIdempotent)

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
		log.Fatal(err)
	}
	scheduler = s1

	// run tests
	exitVal := m.Run()

	// cleanup
	pool.Close()
	if err := pgContainer.Terminate(ctx); err != nil {
		log.Fatalf("failed to terminate pgContainer: %v", err)
	}

	os.Exit(exitVal)
}

func TestCounter(t *testing.T) {
	tasksNum := 5
	for i := range tasksNum {
		payload := counterPayload{1}
		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}

		if err := scheduler.ScheduleTask(ctx, pgotask.TaskArgs{
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
	}
}

func TestIdempotent(t *testing.T) {
	tasksNum := 5
	for i := range tasksNum {
		payload := counterPayload{1}
		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}

		if err := scheduler.ScheduleTask(ctx, pgotask.TaskArgs{
			TaskType:      TASK_TYPE + ".IDEMPOTENT",
			Payload:       data,
			Idempotent:    true,
			DispatchAfter: time.Second * time.Duration(i+1),
		}); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(time.Second * time.Duration(tasksNum+2))
	if val := _COUNTER_IDEMPOTENT.Load(); int(val) != 1 {
		t.Errorf("counter is not 1, but %d", val)
	}
}
