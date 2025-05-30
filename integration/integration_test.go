package integration

import (
	"context"
	"encoding/json"
	"log"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/taleeus/pgotask/v2"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/sync/errgroup"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

var _COUNTER = atomic.Int32{}
var _COUNTER_IDEMPOTENT = atomic.Int32{}
var _COUNTER_NOPAYLOAD = atomic.Int32{}

const TASK_TYPE = "COUNTER"
const TASK_TYPE_IDEMPOTENT = TASK_TYPE + ".IDEMPOTENT"
const TASK_TYPE_NOPAYLOAD = TASK_TYPE + ".NOPAYLOAD"

type counterPayload struct {
	Increment int `json:"increment"`
}

func incrementCounter(ctx context.Context, payload counterPayload) error {
	_COUNTER.Add(int32(payload.Increment))
	return nil
}

func incrementCounterIdempotent(ctx context.Context, payload counterPayload) error {
	_COUNTER_IDEMPOTENT.Add(int32(payload.Increment))
	return nil
}

func incrementCounterNoPayload(ctx context.Context) error {
	_COUNTER_NOPAYLOAD.Add(1)
	return nil
}

var scheduler *pgotask.Scheduler
var ctx = context.Background()

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
		Handler(TASK_TYPE, pgotask.TypedNoDB(incrementCounter)).
		Handler(TASK_TYPE_NOPAYLOAD, pgotask.Simple(incrementCounterNoPayload)).
		Handler(TASK_TYPE_IDEMPOTENT, pgotask.TypedNoDB(incrementCounterIdempotent))
	s2 := pgotask.NewScheduler(stdlib.OpenDBFromPool(pool)).
		Cooldown(time.Second).
		Handler(TASK_TYPE, pgotask.TypedNoDB(incrementCounter)).
		Handler(TASK_TYPE_NOPAYLOAD, pgotask.Simple(incrementCounterNoPayload)).
		Handler(TASK_TYPE_IDEMPOTENT, pgotask.TypedNoDB(incrementCounterIdempotent))
	s3 := pgotask.NewScheduler(stdlib.OpenDBFromPool(pool)).
		Cooldown(time.Second).
		Handler(TASK_TYPE, pgotask.TypedNoDB(incrementCounter)).
		Handler(TASK_TYPE_NOPAYLOAD, pgotask.Simple(incrementCounterNoPayload)).
		Handler(TASK_TYPE_IDEMPOTENT, pgotask.TypedNoDB(incrementCounterIdempotent))
	s4 := pgotask.NewScheduler(stdlib.OpenDBFromPool(pool)).
		Cooldown(time.Second).
		Handler(TASK_TYPE, pgotask.TypedNoDB(incrementCounter)).
		Handler(TASK_TYPE_NOPAYLOAD, pgotask.Simple(incrementCounterNoPayload)).
		Handler(TASK_TYPE_IDEMPOTENT, pgotask.TypedNoDB(incrementCounterIdempotent))

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
			TaskType:      TASK_TYPE_IDEMPOTENT,
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

func TestNoPayload(t *testing.T) {
	tasksNum := 5
	for i := range tasksNum {
		if err := scheduler.ScheduleTask(ctx, pgotask.TaskArgs{
			TaskType:      TASK_TYPE_NOPAYLOAD,
			Idempotent:    true,
			DispatchAfter: time.Second * time.Duration(i+1),
		}); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(time.Second * time.Duration(tasksNum+2))
	if val := _COUNTER_NOPAYLOAD.Load(); int(val) != tasksNum {
		t.Errorf("counter is not %d, but %d", tasksNum, val)
	}
}
