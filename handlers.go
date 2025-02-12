package pgotask

import (
	"context"
	"encoding/json"
)

// Typed builds an handler that automatically handles the JSON unmarshaling
// of the provided type parameter
func Typed[T any](handler func(context.Context, DB, T) error) HandlerFn {
	return func(ctx context.Context, db DB, data json.RawMessage) error {
		var payload T
		if err := json.Unmarshal(data, &payload); err != nil {
			return err
		}

		return handler(ctx, db, payload)
	}
}

// NoDB builds an handler without [DB] connection
func NoDB(handler func(context.Context, json.RawMessage) error) HandlerFn {
	return func(ctx context.Context, db DB, data json.RawMessage) error {
		return handler(ctx, data)
	}
}

// TypedNoDB merges [Typed] and [NoDB]
func TypedNoDB[T any](handler func(context.Context, T) error) HandlerFn {
	return func(ctx context.Context, _ DB, data json.RawMessage) error {
		var payload T
		if err := json.Unmarshal(data, &payload); err != nil {
			return err
		}

		return handler(ctx, payload)
	}
}

// Simple builds an handler without DB connection nor payload
func Simple(handler func(context.Context) error) HandlerFn {
	return func(ctx context.Context, _ DB, _ json.RawMessage) error {
		return handler(ctx)
	}
}
