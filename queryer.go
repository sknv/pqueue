package pqueue

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// QueryRower is satisfied by *pgxpool.Pool, *pgx.Conn, and pgx.Tx.
type QueryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// BatchSender is satisfied by *pgxpool.Pool, *pgx.Conn, and pgx.Tx.
type BatchSender interface {
	SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults
}
