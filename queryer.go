package pqueue

import (
	"context"

	"github.com/jackc/pgx/v5"
)

type QueryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}
