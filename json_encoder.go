package pqueue

import (
	"encoding/json"
)

// JsonEncoder is the default encoder for the queue.
type JsonEncoder struct{}

// Encode serializes provided value to JSON.
func (*JsonEncoder) Encode(v any) ([]byte, error) {
	return json.Marshal(v) //nolint:wrapcheck // proxy error
}
