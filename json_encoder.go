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

// Decode deserializes data from JSON to provided value.
func (*JsonEncoder) Decode(data []byte, v any) error {
	return json.Unmarshal(data, v) //nolint:wrapcheck // proxy error
}
