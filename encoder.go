package pqueue

// Encoder marshals and unmarshals job payload.
type Encoder interface {
	Encode(v any) ([]byte, error)
	Decode(data []byte, v any) error
}

// Decoder unmarshals job payload.
type Decoder func(data []byte, v any) error
