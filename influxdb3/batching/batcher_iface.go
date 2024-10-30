package batching

import (
	"github.com/InfluxCommunity/influxdb3-go/influxdb3"
)

// Option to adapt properties of a struct implementing BatcherIface
type Option func(BatcherIface)

type BatcherIface interface {
	Size(int)                              // setsize
	Capacity(int)                          // set capacity
	ReadyCallback(func())                  // ready Callback
	EmitCallback(func([]*influxdb3.Point)) // callback for emitting points
	EmitBytesCallback(func([]byte))        // callback for emitting bytes
}

// WithSize changes the batch-size emitted by the batcher
// With the standard Batcher the implied unit is a Point
// With the LPBatcher the implied unit is a byte
func WithSize(size int) Option {
	return func(b BatcherIface) {
		b.Size(size)
	}
}

// WithCapacity changes the initial capacity of the internal buffer
// With the standard Batcher implied unit is a Point
// With the LPBatcher the implied unit is a byte
func WithCapacity(capacity int) Option {
	return func(b BatcherIface) {
		b.Capacity(capacity)
	}
}

// WithReadyCallback sets the function called when a new batch is ready. The
// batcher will wait for the callback to finish, so please return as fast as
// possible and move long-running processing to a  go-routine.
func WithReadyCallback(f func()) Option {
	return func(b BatcherIface) {
		b.ReadyCallback(f)
	}

}

// WithEmitCallback sets the function called when a new batch is ready with the
// batch of points. The batcher will wait for the callback to finish, so please
// return as fast as possible and move long-running processing to a go-routine.
func WithEmitCallback(f func([]*influxdb3.Point)) Option {
	return func(b BatcherIface) {
		b.EmitCallback(f)
	}
}

func WithEmitBytesCallback(f func([]byte)) Option {
	return func(b BatcherIface) {
		b.EmitBytesCallback(f)
	}
}
