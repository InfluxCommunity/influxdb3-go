/*
The MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

// Package batching provides a batcher to collect points and emit them as batches.
package batching

import (
	"log/slog"
	"sync"

	"github.com/InfluxCommunity/influxdb3-go/influxdb3"
)

const (
	BatchUnknown = iota
	BatchPoints  = iota
	BatchLP      = iota
)

type Option func(*interface{})

// Option to adapt properties of a batcher
type PBOption func(*Batcher)

// WithSize changes the batch-size emitted by the batcher
/*func WithSize(size int) PBOption {
	return func(b *Batcher) {
		b.size = size
	}
}*/

func WithSize(size int) Option {
	return func(b *interface{}) {
		if bb, bok := (*b).(*Batcher); bok {
			bb.size = size
		} else if lb, lok := (*b).(*LPBatcher); lok {
			lb.size = size
		} else {
			slog.Warn("Failed to match Batcher in WithCapacityOpt. Value not set.")
		}
	}
}

// WithCapacity changes the initial capacity of the points buffer
/*func WithCapacity(capacity int) PBOption {
	return func(b *Batcher) {
		b.capacity = capacity
	}
}*/

func WithCapacity(capacity int) Option {
	return func(b *interface{}) {
		if bb, bok := (*b).(*Batcher); bok {
			bb.capacity = capacity
		} else if lb, lok := (*b).(*LPBatcher); lok {
			lb.capacity = capacity
		} else {
			slog.Warn("Failed to match Batcher in WithCapacityOpt. Value not set.")
		}
	}
}

// WithReadyCallback sets the function called when a new batch is ready. The
// batcher will wait for the callback to finish, so please return as fast as
// possible and move long-running processing to a  go-routine.
/*func WithReadyCallback(f func()) PBOption {
	return func(b *Batcher) {
		b.callbackReady = f
	}
}*/

func WithReadyCallback(f func()) Option {
	return func(b *interface{}) {
		if bb, bok := (*b).(*Batcher); bok {
			bb.callbackReady = f
		} else if lb, lok := (*b).(*LPBatcher); lok {
			lb.callbackReady = f
		} else {
			slog.Warn("Failed to match Batcher in WithReadyCallbackOpt. Callback not set.")
		}
	}

}

// WithEmitPointsCallback sets the function called when a new batch is ready with the
// batch of points. The batcher will wait for the callback to finish, so please
// return as fast as possible and move long-running processing to a go-routine.
/*func WithEmitPointsCallback(f func([]*influxdb3.Point)) PBOption {
	return func(b *Batcher) {
		b.callbackEmit = f
	}
}*/

func WithEmitPointsCallback(f func([]*influxdb3.Point)) Option {
	return func(b *interface{}) {
		if bb, bok := (*b).(*Batcher); bok {
			bb.callbackEmit = f
		} else {
			slog.Warn("Failed to match Batcher in WithEmitPointsCallbackOpt. Callback not set.")
		}
	}
}

// DefaultBatchSize is the default number of points emitted
const DefaultBatchSize = 1000

// DefaultCapacity is the default initial capacity of the point buffer
const DefaultCapacity = 2 * DefaultBatchSize

type BaseBatcher struct {
	size     int
	capacity int

	callbackReady func()
}

// Batcher collects points and emits them as batches
type Batcher struct {
	BaseBatcher

	callbackEmit func([]*influxdb3.Point)

	points []*influxdb3.Point
	sync.Mutex
}

// NewBatcher creates and initializes a new Batcher instance applying the
// specified options. By default, a batch-size is DefaultBatchSize and the
// initial capacity is DefaultCapacity.
func NewBatcher(options ...Option) *Batcher {
	// Set up a batcher with the default values
	base := BaseBatcher{
		size:     DefaultBatchSize,
		capacity: DefaultCapacity,
	}
	b := &Batcher{
		BaseBatcher: base,
	}

	// Apply the options
	for _, o := range options {
		ptr2arg := interface{}(b)
		o(&ptr2arg)
	}

	// setup internal data
	b.points = make([]*influxdb3.Point, 0, b.capacity)

	return b
}

// Add metric(s) to the batcher and call the given callbacks if any
func (b *Batcher) Add(p ...*influxdb3.Point) {
	b.Lock()
	defer b.Unlock()

	// Add the point
	b.points = append(b.points, p...)
	//b.addToBuffer(interfaces...)

	// Call callbacks if a new batch is ready
	for b.isReady() {
		if b.callbackReady != nil {
			b.callbackReady()
		}
		if b.callbackEmit != nil {
			b.callbackEmit(b.emitPoints())
		} else {
			// no emitter callback
			break
		}
	}
}

// Ready tells the call if a new batch is ready to be emitted
func (b *Batcher) Ready() bool {
	b.Lock()
	defer b.Unlock()
	return b.isReady()
}

func (b *Batcher) isReady() bool {
	return len(b.points) >= b.size
}

// Emit returns a new batch of points with the provided batch size or with the
// remaining points. Please drain the points at the end of your processing to
// get the remaining points not filling up a batch.
func (b *Batcher) Emit() []*influxdb3.Point {
	b.Lock()
	defer b.Unlock()

	return b.emitPoints()
}

func (b *Batcher) emitPoints() []*influxdb3.Point {
	l := min(b.size, len(b.points))

	points := b.points[:l]
	b.points = b.points[l:]

	return points
}

/*
func (b *Batcher) addToBuffer(items ...*interface{}) {
	b.Lock()
	defer b.Unlock()

	b.buffer = append(b.buffer, items...)

	//Call callbacks if a new batch is ready
	if b.isReady() {
		if b.callbackReady != nil {
			b.callbackReady()
		}
		if b.callbackEmit != nil {
			// ??? and if its line protocol?
			b.callbackEmit(b.emitPoints())
		}
	}
}
*/
/*
func (b *Batcher) AddPoints(p ...*influxdb3.Point) error {
	//b.Lock()
	//defer b.Unlock()
	if b.idiom != BatchIdiomPoints {
		if len(b.buffer) == 0 {
			b.idiom = BatchIdiomPoints
		} else {
			return errors.New("this batcher does not support the Point idiom")
		}
	}
	interfaces := make([]*interface{}, len(p))
	// Add the point
	for i, point := range p {
		var iface interface{} = point
		interfaces[i] = &iface
	}
	//b.points = append(b.points, p...)
	b.addToBuffer(interfaces...)
	return nil
} */

/*
func (b *Batcher) AddLP(lines ...string) error {
	if b.idiom != BatchIdiomLP {
		if len(b.buffer) == 0 {
			b.idiom = BatchIdiomLP
		} else {
			return errors.New("this batcher does not support the Line Protocol (LP) idiom")
		}
	}
	interfaces := make([]*interface{}, len(lines))
	for n, line := range lines {
		var iface interface{} = line
		interfaces[n] = &iface
	}
	b.addToBuffer(interfaces...)
	return nil
} */
