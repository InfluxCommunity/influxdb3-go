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
	"fmt"
	"log/slog"
	"sync"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
)

// DefaultBatchSize is the default number of points emitted
const DefaultBatchSize = 1000

// DefaultInitialCapacity is the default initial capacity of the point buffer
const DefaultInitialCapacity = 2 * DefaultBatchSize

// Emittable provides the base for any type
// that will collect and then emit data upon
// reaching a ready state.
type Emittable interface {
	SetSize(s int)               // setsize
	SetInitialCapacity(c int)    // set capacity
	SetReadyCallback(rcb func()) // ready Callback
	SetCapacity(c int)           // Deprecated: use SetInitialCapacity instead
}

// PointEmittable provides the basis for any type emitting
// Point arrays as []*influxdb3.Point
type PointEmittable interface {
	Emittable
	SetEmitCallback(epcb func([]*influxdb3.Point)) // callback for emitting points
}

type Option func(PointEmittable)

// WithSize changes the batch-size emitted by the batcher
func WithSize(size int) Option {
	return func(b PointEmittable) {
		b.SetSize(size)
	}
}

// WithInitialCapacity changes the initial capacity of the internal buffer
func WithInitialCapacity(capacity int) Option {
	return func(b PointEmittable) {
		b.SetInitialCapacity(capacity)
	}
}

// WithCapacity changes the initial capacity of the internal buffer
//
// Deprecated: use WithInitialCapacity instead
func WithCapacity(capacity int) Option {
	return func(b PointEmittable) {
		b.SetCapacity(capacity)
	}
}

// WithReadyCallback sets the function called when a new batch is ready. The
// batcher will wait for the callback to finish, so please return as fast as
// possible and move long-running processing to a  go-routine.
func WithReadyCallback(f func()) Option {
	return func(b PointEmittable) {
		b.SetReadyCallback(f)
	}
}

// WithEmitCallback sets the function called when a new batch is ready with the
// batch of points. The batcher will wait for the callback to finish, so please
// return as fast as possible and move long-running processing to a go-routine.
func WithEmitCallback(f func([]*influxdb3.Point)) Option {
	return func(b PointEmittable) {
		b.SetEmitCallback(f)
	}
}

// Batcher collects points and emits them as batches
type Batcher struct {
	size            int
	initialCapacity int
	callbackReady   func()
	callbackEmit    func([]*influxdb3.Point)

	points []*influxdb3.Point
	sync.Mutex
}

// NewBatcher creates and initializes a new Batcher instance applying the
// specified options. By default, a batch-size is DefaultBatchSize and the
// initial capacity is DefaultCapacity.
func NewBatcher(options ...Option) *Batcher {
	// Set up a batcher with the default values
	b := &Batcher{
		size:            DefaultBatchSize,
		initialCapacity: DefaultInitialCapacity,
	}

	// Apply the options
	for _, o := range options {
		o(b)
	}

	// setup internal data
	b.points = make([]*influxdb3.Point, 0, b.initialCapacity)

	return b
}

// SetSize sets the batch size.  Units are Points.
func (b *Batcher) SetSize(s int) {
	b.size = s
}

// SetInitialCapacity sets the initial Capacity of the internal []*influxdb3.Point buffer.
func (b *Batcher) SetInitialCapacity(c int) {
	b.initialCapacity = c
}

// SetCapacity sets the initial Capacity of the internal []*influxdb3.Point buffer.
//
// Deprecated: use SetInitialCapacity instead.
func (b *Batcher) SetCapacity(c int) {
	b.initialCapacity = c
}

// SetReadyCallback sets the callbackReady function.
func (b *Batcher) SetReadyCallback(f func()) {
	b.callbackReady = f
}

// SetEmitCallback sets the callbackEmit function.
func (b *Batcher) SetEmitCallback(f func([]*influxdb3.Point)) {
	b.callbackEmit = f
}

// Add metric(s) to the batcher and call the given callbacks if any
func (b *Batcher) Add(p ...*influxdb3.Point) {
	b.Lock()
	defer b.Unlock()

	// Add the point
	b.points = append(b.points, p...)

	// Call callbacks if a new batch is ready
	for b.isReady() {
		if b.callbackReady != nil {
			b.callbackReady()
		}
		if b.callbackEmit == nil {
			// no emitter callback
			if b.CurrentLoadSize() >= (b.initialCapacity - b.size) {
				slog.Debug(
					fmt.Sprintf("Batcher load is %d points waiting to be emitted.",
						b.CurrentLoadSize()),
				)
			}
			break
		}
		b.callbackEmit(b.emitPoints())
	}
}

// Ready tells the call if a new batch is ready to be emitted
func (b *Batcher) Ready() bool {
	b.Lock()
	defer b.Unlock()
	return b.isReady()
}

// Flush drains all points even if the internal buffer is currently larger than size.
// It does not call the callbackEmit method
func (b *Batcher) Flush() []*influxdb3.Point {
	points := b.points
	b.points = b.points[:0]
	return points
}

func (b *Batcher) CurrentLoadSize() int {
	return len(b.points)
}

// Emit returns a new batch of points with the provided batch size or with the
// remaining points. Please drain the points at the end of your processing to
// get the remaining points not filling up a batch.
func (b *Batcher) Emit() []*influxdb3.Point {
	b.Lock()
	defer b.Unlock()

	return b.emitPoints()
}

func (b *Batcher) isReady() bool {
	return len(b.points) >= b.size
}

func (b *Batcher) emitPoints() []*influxdb3.Point {
	l := min(b.size, len(b.points))

	points := b.points[:l]
	b.points = b.points[l:]

	return points
}
