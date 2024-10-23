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
	"sync"

	"github.com/InfluxCommunity/influxdb3-go/influxdb3"
)

const (
	BatchUnknown = iota
	BatchPoints  = iota
	BatchLP      = iota
)

// Option to adapt properties of a batcher
type PBOption func(*PointBatcher)

// WithSize changes the batch-size emitted by the batcher
func WithSize(size int) PBOption {
	return func(b *PointBatcher) {
		b.size = size
	}
}

// WithCapacity changes the initial capacity of the points buffer
func WithCapacity(capacity int) PBOption {
	return func(b *PointBatcher) {
		b.capacity = capacity
	}
}

// WithReadyCallback sets the function called when a new batch is ready. The
// batcher will wait for the callback to finish, so please return as fast as
// possible and move long-running processing to a  go-routine.
func WithReadyCallback(f func()) PBOption {
	return func(b *PointBatcher) {
		b.callbackReady = f
	}
}

// WithEmitPointsCallback sets the function called when a new batch is ready with the
// batch of points. The batcher will wait for the callback to finish, so please
// return as fast as possible and move long-running processing to a go-routine.
func WithEmitPointsCallback(f func([]*influxdb3.Point)) PBOption {
	return func(b *PointBatcher) {
		b.callbackEmit = f
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
	callbackEmit  func([]any)
}

// Batcher collects points and emits them as batches
type PointBatcher struct {
	BaseBatcher

	callbackEmit func([]*influxdb3.Point)

	points []*influxdb3.Point
	sync.Mutex
}

// NewBatcher creates and initializes a new Batcher instance applying the
// specified options. By default, a batch-size is DefaultBatchSize and the
// initial capacity is DefaultCapacity.
func NewPointBatcher(options ...PBOption) *PointBatcher {
	// Set up a batcher with the default values
	base := BaseBatcher{
		size:     DefaultBatchSize,
		capacity: DefaultCapacity,
	}
	b := &PointBatcher{
		BaseBatcher: base,
	}

	// Apply the options
	for _, o := range options {
		o(b)
	}

	// setup internal data
	b.points = make([]*influxdb3.Point, 0, b.capacity)

	return b
}

// Add metric(s) to the batcher and call the given callbacks if any
func (b *PointBatcher) Add(p ...*influxdb3.Point) {
	b.Lock()
	defer b.Unlock()

	// Add the point
	b.points = append(b.points, p...)
	//b.addToBuffer(interfaces...)

	// Call callbacks if a new batch is ready
	if b.isReady() {
		if b.callbackReady != nil {
			b.callbackReady()
		}
		if b.callbackEmit != nil {
			b.callbackEmit(b.emitPoints())
		}
	}
}

// Ready tells the call if a new batch is ready to be emitted
func (b *PointBatcher) Ready() bool {
	b.Lock()
	defer b.Unlock()
	return b.isReady()
}

func (b *PointBatcher) isReady() bool {
	return len(b.points) >= b.size
}

// Emit returns a new batch of points with the provided batch size or with the
// remaining points. Please drain the points at the end of your processing to
// get the remaining points not filling up a batch.
func (b *PointBatcher) Emit() []*influxdb3.Point {
	b.Lock()
	defer b.Unlock()

	return b.emitPoints()
}

func (b *PointBatcher) emitPoints() []*influxdb3.Point {
	l := min(b.size, len(b.points))

	points := b.points[:l]
	b.points = b.points[l:]

	return points
}

type LPOption func(*LPBatcher)

func WithBufferSize(size int) LPOption {
	return func(b *LPBatcher) {
		b.size = size
	}
}

func WithBufferCapacity(capacity int) LPOption {
	return func(b *LPBatcher) {
		b.capacity = capacity
	}
}

func WithEmitBytesCallback(f func([]byte)) LPOption {
	return func(b *LPBatcher) {
		b.callbackEmit = f
	}
}

type LPBatcher struct {
	BaseBatcher

	callbackEmit func([]byte)

	buffer []byte
	sync.Mutex
}

func NewLPBatcher(options ...LPOption) *LPBatcher {
	base := BaseBatcher{
		size:     DefaultBatchSize,
		capacity: DefaultCapacity,
	}
	l := &LPBatcher{
		BaseBatcher: base,
	}

	// Apply the options
	for _, o := range options {
		o(l)
	}

	// setup internal data
	l.buffer = make([]byte, 0, l.capacity)
	return l
}

func (l *LPBatcher) Add(lines ...string) {
	l.Lock()
	defer l.Unlock()

	for _, line := range lines {
		if len(line) != 0 { // ignore empty lines
			l.buffer = append(l.buffer, line...)
			if line[len(line)-1] != '\n' {
				l.buffer = append(l.buffer, '\n')
			}
		}
	}

	if l.isReady() {
		// TODO callback ready should be checked here
		if l.callbackEmit != nil {
			l.callbackEmit(l.emitBytes())
		}
	}
}

func (l *LPBatcher) Ready() bool {
	l.Lock()
	defer l.Unlock()
	return l.isReady()
}

func (l *LPBatcher) isReady() bool {
	return len(l.buffer) >= l.size
}

func (b *LPBatcher) Emit() []byte {
	b.Lock()
	defer b.Unlock()

	return b.emitBytes()
}

func (l *LPBatcher) emitBytes() []byte {
	c := min(l.size, len(l.buffer))

	packet := l.buffer[:c]
	l.buffer = l.buffer[c:]

	return packet
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
