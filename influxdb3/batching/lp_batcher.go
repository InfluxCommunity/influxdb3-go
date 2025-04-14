package batching

import (
	"bytes"
	"fmt"
	"log/slog"
	"sync"
)

const DefaultByteBatchSize = 100000
const DefaultBufferCapacity = DefaultByteBatchSize * 2

// ByteEmittable provides the basis for a type Emitting line protocol data
// as a byte array (i.e. []byte).
type ByteEmittable interface {
	Emittable
	SetEmitBytesCallback(ebcb func([]byte)) // callback for emitting bytes
}

type LPOption func(ByteEmittable)

// WithBufferSize changes the batch-size emitted by the LPbatcher
// The unit is byte
func WithBufferSize(size int) LPOption {
	return func(b ByteEmittable) {
		b.SetSize(size)
	}
}

// WithBufferCapacity changes the initial capacity of the internal buffer
// The unit is byte
func WithBufferCapacity(capacity int) LPOption {
	return func(b ByteEmittable) {
		b.SetCapacity(capacity)
	}
}

// WithByteEmitReadyCallback sets the function called when a new batch is ready. The
// batcher will wait for the callback to finish, so please return as fast as
// possible and move long-running processing to a  go-routine.
func WithByteEmitReadyCallback(f func()) LPOption {
	return func(b ByteEmittable) {
		b.SetReadyCallback(f)
	}
}

// WithEmitBytesCallback sets the function called when a new batch is ready
// with the batch bytes. The batcher will wait for the callback to finish, so please
// return as quickly as possible and move any long-running processing to a go routine.
func WithEmitBytesCallback(f func([]byte)) LPOption {
	return func(b ByteEmittable) {
		b.SetEmitBytesCallback(f)
	}
}

// LPBatcher collects line protocol strings storing them
// to a byte buffer and then emitting them as []byte.
//
// Lines are added to the LPBatcher using the `Add()` method.
// Lines in the internal buffer are delimited by a '\n' byte,
// which is added automatically, if not already used to terminate
// a line.
//
// As lines are added to LPBatcher a check is made to determine
// whether the `size` property has been exceeded.  At that point
// the function `callbackByteEmit()` is automatically called using
// the internal `emitBytes()` method.
//
// In the most common use case, a response batch packet of lines
// is emitted up to but not exceeding the `size` property.
// When the first line in the buffer exceeds this property,
// only that line is emitted.
type LPBatcher struct {
	size     int
	capacity int

	callbackReady    func()
	callbackByteEmit func([]byte)

	buffer []byte
	sync.Mutex
}

// SetSize sets the batch size of the batcher
func (lpb *LPBatcher) SetSize(s int) {
	lpb.size = s
}

// SetCapacity sets the initial capacity of the internal buffer
func (lpb *LPBatcher) SetCapacity(c int) {
	lpb.capacity = c
}

// SetReadyCallback sets the ReadyCallback function
func (lpb *LPBatcher) SetReadyCallback(f func()) {
	lpb.callbackReady = f
}

// SetEmitBytesCallback sets the callbackByteEmit function
func (lpb *LPBatcher) SetEmitBytesCallback(f func([]byte)) {
	lpb.callbackByteEmit = f
}

// NewLPBatcher creates and initializes a new LPBatcher instance
// applying the supplied options. By default a batch size is DefaultByteBatchSize
// and the initial capacity is the DefaultBufferCapacity.
func NewLPBatcher(options ...LPOption) *LPBatcher {
	lpb := &LPBatcher{
		size:     DefaultByteBatchSize,
		capacity: DefaultBufferCapacity,
	}

	// Apply the options
	for _, o := range options {
		o(lpb)
	}

	// setup internal data
	lpb.buffer = make([]byte, 0, lpb.capacity)
	return lpb
}

// Add lines to the buffer and call appropriate callbacks when
// the ready state is reached.
func (lpb *LPBatcher) Add(lines ...string) {
	lpb.Lock()
	defer lpb.Unlock()

	for _, line := range lines {
		if len(line) != 0 { // ignore empty lines
			lpb.buffer = append(lpb.buffer, line...)
			if line[len(line)-1] != '\n' { // ensure newline demarcation
				lpb.buffer = append(lpb.buffer, '\n')
			}
		}
	}

	for lpb.isReady() {
		if lpb.callbackReady != nil {
			lpb.callbackReady()
		}
		if lpb.callbackByteEmit == nil {
			// no emitter callback
			if lpb.CurrentLoadSize() > (lpb.capacity - lpb.size) {
				slog.Debug(
					fmt.Sprintf("Batcher is ready, but no callbackByteEmit is available.  "+
						"Batcher load is %d bytes waiting to be emitted.",
						lpb.CurrentLoadSize()),
				)
			}
			break
		}
		lpb.callbackByteEmit(lpb.emitBytes())
	}
}

// Ready reports when the ready state is reached.
func (lpb *LPBatcher) Ready() bool {
	lpb.Lock()
	defer lpb.Unlock()
	return lpb.isReady()
}

func (lpb *LPBatcher) isReady() bool {
	return len(lpb.buffer) >= lpb.size
}

// Emit returns a new batch of bytes with upto to the provided batch size
// depending on when the last newline character in the potential batch is met, or
// with all the remaining bytes. Please drain the bytes at the end of your
// processing to get the remaining bytes not filling up a batch.
func (lpb *LPBatcher) Emit() []byte {
	lpb.Lock()
	defer lpb.Unlock()

	return lpb.emitBytes()
}

func (lpb *LPBatcher) emitBytes() []byte {
	firstLF := bytes.IndexByte(lpb.buffer, '\n')

	var packet []byte

	c := min(lpb.size, len(lpb.buffer))

	if c == 0 { // i.e. buffer is empty
		return lpb.buffer
	}

	// With first line larger than defined size
	// just emit first line
	if firstLF > lpb.size {
		packet = lpb.buffer[:firstLF]
		lpb.buffer = lpb.buffer[len(packet)+1:] // remove trailing '\n'
		return packet
	}

	// otherwise: process buffer where len(buffer) > size with multiple lines

	prepacket := lpb.buffer[:c]
	lastLF := bytes.LastIndexByte(prepacket, '\n') + 1

	packet = lpb.buffer[:lastLF]
	lpb.buffer = lpb.buffer[len(packet):]

	return packet
}

// Flush drains all bytes even if buffer currently larger than size
func (lpb *LPBatcher) Flush() []byte {
	packet := lpb.buffer
	lpb.buffer = lpb.buffer[:0]
	return packet
}

// CurrentLoadSize returns the current size of the internal buffer
func (lpb *LPBatcher) CurrentLoadSize() int {
	return len(lpb.buffer)
}
