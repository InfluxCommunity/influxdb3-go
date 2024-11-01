package batching

import (
	"bytes"
	"fmt"
	"log/slog"
	"sync"
)

const DefaultBufferSize = 100000
const DefaultBufferCapacity = DefaultBufferSize * 2

type ByteEmittable interface {
	Emittable
	EmitBytesCallback(func([]byte)) // callback for emitting bytes
}

type LPOption func(ByteEmittable)

// WithSize changes the batch-size emitted by the batcher
// With the standard Batcher the implied unit is a Point
// With the LPBatcher the implied unit is a byte
func WithBufferSize(size int) LPOption {
	return func(b ByteEmittable) {
		b.Size(size)
	}
}

// WithCapacity changes the initial capacity of the internal buffer
// With the standard Batcher implied unit is a Point
// With the LPBatcher the implied unit is a byte
func WithBufferCapacity(capacity int) LPOption {
	return func(b ByteEmittable) {
		b.Capacity(capacity)
	}
}

// WithReadyCallback sets the function called when a new batch is ready. The
// batcher will wait for the callback to finish, so please return as fast as
// possible and move long-running processing to a  go-routine.
func WithByteEmitReadyCallback(f func()) LPOption {
	return func(b ByteEmittable) {
		b.ReadyCallback(f)
	}
}

func WithEmitBytesCallback(f func([]byte)) LPOption {
	return func(b ByteEmittable) {
		b.EmitBytesCallback(f)
	}
}

type LPBatcher struct {
	size     int
	capacity int

	callbackReady    func()
	callbackByteEmit func([]byte)

	buffer []byte
	sync.Mutex
}

func (lpb *LPBatcher) Size(s int) {
	lpb.size = s
}

func (lpb *LPBatcher) Capacity(c int) {
	lpb.capacity = c
}

func (lpb *LPBatcher) ReadyCallback(f func()) {
	lpb.callbackReady = f
}

func (lpb *LPBatcher) EmitBytesCallback(f func([]byte)) {
	lpb.callbackByteEmit = f
}

func NewLPBatcher(options ...LPOption) *LPBatcher {
	lpb := &LPBatcher{
		size:     DefaultBufferSize,
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

func (l *LPBatcher) Add(lines ...string) {
	l.Lock()
	defer l.Unlock()

	for _, line := range lines {
		if len(line) != 0 { // ignore empty lines
			l.buffer = append(l.buffer, line...)
			if line[len(line)-1] != '\n' { //ensure newline demarcation
				l.buffer = append(l.buffer, '\n')
			}
		}
	}

	for l.isReady() {
		if l.callbackReady != nil {
			l.callbackReady()
		}
		if l.callbackByteEmit != nil {
			l.callbackByteEmit(l.emitBytes())
		} else {
			// no emitter callback
			if l.CurrentLoadSize() > (l.capacity - l.size) {
				slog.Warn(
					fmt.Sprintf("Batcher is ready, but no callbackByteEmit is available.  "+
						"Batcher load is %d bytes waiting to be emitted.",
						l.CurrentLoadSize()),
				)
			}
			break

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

// Emit returns a new batch of bytes with the provided batch size or with the
// remaining bytes. Please drain the bytes at the end of your processing to
// get the remaining bytes not filling up a batch.
func (l *LPBatcher) Emit() []byte {
	l.Lock()
	defer l.Unlock()

	return l.emitBytes()
}

func (l *LPBatcher) emitBytes() []byte {
	c := min(l.size, len(l.buffer))

	if c == 0 { // i.e. buffer is empty
		return l.buffer
	}

	prepacket := l.buffer[:c]
	lastLF := bytes.LastIndexByte(prepacket, '\n') + 1

	packet := l.buffer[:lastLF]
	l.buffer = l.buffer[len(packet):]

	return packet
}

// Flush drains all bytes even if buffer currently larger than size
func (l *LPBatcher) Flush() []byte {
	slog.Info(fmt.Sprintf("Flushing all bytes (%d) from buffer.", l.CurrentLoadSize()))
	packet := l.buffer
	l.buffer = l.buffer[len(packet):]
	return packet
}

func (l *LPBatcher) CurrentLoadSize() int {
	return len(l.buffer)
}
