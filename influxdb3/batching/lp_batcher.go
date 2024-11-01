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
	EmitBytesCallback(ebcb func([]byte)) // callback for emitting bytes
}

type LPOption func(ByteEmittable)

// WithBufferSize changes the batch-size emitted by the LPbatcher
// The unit is byte
func WithBufferSize(size int) LPOption {
	return func(b ByteEmittable) {
		b.Size(size)
	}
}

// WithBufferCapacity changes the initial capacity of the internal buffer
// The unit is byte
func WithBufferCapacity(capacity int) LPOption {
	return func(b ByteEmittable) {
		b.Capacity(capacity)
	}
}

// WithByteEmitReadyCallback sets the function called when a new batch is ready. The
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
				slog.Warn(
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

func (lpb *LPBatcher) Ready() bool {
	lpb.Lock()
	defer lpb.Unlock()
	return lpb.isReady()
}

func (lpb *LPBatcher) isReady() bool {
	return len(lpb.buffer) >= lpb.size
}

// Emit returns a new batch of bytes with the provided batch size or with the
// remaining bytes. Please drain the bytes at the end of your processing to
// get the remaining bytes not filling up a batch.
func (lpb *LPBatcher) Emit() []byte {
	lpb.Lock()
	defer lpb.Unlock()

	return lpb.emitBytes()
}

func (lpb *LPBatcher) emitBytes() []byte {
	c := min(lpb.size, len(lpb.buffer))

	if c == 0 { // i.e. buffer is empty
		return lpb.buffer
	}

	prepacket := lpb.buffer[:c]
	lastLF := bytes.LastIndexByte(prepacket, '\n') + 1

	packet := lpb.buffer[:lastLF]
	lpb.buffer = lpb.buffer[len(packet):]

	return packet
}

// Flush drains all bytes even if buffer currently larger than size
func (lpb *LPBatcher) Flush() []byte {
	slog.Info(fmt.Sprintf("Flushing all bytes (%d) from buffer.", lpb.CurrentLoadSize()))
	packet := lpb.buffer
	lpb.buffer = lpb.buffer[len(packet):]
	return packet
}

func (lpb *LPBatcher) CurrentLoadSize() int {
	return len(lpb.buffer)
}
