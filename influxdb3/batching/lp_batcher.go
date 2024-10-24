package batching

import (
	"bytes"
	"sync"
)

const DefaultBufferSize = 100000
const DefaultBufferCapacity = DefaultBufferSize * 2

//type LPOption func(*LPBatcher)

/*
func WithBufferSize(size int) func(*interface{}) {
	return func(b *interface{}) {
		if lpb, ok := (*b).(*LPBatcher); ok {
			lpb.size = size
		}
	}
}

func WithBufferCapacity(capacity int) Option {
	return func(b *interface{}) {
		if lpb, ok := (*b).(*LPBatcher); ok {
			lpb.capacity = capacity
		}
	}
}
*/

func WithEmitBytesCallback(f func([]byte)) Option {
	return func(b *interface{}) {
		if lpb, ok := (*b).(*LPBatcher); ok {
			lpb.callbackEmit = f
		}
	}
}

type LPBatcher struct {
	BaseBatcher

	callbackEmit func([]byte)

	buffer []byte
	sync.Mutex
}

func NewLPBatcher(options ...func(*interface{})) *LPBatcher {
	base := BaseBatcher{
		size:     DefaultBufferSize,
		capacity: DefaultBufferCapacity,
	}
	l := &LPBatcher{
		BaseBatcher: base,
	}

	// Apply the options
	for _, o := range options {
		ptr2arg := interface{}(l)
		o(&ptr2arg)
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
		if l.callbackReady != nil {
			l.callbackReady()
		}
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

	packet := b.emitBytes()

	if b.callbackEmit != nil {
		b.callbackEmit(packet)
	}

	return packet
}

func (l *LPBatcher) emitBytes() []byte {
	c := min(l.size, len(l.buffer))

	prepacket := l.buffer[:c]
	lastLF := bytes.LastIndexByte(prepacket, '\n')
	packet := l.buffer[:lastLF]
	l.buffer = l.buffer[len(packet):]

	return packet
}
