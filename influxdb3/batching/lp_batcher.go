package batching

import (
	"bytes"
	"fmt"
	"log/slog"
	"sync"
)

const DefaultBufferSize = 100000
const DefaultBufferCapacity = DefaultBufferSize * 2

func WithEmitBytesCallback(f func([]byte)) Option {
	return func(b *interface{}) {
		if lpb, ok := (*b).(*LPBatcher); ok {
			lpb.callbackEmit = f
		} else {
			slog.Warn("Failed to match type LPBatcher in WithEmitBytesCallback. Callback not set.")
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
			if line[len(line)-1] != '\n' { //ensure newline demarcation
				l.buffer = append(l.buffer, '\n')
			}
		}
	}

	//fmt.Printf("DEBUG after load l.buffer #%s#\n", string(l.buffer))

	for l.isReady() {
		if l.callbackReady != nil {
			l.callbackReady()
		}
		if l.callbackEmit != nil {
			// fmt.Printf("DEBUG calling emit function\n")
			l.callbackEmit(l.emitBytes())
			//fmt.Printf("DEBUG l.buffer #%s#\n", string(l.buffer))
		} else {
			// no emitter callback
			if l.CurrentLoadSize() > (l.capacity - l.size) {
				slog.Warn(
					fmt.Sprintf("Batcher is ready, but no callbackEmit is available.  "+
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
