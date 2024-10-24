package batching

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLineProtocolBatcherCreate(t *testing.T) {
	size := 1000
	capacity := size * 2

	var emitted bool
	var emittedBytes []byte

	l := NewLPBatcher(
		WithSize(size),
		WithCapacity(capacity),
		WithEmitBytesCallback(func(ba []byte) {
			emitted = true
			emittedBytes = ba
		}),
	)

	fmt.Printf("DEBUG l: %v\n", l)

	assert.Equal(t, size, l.size)
	assert.Equal(t, capacity, l.capacity)
	assert.False(t, emitted)
	assert.Nil(t, emittedBytes)
	assert.NotNil(t, l.callbackEmit)
	assert.Nil(t, l.callbackReady)
}

func TestAddAndEmitLineProtocolDefault(t *testing.T) {
	batchSize := 1000 // Bytes
	capacity := 10000 // Bytes
	emitCount := 0
	emittedBytes := make([]byte, 0)
	readyCalled := 0

	lps2emit := make([]string, 100)

	b := NewLPBatcher(
		WithSize(batchSize),
		WithCapacity(capacity),
		WithReadyCallback(func() {
			readyCalled++
		}),
		WithEmitBytesCallback(func(b []byte) {
			emitCount++
			emittedBytes = append(emittedBytes, b...)
		}))
	fmt.Printf("\nDEBUG b: %+v\n", b)

	for n := range lps2emit {
		lps2emit[n] = fmt.Sprintf("lptest,foo=bar count=%di", n+1)
	}

	for i, _ := range lps2emit {
		if i > 0 && i%10 == 0 {
			set := lps2emit[i-10 : i]
			b.Add(set...)
		}
	}
	// add lingering set
	b.Add(lps2emit[len(lps2emit)-10:]...)

	verify := strings.Join(lps2emit, "\n")

	assert.False(t, b.Ready())

	_ = b.Emit() // flush any leftovers - to be collected in callback above

	expectCall := math.Ceil(float64(len(emittedBytes)) / float64(batchSize))
	expectReady := len(emittedBytes) / batchSize
	assert.Equal(t, int(expectCall), emitCount)
	assert.Equal(t, verify, string(emittedBytes))
	fmt.Printf("DEBUG expectedReady: %d, readyCalled: %d\n", expectReady, readyCalled)
	assert.Equal(t, expectReady, readyCalled)
}
