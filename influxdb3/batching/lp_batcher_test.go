package batching

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLPDefaultValues(t *testing.T) {
	lpb := NewLPBatcher()

	assert.Equal(t, DefaultBufferSize, lpb.size)
	assert.Equal(t, DefaultBufferCapacity, lpb.capacity)
	assert.Nil(t, lpb.callbackReady)
	assert.Nil(t, lpb.callbackEmit)
}

func TestLPCustomValues(t *testing.T) {
	size := 2048
	capacity := size * 2

	lpb := NewLPBatcher(
		WithSize(size),
		WithCapacity(capacity),
	)

	assert.Equal(t, size, lpb.size)
	assert.Equal(t, capacity, lpb.capacity)
	assert.Nil(t, lpb.callbackReady)
	assert.Nil(t, lpb.callbackEmit)
}

func TestLPBatcherCreate(t *testing.T) {
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

func TestLPReady(t *testing.T) {
	size := 10
	capacity := size * 2
	lpb := NewLPBatcher(WithSize(size), WithCapacity(capacity))
	lpb.Add("0123456789ABCDEF")

	assert.True(t, lpb.Ready(), "LPBatcher should be ready when the batch size is reached")
}

func TestLPReadyCallback(t *testing.T) {
	size := 10
	capacity := size * 2
	readyCalled := false

	lpb := NewLPBatcher(WithSize(size),
		WithCapacity(capacity),
		WithReadyCallback(func() {
			readyCalled = true
		}))

	lpb.Add("0123456789ABCDEF")

	assert.True(t, readyCalled)
}

func TestLPAddAndPartialEmit(t *testing.T) {
	size := 500
	capacity := size * 2
	emitCount := 0
	emittedBytes := make([]byte, 0)

	lineTemplate := "cpu,location=tabor fVal=2.71,count=%di"
	lines := make([]string, 5)
	lineByteCt := 0
	for n := range 5 {
		lines[n] = fmt.Sprintf(lineTemplate, n+1)
		lineByteCt += len([]byte(lines[n])) + 1
	}

	verif := strings.Join(lines, "\n")

	lpb := NewLPBatcher(
		WithSize(size),
		WithCapacity(capacity),
		WithEmitBytesCallback(func(ba []byte) {
			emitCount++
			emittedBytes = append(emittedBytes, ba...)
		}))
	lpb.Add(lines...)

	assert.Equal(t, lineByteCt, lpb.CurrentLoadSize())

	packet := lpb.Emit()

	assert.Equal(t, verif, string(packet))
	assert.Equal(t, 0, lpb.CurrentLoadSize())
	assert.Equal(t, 0, emitCount)         // callback should not have been called
	assert.Equal(t, 0, len(emittedBytes)) // callback should not have been called
}

func TestLPAddAndEmitCallBack(t *testing.T) {
	batchSize := 1000 // Bytes
	capacity := 10000 // Bytes
	emitCount := 0
	emittedBytes := make([]byte, 0)
	readyCalled := 0

	lps2emit := make([]string, 100)

	lpb := NewLPBatcher(
		WithSize(batchSize),
		WithCapacity(capacity),
		WithReadyCallback(func() {
			readyCalled++
		}),
		WithEmitBytesCallback(func(b []byte) {
			emitCount++
			emittedBytes = append(emittedBytes, b...)
		}))

	for n := range lps2emit {
		lps2emit[n] = fmt.Sprintf("lptest,foo=bar count=%di", n+1)
	}

	for i, _ := range lps2emit {
		if i > 0 && i%10 == 0 {
			set := lps2emit[i-10 : i]
			lpb.Add(set...)
		}
	}
	// add lingering set
	lpb.Add(lps2emit[len(lps2emit)-10:]...)

	verify := strings.Join(lps2emit, "\n")

	assert.False(t, lpb.Ready())

	emittedBytes = append(emittedBytes, lpb.Emit()...) // drain any leftovers

	expectCall := len(emittedBytes) / batchSize
	assert.Equal(t, int(expectCall), emitCount)
	assert.Equal(t, verify, string(emittedBytes))
	assert.Equal(t, expectCall, readyCalled)
}

func TestLPBufferFlush(t *testing.T) {
	size := 10
	capacity := size * 2

	lpb := NewLPBatcher(WithSize(size), WithCapacity(capacity))
	testString := "0123456789ABCDEF\n"

	assert.Equal(t, 0, lpb.CurrentLoadSize())
	lpb.Add(testString)
	assert.Equal(t, len(testString), lpb.CurrentLoadSize())
	packet := lpb.Flush()
	assert.Equal(t, 0, lpb.CurrentLoadSize())
	assert.Equal(t, testString, string(packet))
}

func TestLPThreadSafety(t *testing.T) {
	size := 80
	capacity := size * 2
	var wg sync.WaitGroup
	emitCt := 0
	testString := "123456789ABCDEF\n"

	lpb := NewLPBatcher(WithSize(size),
		WithCapacity(capacity),
		WithEmitBytesCallback(func(ba []byte) {
			emitCt++
		}))

	for range 25 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 4 {
				lpb.Add(testString)
			}
		}()
	}

	wg.Wait()
	packet := lpb.Emit()
	assert.Equal(t, 20, emitCt, "All bytes should have been emitted")
	assert.Empty(t, packet, "Remaining bytes should be emitted correctly")
}

func TestLPAddLargerThanSize(t *testing.T) {
	// TODO review test -- appears Emit called too frequently
	// Look for leading '\n' in lp.buffer
	size := 64
	loadFactor := 10
	capacity := size * loadFactor
	remainder := 3
	testString := "123456789ABCDEF\n"
	stringSet := make([]string, ((size/len(testString))*loadFactor)+remainder)
	stringSetByteCt := 0
	for ct := range stringSet {
		stringSet[ct] = testString
		stringSetByteCt += len([]byte(testString))
	}

	fmt.Printf("DEBUG len(stringSet)=%d\n", len(stringSet))
	fmt.Printf("DEBUG stringSetByteCount %d\n", stringSetByteCt)
	fmt.Printf("DEBUG stringSet: %v\n", stringSet)

	emitCt := 0
	resultBuffer := make([]byte, 0)
	lpb := NewLPBatcher(
		WithSize(size),
		WithCapacity(capacity),
		WithEmitBytesCallback(func(ba []byte) {
			emitCt++
			resultBuffer = append(resultBuffer, ba...)
		}))

	lpb.Add(stringSet...)

	results := strings.Split(string(resultBuffer), "\n")
	resultsBytes := len(resultBuffer)
	fmt.Printf("DEBUG emitCt: %d\n", emitCt)
	fmt.Printf("DEBUG resultsBytes: %d\n", resultsBytes)
	fmt.Printf("DEBUG len(results): %d\n", len(results))
	fmt.Printf("DEBUG results: %s\n", results)
	fmt.Printf("DEBUG lpb.CurrentLoadSize: %d\n", lpb.CurrentLoadSize())
	fmt.Printf("DEBUG lpb.buffer #%s#\n", string(lpb.buffer))

}
