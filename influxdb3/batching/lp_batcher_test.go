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

	assert.Equal(t, DefaultByteBatchSize, lpb.size)
	assert.Equal(t, DefaultBufferCapacity, lpb.capacity)
	assert.Nil(t, lpb.callbackReady)
	assert.Nil(t, lpb.callbackByteEmit)
}

func TestLPCustomValues(t *testing.T) {
	size := 2048
	capacity := size * 2

	lpb := NewLPBatcher(
		WithBufferSize(size),
		WithBufferCapacity(capacity),
	)

	assert.Equal(t, size, lpb.size)
	assert.Equal(t, capacity, lpb.capacity)
	assert.Nil(t, lpb.callbackReady)
	assert.Nil(t, lpb.callbackByteEmit)
}

func TestLPBatcherCreate(t *testing.T) {
	size := 1000
	capacity := size * 2

	var emitted bool
	var emittedBytes []byte

	l := NewLPBatcher(
		WithBufferSize(size),
		WithBufferCapacity(capacity),
		WithEmitBytesCallback(func(ba []byte) {
			emitted = true
			emittedBytes = ba
		}),
	)

	assert.Equal(t, size, l.size)
	assert.Equal(t, capacity, l.capacity)
	assert.False(t, emitted)
	assert.Nil(t, emittedBytes)
	assert.NotNil(t, l.callbackByteEmit)
	assert.Nil(t, l.callbackReady)
}

func TestLPReady(t *testing.T) {
	size := 10
	capacity := size * 2
	lpb := NewLPBatcher(WithBufferSize(size), WithBufferCapacity(capacity))
	lpb.Add("0123456789ABCDEF")

	assert.True(t, lpb.Ready(), "LPBatcher should be ready when the batch size is reached")
}

func TestLPReadyCallback(t *testing.T) {
	size := 10
	capacity := size * 2
	readyCalled := false

	lpb := NewLPBatcher(WithBufferSize(size),
		WithBufferCapacity(capacity),
		WithByteEmitReadyCallback(func() {
			readyCalled = true
		}))

	lpb.Add("0123456789ABCDEF")

	assert.True(t, readyCalled)
}

func TestEmitEmptyBatcher(t *testing.T) {
	size := 256
	capacity := size * 2

	lpb := NewLPBatcher(WithBufferSize(size), WithBufferCapacity(capacity))

	results := lpb.Emit()

	assert.Empty(t, results)
}

func TestAddLineAppendsLF(t *testing.T) {
	size := 256
	capacity := size * 2

	lpb := NewLPBatcher(WithBufferSize(size), WithBufferCapacity(capacity))
	lines := []string{
		"cpu,location=roswell,id=R2D2 fVal=3.14,iVal=42i",
		"cpu,location=dyatlov,id=C3PO fVal=2.71,iVal=21i",
		"cpu,location=titan,id=HAL69 fVal=1.41,iVal=7i",
	}
	lpb.Add(lines...)
	results := lpb.Emit()
	assert.Equal(t, []byte(strings.Join(lines, "\n")+"\n"), results)
}

func TestAddLineAppendsNoLFWhenPresent(t *testing.T) {
	size := 256
	capacity := size * 2
	lpb := NewLPBatcher(WithBufferSize(size), WithBufferCapacity(capacity))
	lines := []string{
		"cpu,location=roswell,id=R2D2 fVal=3.14,iVal=42i\n",
		"cpu,location=dyatlov,id=C3PO fVal=2.71,iVal=21i\n",
		"cpu,location=titan,id=HAL69 fVal=1.41,iVal=7i\n",
	}
	lpb.Add(lines...)
	results := lpb.Emit()
	assert.Equal(t, []byte(strings.Join(lines, "")), results)
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

	verify := strings.Join(lines, "\n")
	verify += "\n"

	lpb := NewLPBatcher(
		WithBufferSize(size),
		WithBufferCapacity(capacity),
		WithEmitBytesCallback(func(ba []byte) {
			emitCount++
			emittedBytes = append(emittedBytes, ba...)
		}))
	lpb.Add(lines...)

	assert.Equal(t, lineByteCt, lpb.CurrentLoadSize())

	packet := lpb.Emit()

	assert.Equal(t, verify, string(packet))
	assert.Equal(t, 0, lpb.CurrentLoadSize())
	assert.Equal(t, 0, emitCount) // callback should not have been called
	assert.Empty(t, emittedBytes) // callback should not have been called
}

func TestLPAddAndEmitCallBack(t *testing.T) {
	batchSize := 1000 // Bytes
	capacity := 10000 // Bytes
	emitCount := 0
	emittedBytes := make([]byte, 0)
	readyCalled := 0

	lps2emit := make([]string, 100)

	lpb := NewLPBatcher(
		WithBufferSize(batchSize),
		WithBufferCapacity(capacity),
		WithByteEmitReadyCallback(func() {
			readyCalled++
		}),
		WithEmitBytesCallback(func(b []byte) {
			emitCount++
			emittedBytes = append(emittedBytes, b...)
		}))

	for n := range lps2emit {
		lps2emit[n] = fmt.Sprintf("lptest,foo=bar count=%di", n+1)
	}

	for i := range lps2emit {
		if i > 0 && i%10 == 0 {
			set := lps2emit[i-10 : i]
			lpb.Add(set...)
		}
	}
	// add lingering set
	lpb.Add(lps2emit[len(lps2emit)-10:]...)

	verify := strings.Join(lps2emit, "\n")
	verify += "\n"

	assert.False(t, lpb.Ready())

	emittedBytes = append(emittedBytes, lpb.Emit()...) // drain any leftovers

	expectCall := len(emittedBytes) / batchSize
	assert.Equal(t, expectCall, emitCount)
	assert.Equal(t, verify, string(emittedBytes))
	assert.Equal(t, expectCall, readyCalled)
}

func TestLPBufferFlush(t *testing.T) {
	size := 10
	capacity := size * 2

	lpb := NewLPBatcher(WithBufferSize(size), WithBufferCapacity(capacity))
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

	lpb := NewLPBatcher(WithBufferSize(size),
		WithBufferCapacity(capacity),
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
	batchSize := 64
	loadFactor := 10
	capacity := batchSize * loadFactor
	remainder := 3
	testString := "123456789ABCDEF\n"
	stringSet := make([]string, ((batchSize/len(testString))*loadFactor)+remainder)
	verify := make([]byte, 0)
	for ct := range stringSet {
		stringSet[ct] = testString
		verify = append(verify, []byte(stringSet[ct])...)
	}

	emitCt := 0
	resultBuffer := make([]byte, 0)
	lpb := NewLPBatcher(
		WithBufferSize(batchSize),
		WithBufferCapacity(capacity),
		WithEmitBytesCallback(func(ba []byte) {
			emitCt++
			resultBuffer = append(resultBuffer, ba...)
		}))

	lpb.Add(stringSet...)

	resultBytes := len(resultBuffer)
	assert.Equal(t, len(verify)/batchSize, emitCt, "Emit should be called correct number of times")
	assert.Equal(t, batchSize*emitCt, resultBytes,
		"ResultBuffer should have size of batchSize * number of emit calls ")
	checkBuffer := verify[:batchSize*emitCt]
	remainBuffer := verify[batchSize*emitCt:]
	assert.Equal(t, checkBuffer, resultBuffer)
	assert.Equal(t, len(remainBuffer), lpb.CurrentLoadSize())
	assert.Equal(t, remainBuffer, lpb.buffer)
}
