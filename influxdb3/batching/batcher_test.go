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

package batching

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/influxdb3"
	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"github.com/stretchr/testify/assert"
)

func TestDefaultValues(t *testing.T) {
	b := NewBatcher()

	// Check that default values are set correctly
	assert.Equal(t, DefaultBatchSize, b.size)
	assert.Equal(t, DefaultCapacity, cap(b.points))
}

func TestCustomValues(t *testing.T) {
	batchSize := 10
	capacity := 100

	b := NewBatcher(
		WithSize(batchSize),
		WithCapacity(capacity),
	)

	assert.Equal(t, batchSize, b.size)
	assert.Equal(t, capacity, cap(b.points))
}

func TestAddAndDefaultEmitPointDefault(t *testing.T) {
	batchSize := 10
	//emitted := false
	points2emit := make([]*influxdb3.Point, batchSize)

	b := NewBatcher(WithSize(batchSize))
	for n := range batchSize / 2 {
		points2emit[n] = influxdb3.NewPointWithMeasurement("pointtest").
			SetTag("foo", "bar").
			SetIntegerField("count", int64(n+1))
	}
	fmt.Printf("\nDEBUG points2emit[0]: %v\n", reflect.TypeOf(points2emit[0]))
	b.Add(points2emit...)
	// force Emit
	result := b.Emit()
	fmt.Printf("\nDEBUG Inspect result %+v", result)
	fmt.Printf("\nDEBUG Inspect result %+v", reflect.TypeOf(result[0]))
	lp, err := result[0].MarshalBinary(lineprotocol.Millisecond)
	if err != nil {
		fmt.Printf("err: %s\n", err)
	}
	fmt.Printf("\nDEBUG Inspect lp %s", string(lp))
}

func TestAddAndCallBackEmitPoint(t *testing.T) {
	batchSize := 5
	emitted := false
	var emittedPoints []*influxdb3.Point

	b := NewBatcher(
		WithSize(batchSize),
		WithEmitPointsCallback(func(points []*influxdb3.Point) {
			fmt.Printf("callback called with %v\n", points)
			emitted = true
			emittedPoints = points
		}),
	)

	for range batchSize {
		fmt.Printf("Adding point")
		b.Add(&influxdb3.Point{})
	}

	assert.True(t, emitted, "Emit callback should have been called")
	assert.Len(t, emittedPoints, batchSize, "The emitted batch size should match the expected size")
}

func TestReady(t *testing.T) {
	batchSize := 5

	b := NewBatcher(
		WithSize(batchSize),
	)

	points := []*influxdb3.Point{{}, {}, {}, {}, {}}
	b.Add(points...)

	assert.True(t, b.Ready(), "Batcher should be ready when the batch size is reached")
}

func TestReadyCallback(t *testing.T) {
	batchSize := 5
	readyCalled := false

	b := NewBatcher(
		WithSize(batchSize),
		WithReadyCallback(func() {
			readyCalled = true
		}),
	)

	for range batchSize {
		b.Add(&influxdb3.Point{})
	}

	assert.True(t, readyCalled, "Ready callback should have been called when the batch is full")
}

func TestPartialEmit(t *testing.T) {
	batchSize := 5
	emitted := false

	b := NewBatcher(
		WithSize(batchSize),
		WithEmitPointsCallback(func(points []*influxdb3.Point) {
			emitted = true
		}),
	)

	b.Add(&influxdb3.Point{}, &influxdb3.Point{})
	b.Add(&influxdb3.Point{})

	points := b.Emit()

	assert.False(t, emitted, "Emit callback should not have been called automatically")
	assert.Len(t, points, 3, "Emit should return all points when batch size is not reached")
}

func TestThreadSafety(t *testing.T) {
	batchSize := 5
	var wg sync.WaitGroup
	emits := 0
	b := NewBatcher(
		WithSize(batchSize),
		WithEmitPointsCallback(func(points []*influxdb3.Point) {
			emits++
		}),
	)

	for range 25 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 4 {
				b.Add(&influxdb3.Point{})
			}
		}()
	}

	wg.Wait()

	points := b.Emit()
	assert.Equal(t, 20, emits, "All points should have been emitted")
	assert.Empty(t, points, "Remaining points should be emitted correctly")
}

func TestAddLargerThanSize(t *testing.T) {
	batchSize := 5
	emitCt := 0
	pointSet := make([]*influxdb3.Point, (batchSize*10)+3)
	for ct := range pointSet {
		pointSet[ct] = influxdb3.NewPoint("test",
			map[string]string{"foo": "bar"},
			map[string]interface{}{"count": ct + 1},
			time.Now())
	}

	for _, v := range pointSet {
		lp, _ := v.MarshalBinary(lineprotocol.Millisecond)
		fmt.Printf("%s", string(lp))
	}
	subSet := pointSet[:5]
	fmt.Printf("DEBUG subset: %v\n", subSet)
	resultSet := make([]*influxdb3.Point, 0)
	b := NewBatcher(WithSize(batchSize),
		WithEmitPointsCallback(func(points []*influxdb3.Point) {
			resultSet = append(resultSet, points...)
			emitCt++
		}))

	b.Add(pointSet...)
	fmt.Printf("\nDEBUG emitCt: %d\n", emitCt)
	fmt.Printf("\nDEBUG len(resultSet): %d\n", len(resultSet))
	fmt.Printf("\nDEBUG len(b.points): %d\n", len(b.points))
	fmt.Printf("\nDEBUG dump resultSet:\n")
	for _, rp := range resultSet {
		lp, _ := rp.MarshalBinary(lineprotocol.Millisecond)
		fmt.Printf("%s", string(lp))
	}
	fmt.Println()
	//assert.Equal(t, pointSet, resultSet)

}
