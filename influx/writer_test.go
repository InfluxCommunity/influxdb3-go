package influx

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var mu sync.Mutex

func TestWrite(t *testing.T) {
	output := make([]byte, 0, 10)
	var w *PointsWriter
	fill := func() {
		for i := 0; i < 10; i++ {
			s := []byte(fmt.Sprintf("a%d\n", i))
			w.Write(s)
		}
	}
	check := func(t *testing.T) {
		mu.Lock()
		l := len(output)
		assert.True(t, l >= 15, "Len %d", l)
		mu.Unlock()
	}
	tests := []struct {
		name    string
		paramsF func() WriteParams
		testF   func(t *testing.T)
	}{
		{
			"Flush when batchsize reached",
			func() WriteParams {
				params := DefaultWriteParams
				params.BatchSize = 5
				return params
			},
			func(t *testing.T) {
				fill()
				waitForCondition(t, 100, func() bool {
					mu.Lock()
					defer mu.Unlock()
					return len(output) >= 10
				})
				check(t)
			},
		},
		{
			"Flush in interval",
			func() WriteParams {
				params := DefaultWriteParams
				params.FlushInterval = 100
				return params
			},
			func(t *testing.T) {
				fill()
				<-time.After(110 * time.Millisecond)
				waitForCondition(t, 100, func() bool {
					mu.Lock()
					defer mu.Unlock()
					return len(output) >= 10
				})
				check(t)
				mu.Lock()
				output = output[:0]
				mu.Unlock()
				fill()
				<-time.After(110 * time.Millisecond)
				waitForCondition(t, 100, func() bool {
					mu.Lock()
					defer mu.Unlock()
					return len(output) >= 10
				})
				check(t)
			},
		},
		{
			"Manual flush",
			func() WriteParams {
				return DefaultWriteParams
			},
			func(t *testing.T) {
				fill()
				w.Flush()
				waitForCondition(t, 100, func() bool {
					mu.Lock()
					defer mu.Unlock()
					return len(output) >= 10
				})
				check(t)
			},
		},
		{
			"Flush  when max bytes reached",
			func() WriteParams {
				params := DefaultWriteParams
				params.MaxBatchBytes = 15
				return params
			},
			func(t *testing.T) {
				fill()
				waitForCondition(t, 100, func() bool {
					mu.Lock()
					defer mu.Unlock()
					return len(output) >= 10
				})
				check(t)
			},
		},
	}
	for _, test := range tests {
		mu.Lock()
		output = output[:0]
		mu.Unlock()
		t.Run(test.name, func(t *testing.T) {
			w = NewPointsWriter(func(ctx context.Context, bucket string, bs []byte) error {
				mu.Lock()
				defer mu.Unlock()
				output = append(output, bs...)
				return nil
			}, "bucket", test.paramsF())

			test.testF(t)

			w.Close()

		})
	}
}

func TestIgnoreErrors(t *testing.T) {
	i := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i++
		w.WriteHeader(http.StatusInternalServerError)
		switch i {
		case 1:
			_, _ = w.Write([]byte(`{"error":" "write failed: hinted handoff queue not empty"`))
		case 2:
			_, _ = w.Write([]byte(`{"code":"internal error", "message":"partial write: field type conflict"}`))
		case 3:
			_, _ = w.Write([]byte(`{"code":"internal error", "message":"partial write: points beyond retention policy"}`))
		case 4:
			_, _ = w.Write([]byte(`{"code":"internal error", "message":"unable to parse 'cpu value': invalid field format"}`))
		case 5:
			_, _ = w.Write([]byte(`{"code":"internal error", "message":"gateway error"}`))
		}
	}))
	defer server.Close()

	cl, err := New(Configs{HostURL: server.URL})
	require.NoError(t, err)

	writer := cl.PointsWriter("bucket")

	b := &batch{
		lines:             []byte("a"),
		expires:           time.Time{},
	}
	err = writer.writeBatch(b)
	assert.NoError(t, err)
}

func waitForCondition(t *testing.T, timeout int, a func() bool) {
	step := 5
	for {
		<-time.After(time.Duration(step) * time.Millisecond)
		timeout -= step
		if timeout < 0 {
			t.Fatal("wait timeout")
		}
		if a() {
			return
		}
	}
}