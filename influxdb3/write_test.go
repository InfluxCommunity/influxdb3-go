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

package influxdb3

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/runtime/protoimpl"
)

func TestEncode(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name  string
		s     interface{}
		line  string
		error string
	}{
		{
			name: "test normal structure",
			s: struct {
				Measurement string    `lp:"measurement"`
				Sensor      string    `lp:"tag,sensor"`
				ID          string    `lp:"tag,device_id"`
				Temp        float64   `lp:"field,temperature"`
				Hum         int       `lp:"field,humidity"`
				Time        time.Time `lp:"timestamp"`
				Description string    `lp:"-"`
			}{
				"air",
				"SHT31",
				"10",
				23.5,
				55,
				now,
				"Room temp",
			},
			line: fmt.Sprintf("air,device_id=10,sensor=SHT31 humidity=55i,temperature=23.5 %d\n", now.UnixNano()),
		},
		{
			name: "test pointer to normal structure",
			s: &struct {
				Measurement string    `lp:"measurement"`
				Sensor      string    `lp:"tag,sensor"`
				ID          string    `lp:"tag,device_id"`
				Temp        float64   `lp:"field,temperature"`
				Hum         int       `lp:"field,humidity"`
				Time        time.Time `lp:"timestamp"`
				Description string    `lp:"-"`
			}{
				"air",
				"SHT31",
				"10",
				23.5,
				55,
				now,
				"Room temp",
			},
			line: fmt.Sprintf("air,device_id=10,sensor=SHT31 humidity=55i,temperature=23.5 %d\n", now.UnixNano()),
		},
		{
			name: "test normal structure with unexported field",
			s: struct {
				Measurement string    `lp:"measurement"`
				Sensor      string    `lp:"tag,sensor"`
				ID          string    `lp:"tag,device_id"`
				Temp        float64   `lp:"field,temperature"`
				Hum         int64     `lp:"field,humidity"`
				Time        time.Time `lp:"timestamp"`
				Description string    `lp:"-"`
			}{
				"air",
				"SHT31",
				"10",
				23.5,
				55,
				now,
				"Room temp",
			},
			line: fmt.Sprintf("air,device_id=10,sensor=SHT31 humidity=55i,temperature=23.5 %d\n", now.UnixNano()),
		},
		{
			name: "test protobuf structure",
			s: struct {
				Measurement   string    `lp:"measurement"`
				Sensor        string    `lp:"tag,sensor"`
				ID            string    `lp:"tag,device_id"`
				Temp          float64   `lp:"field,temperature"`
				Hum           int64     `lp:"field,humidity"`
				Time          time.Time `lp:"timestamp"`
				Description   string    `lp:"-"`
				state         protoimpl.MessageState
				sizeCache     protoimpl.SizeCache
				unknownFields protoimpl.UnknownFields
			}{
				Measurement: "air",
				Sensor:      "SHT31",
				ID:          "10",
				Temp:        23.5,
				Hum:         55,
				Time:        now,
				Description: "Room temp",
			},
			line: fmt.Sprintf("air,device_id=10,sensor=SHT31 humidity=55i,temperature=23.5 %d\n", now.UnixNano()),
		},
		{
			name: "test no tag, no timestamp",
			s: &struct {
				Measurement string  `lp:"measurement"`
				Temp        float64 `lp:"field,temperature"`
			}{
				"air",
				23.5,
			},
			line: "air temperature=23.5\n",
		},
		{
			name: "test default struct field name",
			s: &struct {
				Measurement string  `lp:"measurement"`
				Sensor      string  `lp:"tag"`
				Temp        float64 `lp:"field"`
			}{
				"air",
				"SHT31",
				23.5,
			},
			line: "air,Sensor=SHT31 Temp=23.5\n",
		},
		{
			name: "test missing struct field tag name",
			s: &struct {
				Measurement string  `lp:"measurement"`
				Sensor      string  `lp:"tag,"`
				Temp        float64 `lp:"field"`
			}{
				"air",
				"SHT31",
				23.5,
			},
			error: `encoding error: invalid tag key ""`,
		},
		{
			name: "test missing struct field field name", //nolint
			s: &struct {
				Measurement string  `lp:"measurement"`
				Temp        float64 `lp:"field,"`
			}{
				"air",
				23.5,
			},
			error: `encoding error: invalid field key ""`,
		},
		{
			name: "test missing measurement",
			s: &struct {
				Measurement string  `lp:"tag"`
				Sensor      string  `lp:"tag"`
				Temp        float64 `lp:"field"`
			}{
				"air",
				"SHT31",
				23.5,
			},
			error: `no struct field with tag 'measurement'`,
		},
		{
			name: "test no field",
			s: &struct {
				Measurement string  `lp:"measurement"`
				Sensor      string  `lp:"tag"`
				Temp        float64 `lp:"tag"`
			}{
				"air",
				"SHT31",
				23.5,
			},
			error: `no struct field with tag 'field'`,
		},
		{
			name: "test double measurement",
			s: &struct {
				Measurement string  `lp:"measurement"`
				Sensor      string  `lp:"measurement"`
				Temp        float64 `lp:"field,a"`
				Hum         float64 `lp:"field,a"`
			}{
				"air",
				"SHT31",
				23.5,
				43.1,
			},
			error: `multiple measurement fields`,
		},
		{
			name: "test multiple tag attributes",
			s: &struct {
				Measurement string  `lp:"measurement"`
				Sensor      string  `lp:"tag,a,a"`
				Temp        float64 `lp:"field,a"`
				Hum         float64 `lp:"field,a"`
			}{
				"air",
				"SHT31",
				23.5,
				43.1,
			},
			error: `multiple tag attributes are not supported`,
		},
		{
			name: "test invalid tag attribute",
			s: &struct {
				Measurement string  `lp:"measurement"`
				Sensor      string  `lp:"blah,a"`
				Temp        float64 `lp:"field,a"`
				Hum         float64 `lp:"field,a"`
			}{
				"air",
				"SHT31",
				23.5,
				43.1,
			},
			error: `invalid tag blah`,
		},
		{
			name: "test wrong timestamp type",
			s: &struct {
				Measurement string  `lp:"measurement"`
				Sensor      string  `lp:"tag,sensor"`
				Temp        float64 `lp:"field,a"`
				Hum         float64 `lp:"timestamp"`
			}{
				"air",
				"SHT31",
				23.5,
				43.1,
			},
			error: `cannot use field 'Hum' as a timestamp`,
		},
		{
			name: "test map",
			s: map[string]interface{}{
				"measurement": "air",
				"sensor":      "SHT31",
				"temp":        23.5,
			},
			error: `cannot use map[string]interface {} as point`,
		},
	}

	for _, ts := range tests {
		t.Run(ts.name, func(t *testing.T) {
			client, err := New(ClientConfig{
				Host:  "http://localhost:8086",
				Token: "my-token",
			})
			require.NoError(t, err)
			b, err := encode(ts.s, client.config.WriteOptions)
			if ts.error == "" {
				require.NoError(t, err)
				assert.Equal(t, ts.line, string(b))
			} else {
				require.Error(t, err)
				assert.Equal(t, ts.error, err.Error())
			}
		})
	}
}

func genPoints(count int) []*Point {
	ps := make([]*Point, count)
	ts := time.Now()
	gen := rand.New(rand.NewSource(321))
	for i := range ps {
		p := NewPointWithMeasurement("host")
		p.SetTag("rack", fmt.Sprintf("rack_%2d", i%10))
		p.SetTag("name", fmt.Sprintf("machine_%2d", i))
		p.SetField("temperature", gen.Float64()*80.0)
		p.SetField("disk_free", gen.Float64()*1000.0)
		p.SetField("disk_total", (i/10+1)*1000000)
		p.SetField("mem_total", (i/100+1)*10000000)
		p.SetField("mem_free", gen.Uint64())
		p.SetTimestamp(ts)
		ps[i] = p
		ts = ts.Add(time.Millisecond)
	}
	return ps
}

func points2bytes(t *testing.T, points []*Point, defaultTags ...map[string]string) []byte {
	var bytes []byte
	var defaultTagsOrNil map[string]string
	if len(defaultTags) > 0 {
		defaultTagsOrNil = defaultTags[0]
	}
	for _, p := range points {
		bs, err := p.MarshalBinaryWithDefaultTags(lineprotocol.Millisecond, defaultTagsOrNil)
		require.NoError(t, err)
		bytes = append(bytes, bs...)
	}
	return bytes
}

func returnHTTPError(w http.ResponseWriter, code int, message string) {
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write([]byte(fmt.Sprintf(`{"code":"invalid", "message":"%s"}`, message)))
}

// compArrays compares arrays
func compArrays(b1 []byte, b2 []byte) int {
	if len(b1) != len(b2) {
		return -1
	}
	for i := 0; i < len(b1); i++ { //nolint:intrange
		if b1[i] != b2[i] {
			return i
		}
	}
	return 0
}

func TestWriteCorrectUrl(t *testing.T) {
	correctPath := "/path/api/v2/write?bucket=my-database&org=my-org&precision=ms"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// initialization of query client
		if r.Method == "PRI" {
			return
		}
		assert.EqualValues(t, correctPath, r.URL.String())
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()
	options := DefaultWriteOptions
	options.Precision = lineprotocol.Millisecond
	options.NoSync = false
	c, err := New(ClientConfig{
		Host:         ts.URL + "/path/",
		Token:        "my-token",
		Organization: "my-org",
		Database:     "my-database",
		WriteOptions: &options,
	})
	require.NoError(t, err)
	err = c.Write(context.Background(), []byte("a f=1"))
	assert.NoError(t, err)
	err = c.Write(context.Background(), []byte("a f=1"))
	assert.NoError(t, err)
}

func TestWriteCorrectUrlNoSync(t *testing.T) {
	var correctPath string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// initialization of query client
		if r.Method == "PRI" {
			return
		}
		assert.EqualValues(t, correctPath, r.URL.String())
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	options := DefaultWriteOptions
	options.Precision = lineprotocol.Millisecond

	clientConfig := ClientConfig{
		Host:         ts.URL + "/path/",
		Token:        "my-token",
		Organization: "my-org",
		Database:     "my-database",
		WriteOptions: &options,
	}

	// options.NoSync unset
	c, err := New(clientConfig)
	require.NoError(t, err)
	correctPath = "/path/api/v2/write?bucket=my-database&org=my-org&precision=ms" // v2 call
	err = c.Write(context.Background(), []byte("a f=1"))
	assert.NoError(t, err)

	// options.NoSync = false
	options.NoSync = false
	c, err = New(clientConfig)
	require.NoError(t, err)
	correctPath = "/path/api/v2/write?bucket=my-database&org=my-org&precision=ms" // v2 call
	err = c.Write(context.Background(), []byte("a f=1"))
	assert.NoError(t, err)

	// options.NoSync = true
	options.NoSync = true
	c, err = New(clientConfig)
	require.NoError(t, err)
	correctPath = "/path/api/v3/write_lp?db=my-database&no_sync=true&org=my-org&precision=millisecond" // v3 call
	err = c.Write(context.Background(), []byte("a f=1"))
	assert.NoError(t, err)

	// options.NoSync = false & WithNoSync(true)
	options.NoSync = false
	c, err = New(clientConfig)
	require.NoError(t, err)
	correctPath = "/path/api/v3/write_lp?db=my-database&no_sync=true&org=my-org&precision=millisecond" // v3 call
	err = c.Write(context.Background(), []byte("a f=1"), WithNoSync(true))
	assert.NoError(t, err)

	// options.NoSync = true & WithNoSync(false)
	options.NoSync = true
	c, err = New(clientConfig)
	require.NoError(t, err)
	correctPath = "/path/api/v2/write?bucket=my-database&org=my-org&precision=ms" // v2 call
	err = c.Write(context.Background(), []byte("a f=1"), WithNoSync(false))
	assert.NoError(t, err)
}

func TestWriteWithNoSyncToV2Server(t *testing.T) {
	var correctPath string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// initialization of query client
		if r.Method == "PRI" {
			return
		}
		// Fails on v3 API calls.
		if strings.Contains(r.URL.Path, "/api/v3/") {
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
		assert.EqualValues(t, correctPath, r.URL.String())
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	options := DefaultWriteOptions
	options.Precision = lineprotocol.Millisecond

	clientConfig := ClientConfig{
		Host:         ts.URL + "/path/",
		Token:        "my-token",
		Organization: "my-org",
		Database:     "my-database",
		WriteOptions: &options,
	}

	// options.NoSync = false
	options.NoSync = false
	c, err := New(clientConfig)
	require.NoError(t, err)
	correctPath = "/path/api/v2/write?bucket=my-database&org=my-org&precision=ms" // v2 call
	err = c.Write(context.Background(), []byte("a f=1"))
	assert.NoError(t, err)

	// options.NoSync = true
	options.NoSync = true
	c, err = New(clientConfig)
	require.NoError(t, err)
	correctPath = "/path/api/v3/write_lp?db=my-database&no_sync=true&org=my-org&precision=millisecond" // v3 call
	err = c.Write(context.Background(), []byte("a f=1"))
	// should fail, as v3 API is not supported
	require.Error(t, err)
	assert.ErrorContains(t, err, "server doesn't support write with NoSync=true (supported by InfluxDB 3 Core/Enterprise servers only)")
}

func TestWritePointsAndBytes(t *testing.T) {
	points := genPoints(5000)
	byts := points2bytes(t, points)
	reqs := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// initialization of query client
		if r.Method == "PRI" {
			return
		}
		reqs++
		buff, err := io.ReadAll(r.Body)
		if err != nil {
			returnHTTPError(w, http.StatusInternalServerError, fmt.Sprintf("error reading body: %v", err))
			return
		}
		if r := compArrays(byts, buff); r != 0 {
			if r == -1 {
				returnHTTPError(w, http.StatusInternalServerError, fmt.Sprintf("error lens are not equal %d vs %d", len(byts), len(buff)))
			} else {
				returnHTTPError(w, http.StatusInternalServerError, fmt.Sprintf("error bytes are not equal %d", r))
			}
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()
	c, err := New(ClientConfig{
		Host:     ts.URL,
		Token:    "my-token",
		Database: "my-database",
	})
	require.NoError(t, err)
	c.config.WriteOptions.Precision = lineprotocol.Millisecond
	c.config.WriteOptions.GzipThreshold = 0
	require.NoError(t, err)
	err = c.Write(context.Background(), byts)
	assert.NoError(t, err)
	assert.Equal(t, 1, reqs)

	err = c.WritePoints(context.Background(), points)
	assert.NoError(t, err)
	assert.Equal(t, 2, reqs)

	// test error
	err = c.Write(context.Background(), []byte("line"))
	require.Error(t, err)
	assert.Equal(t, 3, reqs)
	assert.Equal(t, "invalid: error lens are not equal 911244 vs 4", err.Error())
}

func TestWritePointsWithOptionsDeprecated(t *testing.T) {
	points := genPoints(1)
	defaultTags := map[string]string{
		"defaultTag": "default",
		"rack":       "main",
	}
	lp := points2bytes(t, points, defaultTags)
	correctPath := "/api/v2/write?bucket=db-x&org=&precision=ms"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// initialization of query client
		if r.Method == "PRI" {
			return
		}

		assert.EqualValues(t, correctPath, r.URL.String())
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		assert.Equal(t, string(lp), string(body))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()
	c, err := New(ClientConfig{
		Host:     ts.URL,
		Token:    "my-token",
		Database: "my-database",
	})
	options := WriteOptions{
		Database:    "db-x",
		Precision:   lineprotocol.Millisecond,
		DefaultTags: defaultTags,
	}
	require.NoError(t, err)
	err = c.WritePointsWithOptions(context.Background(), &options, points...)
	assert.NoError(t, err)
}

func TestWritePointsWithOptions(t *testing.T) {
	points := genPoints(1)
	defaultTags := map[string]string{
		"defaultTag": "default",
		"rack":       "main",
	}
	lp := points2bytes(t, points, defaultTags)
	correctPath := "/api/v2/write?bucket=db-x&org=&precision=ms"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// initialization of query client
		if r.Method == "PRI" {
			return
		}

		assert.EqualValues(t, correctPath, r.URL.String())
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		assert.Equal(t, string(lp), string(body))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()
	c, err := New(ClientConfig{
		Host:     ts.URL,
		Token:    "my-token",
		Database: "my-database",
	})
	require.NoError(t, err)
	err = c.WritePoints(context.Background(), points,
		WithPrecision(lineprotocol.Millisecond),
		WithDatabase("db-x"),
		WithDefaultTags(defaultTags))
	assert.NoError(t, err)
}

func TestWriteData(t *testing.T) {
	now := time.Now()
	s := sampleDataStruct(now)
	lp := fmt.Sprintf("air,device_id=10,sensor=SHT31 humidity=55i,temperature=23.5 %d\n", now.UnixNano())
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// initialization of query client
		if r.Method == "PRI" {
			return
		}
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		assert.Equal(t, lp, string(body))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()
	c, err := New(ClientConfig{
		Host:     ts.URL,
		Token:    "my-token",
		Database: "my-database",
	})
	require.NoError(t, err)
	err = c.WriteData(context.Background(), []any{s})
	assert.NoError(t, err)
}

func sampleDataStruct(now time.Time) struct {
	Measurement string `lp:"measurement"`
	Sensor      string `lp:"tag,sensor"`
	ID          string `lp:"tag,device_id"`

	Temp        float64   `lp:"field,temperature"`
	Hum         int       `lp:"field,humidity"`
	Time        time.Time `lp:"timestamp"`
	Description string    `lp:"-"`
} {
	return struct {
		Measurement string    `lp:"measurement"`
		Sensor      string    `lp:"tag,sensor"`
		ID          string    `lp:"tag,device_id"`
		Temp        float64   `lp:"field,temperature"`
		Hum         int       `lp:"field,humidity"`
		Time        time.Time `lp:"timestamp"`
		Description string    `lp:"-"`
	}{
		"air",
		"SHT31",
		"10",
		23.5,
		55,
		now,
		"Room temp",
	}
}

func TestWriteEmptyData(t *testing.T) {
	calls := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()
	c, err := New(ClientConfig{
		Host:     ts.URL,
		Token:    "my-token",
		Database: "my-database",
	})
	require.NoError(t, err)

	err = c.Write(context.Background(), []byte{})
	assert.NoError(t, err)

	err = c.Write(context.Background(), nil)
	assert.NoError(t, err)

	err = c.WritePoints(context.Background(), []*Point{})
	assert.NoError(t, err)

	err = c.WritePoints(context.Background(), nil)
	assert.NoError(t, err)

	err = c.WriteData(context.Background(), []any{})
	assert.NoError(t, err)

	err = c.WriteData(context.Background(), nil)
	assert.NoError(t, err)

	assert.Equal(t, 0, calls)
}

func TestWriteDataWithOptionsDeprecated(t *testing.T) {
	now := time.Now()
	s := struct {
		Measurement string    `lp:"measurement"`
		Sensor      string    `lp:"tag,sensor"`
		ID          string    `lp:"tag,device_id"`
		Temp        float64   `lp:"field,temperature"`
		Hum         int       `lp:"field,humidity"`
		Time        time.Time `lp:"timestamp"`
		Description string    `lp:"-"`
	}{
		"air",
		"SHT31",
		"10",
		23.5,
		55,
		now,
		"Room temp",
	}
	defaultTags := map[string]string{
		"defaultTag": "default",
		"rack":       "main",
	}
	lp := fmt.Sprintf("air,defaultTag=default,device_id=10,rack=main,sensor=SHT31 humidity=55i,temperature=23.5 %d\n", now.Unix())
	correctPath := "/api/v2/write?bucket=db-x&org=my-org&precision=s"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// initialization of query client
		if r.Method == "PRI" {
			return
		}
		assert.EqualValues(t, correctPath, r.URL.String())
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		assert.Equal(t, lp, string(body))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()
	c, err := New(ClientConfig{
		Host:         ts.URL,
		Token:        "my-token",
		Organization: "my-org",
		Database:     "my-database",
	})
	options := WriteOptions{
		Database:    "db-x",
		Precision:   lineprotocol.Second,
		DefaultTags: defaultTags,
	}
	require.NoError(t, err)
	err = c.WriteDataWithOptions(context.Background(), &options, s)
	assert.NoError(t, err)
}

func TestWriteDataWithOptions(t *testing.T) {
	now := time.Now()
	s := struct {
		Measurement string    `lp:"measurement"`
		Sensor      string    `lp:"tag,sensor"`
		ID          string    `lp:"tag,device_id"`
		Temp        float64   `lp:"field,temperature"`
		Hum         int       `lp:"field,humidity"`
		Time        time.Time `lp:"timestamp"`
		Description string    `lp:"-"`
	}{
		"air",
		"SHT31",
		"10",
		23.5,
		55,
		now,
		"Room temp",
	}
	defaultTags := map[string]string{
		"defaultTag": "default",
		"rack":       "main",
	}
	lp := fmt.Sprintf("air,defaultTag=default,device_id=10,rack=main,sensor=SHT31 humidity=55i,temperature=23.5 %d\n", now.Unix())
	correctPath := "/api/v2/write?bucket=db-x&org=my-org&precision=s"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// initialization of query client
		if r.Method == "PRI" {
			return
		}
		assert.EqualValues(t, correctPath, r.URL.String())
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		assert.Equal(t, lp, string(body))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()
	c, err := New(ClientConfig{
		Host:         ts.URL,
		Token:        "my-token",
		Organization: "my-org",
		Database:     "my-database",
	})
	require.NoError(t, err)
	err = c.WriteData(context.Background(), []any{s},
		WithDatabase("db-x"),
		WithPrecision(lineprotocol.Second),
		WithDefaultTags(defaultTags))
	assert.NoError(t, err)
}

func TestGzip(t *testing.T) {
	points := genPoints(1)
	byts := points2bytes(t, points)
	wasGzip := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// initialization of query client
		if r.Method == "PRI" {
			return
		}
		body := r.Body
		if r.Header.Get("Content-Encoding") == "gzip" {
			body, _ = gzip.NewReader(body)
			wasGzip = true
		}
		buff, err := io.ReadAll(body)
		if err != nil {
			returnHTTPError(w, http.StatusInternalServerError, fmt.Sprintf("error reading body: %v", err))
			return
		}
		if r := compArrays(byts, buff); r != 0 {
			if r == -1 {
				returnHTTPError(w, http.StatusInternalServerError, fmt.Sprintf("error lens  are not equal %d vs %d", len(byts), len(buff)))
			} else {
				returnHTTPError(w, http.StatusInternalServerError, fmt.Sprintf("error bytes are not equal %d", r))
			}
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()
	c, err := New(ClientConfig{
		Host:     ts.URL,
		Token:    "my-token",
		Database: "my-database",
	})
	require.NoError(t, err)
	// Test no gzip on small body
	err = c.Write(context.Background(), byts)
	assert.NoError(t, err)
	assert.False(t, wasGzip)
	// Test gzip on larger body
	points = genPoints(100)
	byts = points2bytes(t, points)
	err = c.Write(context.Background(), byts)
	assert.NoError(t, err)
	assert.True(t, wasGzip)
	// Test disable gzipping
	wasGzip = false
	c.config.WriteOptions.GzipThreshold = 0
	err = c.Write(context.Background(), byts)
	assert.NoError(t, err)
	assert.False(t, wasGzip)
}

func TestCustomHeaders(t *testing.T) {
	p := NewPointWithMeasurement("cpu")
	p.SetTag("host", "local")
	p.SetField("usage_user", 16.75)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PRI" { // query client initialization; HTTP/2 should not happen if https was used?
			return
		}
		xHeader := r.Header.Get("X-Device")
		assert.Equal(t, "ab-01", xHeader)
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		assert.Equal(t, "cpu,host=local usage_user=16.75\n", string(body))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()
	c, err := New(ClientConfig{
		Host:     ts.URL,
		Token:    "my-token",
		Database: "my-database",
		Headers: http.Header{
			"X-Device": []string{"ab-01"},
		},
	})
	require.NoError(t, err)
	err = c.WritePoints(context.Background(), []*Point{p})
	require.NoError(t, err)
}

func TestWriteErrorMarshalPoint(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()
	c, err := New(ClientConfig{
		Host:     ts.URL,
		Token:    "my-token",
		Database: "my-database",
	})
	require.NoError(t, err)
	c.config.WriteOptions.Precision = lineprotocol.Millisecond
	c.config.WriteOptions.GzipThreshold = 0
	require.NoError(t, err)

	p := NewPointWithMeasurement("host")
	p.SetTag("rack", fmt.Sprintf("rack_%2d", 7))
	p.SetTag("name", fmt.Sprintf("machine_%2d", 2))
	// invalid field
	p.SetField("", 80.0)

	p.SetTimestamp(time.Now())

	err = c.WritePoints(context.Background(), []*Point{p})
	assert.Error(t, err)

	err = c.WriteData(context.Background(), []interface{}{
		0,
	})
	assert.Error(t, err)
}

func TestHttpError(t *testing.T) {
	p := NewPointWithMeasurement("cpu")
	p.SetTag("host", "local")
	p.SetField("usage_user", 16.75)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PRI" { // query client initialization; HTTP/2 should not happen if https was used?
			return
		}
		panic("simulated server error")
	}))
	defer ts.Close()
	c, err := New(ClientConfig{
		Host:     ts.URL,
		Token:    "my-token",
		Database: "my-database",
	})
	require.NoError(t, err)
	err = c.WritePoints(context.Background(), []*Point{p})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "error calling")
}

func TestHttpErrorWithHeaders(t *testing.T) {
	traceID := "123456789ABCDEF0"
	tsVersion := "v0.0.1"
	build := "TestServer"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Trace-Id", traceID)
		w.Header().Set("X-Influxdb-Build", build)
		w.Header().Set("X-Influxdb-Version", tsVersion)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, err := w.Write([]byte("{ \"message\": \"Test Response\" }"))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
	}))
	defer ts.Close()
	tc, err := New(ClientConfig{
		Host:     ts.URL,
		Token:    "my-token",
		Database: "my-database",
	})
	require.NoError(t, err)
	err = tc.Write(context.Background(), []byte("data"))
	require.Error(t, err)
	var serr *ServerError
	require.ErrorAs(t, err, &serr)
	assert.Equal(t, 400, serr.StatusCode)
	assert.Equal(t, "Test Response", serr.Message)
	assert.Len(t, serr.Headers, 6)
	assert.Equal(t, traceID, serr.Headers["Trace-Id"][0])
	assert.Equal(t, build, serr.Headers["X-Influxdb-Build"][0])
	assert.Equal(t, tsVersion, serr.Headers["X-Influxdb-Version"][0])
}

func TestWriteDatabaseNotSet(t *testing.T) {
	p := NewPointWithMeasurement("cpu")
	p.SetTag("host", "local")
	p.SetField("usage_user", 16.75)
	c, err := New(ClientConfig{
		Host:  "http://localhost:8086",
		Token: "my-token",
	})
	require.NoError(t, err)
	err = c.WritePoints(context.Background(), []*Point{p})
	assert.Error(t, err)
	assert.EqualError(t, err, "database not specified")
}

func TestWriteWithOptionsNotSet(t *testing.T) {
	p := NewPointWithMeasurement("cpu")
	p.SetTag("host", "local")
	p.SetField("usage_user", 16.75)
	c, err := New(ClientConfig{
		Host:     "http://localhost:8086",
		Token:    "my-token",
		Database: "my-database",
	})
	require.NoError(t, err)
	err = c.WritePointsWithOptions(context.Background(), nil, p)
	assert.Error(t, err)
	assert.EqualError(t, err, "options not set")
}

func TestMakeHTTPParamsBody(t *testing.T) {
	points := genPoints(100)
	byts := points2bytes(t, points)

	c, err := New(ClientConfig{
		Host:     "http://localhost",
		Token:    "my-token",
		Database: "my-database",
	})
	require.NoError(t, err)

	for _, gzipThreshold := range []int{
		0, // gzipping disabled
		1, // gzipping enabled
	} {
		c.config.WriteOptions.GzipThreshold = gzipThreshold

		params, err := c.makeHTTPParams(byts, c.config.WriteOptions)
		assert.NoError(t, err)

		// copy URL
		urlObj := *params.endpointURL
		urlObj.RawQuery = params.queryParams.Encode()

		fullURL := urlObj.String()

		req, err := http.NewRequestWithContext(context.Background(), params.httpMethod, fullURL, params.body)
		assert.NoError(t, err)

		slurp1, err := io.ReadAll(req.Body)
		assert.NoError(t, err)

		newBody, err := req.GetBody()
		assert.NoError(t, err)

		slurp2, err := io.ReadAll(newBody)
		assert.NoError(t, err)

		assert.Equal(t, string(slurp1), string(slurp2))
	}
}

func TestWriteWithClientTimeout(t *testing.T) {
	timeout := 500 * time.Millisecond
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(timeout + 1*time.Second)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()
	c, err := New(ClientConfig{
		Host:     ts.URL,
		Token:    "my-token",
		Database: "my-database",
		Timeout:  timeout,
	})
	require.NoError(t, err)

	err = c.Write(context.Background(), []byte("data"))
	assert.Error(t, err)
	assert.ErrorContains(t, err, "context deadline exceeded")

	p := NewPointWithMeasurement("cpu")
	p.SetTag("host", "local")
	p.SetField("usage_user", 16.75)
	err = c.WritePoints(context.Background(), []*Point{p})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "context deadline exceeded")

	now := time.Now()
	s := sampleDataStruct(now)
	err = c.WriteData(context.Background(), []any{s})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "context deadline exceeded")
}

func TestWriteWithClientWriteTimeout(t *testing.T) {
	timeout := 100 * time.Millisecond
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(timeout + 1*time.Second)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()
	c, err := New(ClientConfig{
		Host:         ts.URL,
		Token:        "my-token",
		Database:     "my-database",
		WriteTimeout: timeout,
	})
	require.NoError(t, err)

	err = c.Write(context.Background(), []byte("data"))
	assert.Error(t, err)
	assert.ErrorContains(t, err, "context deadline exceeded")

	p := NewPointWithMeasurement("temp")
	p.SetTag("location", "harfa")
	p.SetField("spot", 21.3)
	err = c.WritePoints(context.Background(), []*Point{p})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "context deadline exceeded")

	now := time.Now()
	s := sampleDataStruct(now)
	err = c.WriteData(context.Background(), []any{s})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "context deadline exceeded")
}

func TestWriteWithMaxIdleConnections(t *testing.T) {
	requestCount := 0
	uniqueConnectionCount := 0
	var addrMap sync.Map
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// initialization of query client
		if r.Method == "PRI" {
			return
		}
		requestCount++
		addr, ok := r.Context().Value(http.LocalAddrContextKey).(net.Addr)
		if !ok {
			t.Errorf("could not get local address from context: %v", addr)
		}
		_, loaded := addrMap.LoadOrStore(addr, true)
		if !loaded {
			uniqueConnectionCount++
		}
		// Add some sleep time to ensure that all parallel requests reach the server before any of them finishes.
		time.Sleep(1 * time.Second)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	maxIdleConnections := 2
	c, err := New(ClientConfig{
		Host:               ts.URL,
		Token:              "my-token",
		Database:           "my-database",
		MaxIdleConnections: maxIdleConnections,
	})
	require.NoError(t, err)

	writeInParallel := func(callCount int) {
		var wg sync.WaitGroup
		wg.Add(callCount)
		for i := 1; i <= callCount; i++ {
			go func() {
				defer wg.Done()
				err := c.Write(context.Background(), []byte("data"))
				require.NoError(t, err)
			}()
		}
		wg.Wait()
	}

	// 1st batch: do 5 writes in parallel to open 5 new connections.
	batch1Count := 5
	writeInParallel(batch1Count)

	// Check that 5 unique connections were used.
	assert.Equal(t, batch1Count, uniqueConnectionCount)
	assert.Equal(t, batch1Count, requestCount)

	// 2nd batch: do another 5 writes in parallel.
	batch2Count := 5
	writeInParallel(batch2Count)

	// Check that only 5+3 unique connections were used (instead of 5+5)
	// as 2 idle connections were reused from the 1st batch.
	assert.Equal(t, batch1Count+batch2Count-maxIdleConnections, uniqueConnectionCount)
	assert.Equal(t, batch1Count+batch2Count, requestCount)
}

func TestToV3PrecisionString(t *testing.T) {
	assert.Equal(t, "nanosecond", toV3PrecisionString(lineprotocol.Nanosecond))
	assert.Equal(t, "microsecond", toV3PrecisionString(lineprotocol.Microsecond))
	assert.Equal(t, "millisecond", toV3PrecisionString(lineprotocol.Millisecond))
	assert.Equal(t, "second", toV3PrecisionString(lineprotocol.Second))
	assert.Panics(t, func() {
		toV3PrecisionString(5)
	})
}
