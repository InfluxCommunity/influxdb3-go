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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncode(t *testing.T) {

	now := time.Now()
	tests := []struct {
		name  string
		s     interface{}
		line  string
		error string
	}{{
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
		}, {
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
			name: "test missing struct field field name",
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

func genPoints(t *testing.T, count int) []*Point {
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
func points2bytes(t *testing.T, points []*Point) []byte {
	var bytes []byte
	for _, p := range points {
		bs, err := p.MarshalBinary(lineprotocol.Millisecond)
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
	for i := 0; i < len(b1); i++ {
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
		w.WriteHeader(204)
	}))
	defer ts.Close()
	options := DefaultWriteOptions
	options.Precision = lineprotocol.Millisecond
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
	correctPath = "/path/api/v2/write?bucket=my-database&org=my-org&precision=ms"
	err = c.Write(context.Background(), []byte("a f=1"))
	assert.NoError(t, err)
}

func TestWritePointsAndBytes(t *testing.T) {
	points := genPoints(t, 5000)
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
		w.WriteHeader(204)
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

	err = c.WritePoints(context.Background(), points...)
	assert.NoError(t, err)
	assert.Equal(t, 2, reqs)

	// test error
	err = c.Write(context.Background(), []byte("line"))
	require.Error(t, err)
	assert.Equal(t, 3, reqs)
	assert.Equal(t, "invalid: error lens are not equal 911244 vs 4", err.Error())
}

func TestWritePointsWithOptions(t *testing.T) {
	points := genPoints(t, 1)
	lp := points2bytes(t, points)
	correctPath := "/api/v2/write?bucket=x-db&org=&precision=ms"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// initialization of query client
		if r.Method == "PRI" {
			return
		}

		assert.EqualValues(t, correctPath, r.URL.String())
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		assert.Equal(t, string(lp), string(body))
		w.WriteHeader(204)
	}))
	defer ts.Close()
	c, err := New(ClientConfig{
		Host:     ts.URL,
		Token:    "my-token",
		Database: "my-database",
	})
	options := WriteOptions{
		Database:  "x-db",
		Precision: lineprotocol.Millisecond,
	}
	require.NoError(t, err)
	err = c.WritePointsWithOptions(context.Background(), &options, points...)
	assert.NoError(t, err)
}

func TestWriteData(t *testing.T) {
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
	lp := fmt.Sprintf("air,device_id=10,sensor=SHT31 humidity=55i,temperature=23.5 %d\n", now.UnixNano())
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// initialization of query client
		if r.Method == "PRI" {
			return
		}
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		assert.Equal(t, lp, string(body))
		w.WriteHeader(204)
	}))
	defer ts.Close()
	c, err := New(ClientConfig{
		Host:     ts.URL,
		Token:    "my-token",
		Database: "my-database",
	})
	require.NoError(t, err)
	err = c.WriteData(context.Background(), s)
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
	lp := fmt.Sprintf("air,device_id=10,sensor=SHT31 humidity=55i,temperature=23.5 %d\n", now.Unix())
	correctPath := "/api/v2/write?bucket=x-db&org=my-org&precision=s"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// initialization of query client
		if r.Method == "PRI" {
			return
		}
		assert.EqualValues(t, correctPath, r.URL.String())
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		assert.Equal(t, lp, string(body))
		w.WriteHeader(204)
	}))
	defer ts.Close()
	c, err := New(ClientConfig{
		Host:         ts.URL,
		Token:        "my-token",
		Organization: "my-org",
		Database:     "my-database",
	})
	options := WriteOptions{
		Database:  "x-db",
		Precision: lineprotocol.Second,
	}
	require.NoError(t, err)
	err = c.WriteDataWithOptions(context.Background(), &options, s)
	assert.NoError(t, err)
}

func TestGzip(t *testing.T) {
	points := genPoints(t, 1)
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
		w.WriteHeader(204)
	}))
	defer ts.Close()
	c, err := New(ClientConfig{
		Host:     ts.URL,
		Token:    "my-token",
		Database: "my-database",
	})
	require.NoError(t, err)
	//Test no gzip on small body
	err = c.Write(context.Background(), byts)
	assert.NoError(t, err)
	assert.False(t, wasGzip)
	// Test gzip on larger body
	points = genPoints(t, 100)
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
		xHeader := r.Header.Get("X-device")
		assert.Equal(t, "ab-01", xHeader)
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		assert.Equal(t, "cpu,host=local usage_user=16.75\n", string(body))
		w.WriteHeader(204)
	}))
	defer ts.Close()
	c, err := New(ClientConfig{
		Host:     ts.URL,
		Token:    "my-token",
		Database: "my-database",
		Headers: http.Header{
			"X-device": []string{"ab-01"},
		},
	})
	require.NoError(t, err)
	err = c.WritePoints(context.Background(), p)
	require.NoError(t, err)
}

func TestWriteErrorMarshalPoint(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
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

	err = c.WritePoints(context.Background(), p)
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
	err = c.WritePoints(context.Background(), p)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "error calling")
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
	err = c.WritePoints(context.Background(), p)
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
