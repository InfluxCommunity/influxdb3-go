//go:build e2e
// +build e2e

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

package influxdb3_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/influxdb3"
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteAndQueryExample(t *testing.T) {
	now := time.Now().UTC()
	testId := now.UnixNano()

	url := os.Getenv("TESTING_INFLUXDB_URL")
	token := os.Getenv("TESTING_INFLUXDB_TOKEN")
	database := os.Getenv("TESTING_INFLUXDB_DATABASE")

	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     url,
		Token:    token,
		Database: database,
	})
	require.NoError(t, err)
	defer client.Close()

	tableName := "weather"
	tagKey := "location"
	tagValue := "sun-valley-1"

	// Write test

	p := influxdb3.NewPointWithMeasurement(tableName).
		SetTag(tagKey, tagValue).
		SetField("temp", 15.5).
		SetField("index", 80).
		SetField("uindex", uint64(800)).
		SetField("valid", true).
		SetField("testId", testId).
		SetField("text", "a1").
		SetTimestamp(now)
	err = client.WritePoints(context.Background(), []*influxdb3.Point{p})
	require.NoError(t, err)

	sensorData := struct {
		Table  string    `lp:"measurement"`
		Loc    string    `lp:"tag,location"`
		Temp   float64   `lp:"field,temp"`
		Index  int64     `lp:"field,index"`
		UIndex uint64    `lp:"field,uindex"`
		Valid  bool      `lp:"field,valid"`
		TestId int64     `lp:"field,testId"`
		Text   string    `lp:"field,text"`
		Time   time.Time `lp:"timestamp"`
	}{tableName, tagValue, 24.5, -15, uint64(150), false, testId, "b1", now.Add(1 * time.Second)}
	err = client.WriteData(context.Background(), []any{sensorData})
	require.NoError(t, err)

	// SQL query test

	query := fmt.Sprintf(`
		SELECT *
		FROM "%s"
		WHERE
		time >= now() - interval '10 minute'
		AND
		"%s" = '%s'
		AND
		"testId" = %d
		ORDER BY time
	`, tableName, tagKey, tagValue, testId)

	// retry query few times until data updates
	sleepTime := 2 * time.Second
	time.Sleep(sleepTime)
	iterator, err := client.Query(context.Background(), query)
	require.NoError(t, err)
	require.NotNil(t, iterator)

	// row #1

	hasValue := iterator.Next()
	assert.True(t, hasValue)
	value := iterator.Value()
	assert.Equal(t, tagValue, value[tagKey])
	assert.Equal(t, 15.5, value["temp"])
	assert.Equal(t, int64(80), value["index"])
	assert.Equal(t, uint64(800), value["uindex"])
	assert.Equal(t, true, value["valid"])
	assert.Equal(t, "a1", value["text"])
	assert.Equal(t, now, value["time"].(arrow.Timestamp).ToTime(arrow.Nanosecond))

	// row #2

	hasValue = iterator.Next()
	assert.True(t, hasValue)
	value = iterator.Value()
	assert.Equal(t, tagValue, value[tagKey])
	assert.Equal(t, 24.5, value["temp"])
	assert.Equal(t, int64(-15), value["index"])
	assert.Equal(t, uint64(150), value["uindex"])
	assert.Equal(t, false, value["valid"])
	assert.Equal(t, "b1", value["text"])
	assert.Equal(t, now.Add(1*time.Second), value["time"].(arrow.Timestamp).ToTime(arrow.Nanosecond))

	assert.False(t, iterator.Done())

	assert.False(t, iterator.Next())
	assert.True(t, iterator.Done())

	iterator, err = client.Query(context.Background(), query)
	hasValue = iterator.Next()
	assert.True(t, hasValue)
	points := iterator.AsPoints()
	assert.Equal(t, uint64(800), points.Fields["uindex"])

	hasValue = iterator.Next()
	assert.True(t, hasValue)

	newPoint, _ := points.AsPointWithMeasurement("to_write")
	assert.True(t, newPoint != nil)
}

func TestQueryWithParameters(t *testing.T) {
	now := time.Now().UTC()
	testId := now.UnixNano()

	url := os.Getenv("TESTING_INFLUXDB_URL")
	token := os.Getenv("TESTING_INFLUXDB_TOKEN")
	database := os.Getenv("TESTING_INFLUXDB_DATABASE")

	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     url,
		Token:    token,
		Database: database,
	})
	require.NoError(t, err)
	defer client.Close()

	p := influxdb3.NewPointWithMeasurement("weather").
		SetTag("location", "sun-valley-1").
		SetField("temp", 15.5).
		SetField("index", 80).
		SetField("uindex", uint64(800)).
		SetField("valid", true).
		SetField("testId", testId).
		SetField("text", "a1").
		SetTimestamp(now)
	err = client.WritePoints(context.Background(), []*influxdb3.Point{p})
	require.NoError(t, err)

	query := `
		SELECT *
		FROM weather
		WHERE
		time >= now() - interval '10 minute'
		AND
		location = $location
		AND
		"testId" = $testId
		ORDER BY time
	`
	parameters := influxdb3.QueryParameters{
		"location": "sun-valley-1",
		"testId":   testId,
	}

	sleepTime := 5 * time.Second
	time.Sleep(sleepTime)

	iterator, err := client.QueryWithParameters(context.Background(), query, parameters)
	require.NoError(t, err)
	require.NotNil(t, iterator)

	hasValue := iterator.Next()
	assert.True(t, hasValue)

	value := iterator.Value()
	assert.Equal(t, "sun-valley-1", value["location"])
	assert.Equal(t, 15.5, value["temp"])
	assert.Equal(t, int64(80), value["index"])
	assert.Equal(t, uint64(800), value["uindex"])
	assert.Equal(t, true, value["valid"])
	assert.Equal(t, "a1", value["text"])
	assert.Equal(t, now, value["time"].(arrow.Timestamp).ToTime(arrow.Nanosecond))

	assert.False(t, iterator.Done())
	assert.False(t, iterator.Next())
	assert.True(t, iterator.Done())
}

func TestQueryDatabaseDoesNotExist(t *testing.T) {
	url := os.Getenv("TESTING_INFLUXDB_URL")
	token := os.Getenv("TESTING_INFLUXDB_TOKEN")

	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     url,
		Token:    token,
		Database: "does not exist",
	})

	iterator, err := client.Query(context.Background(), "SHOW TABLES")
	assert.Nil(t, iterator)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "bucket \"does not exist\" not found")
}

func TestQuerySchema(t *testing.T) {
	url := os.Getenv("TESTING_INFLUXDB_URL")
	token := os.Getenv("TESTING_INFLUXDB_TOKEN")
	database := os.Getenv("TESTING_INFLUXDB_DATABASE")

	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     url,
		Token:    token,
		Database: database,
	})

	iterator, err := client.Query(context.Background(), "SHOW TABLES")
	require.NoError(t, err)
	assert.NotNil(t, iterator.Raw())
}

func TestQuerySchemaWithOptions(t *testing.T) {
	url := os.Getenv("TESTING_INFLUXDB_URL")
	token := os.Getenv("TESTING_INFLUXDB_TOKEN")
	database := os.Getenv("TESTING_INFLUXDB_DATABASE")

	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     url,
		Token:    token,
		Database: "does not exist",
	})

	iterator, err := client.Query(context.Background(), "SHOW TABLES", influxdb3.WithDatabase(database))
	require.NoError(t, err)
	assert.NotNil(t, iterator.Raw())
}

func TestQuerySchemaInfluxQL(t *testing.T) {
	url := os.Getenv("TESTING_INFLUXDB_URL")
	token := os.Getenv("TESTING_INFLUXDB_TOKEN")
	database := os.Getenv("TESTING_INFLUXDB_DATABASE")

	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     url,
		Token:    token,
		Database: database,
	})

	iterator, err := client.Query(context.Background(), "SHOW MEASUREMENTS", influxdb3.WithQueryType(influxdb3.InfluxQL))
	require.NoError(t, err)
	assert.NotNil(t, iterator.Raw())
}

func TestWriteError(t *testing.T) {
	url := os.Getenv("TESTING_INFLUXDB_URL")
	token := os.Getenv("TESTING_INFLUXDB_TOKEN")
	database := os.Getenv("TESTING_INFLUXDB_DATABASE")

	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     url,
		Token:    token,
		Database: database,
	})
	require.NoError(t, err)

	err = client.Write(context.Background(), []byte("test,type=negative val="))
	require.Error(t, err)
	assert.NotPanics(t, func() { _ = err.(*influxdb3.ServerError) })
	assert.Regexp(t, "[0-9a-f]{16}", err.(*influxdb3.ServerError).Headers["Trace-Id"][0])
	b, perr := strconv.ParseBool(err.(*influxdb3.ServerError).Headers["Trace-Sampled"][0])
	require.NoError(t, perr)
	assert.False(t, b)
	assert.NotNil(t, err.(*influxdb3.ServerError).Headers["Strict-Transport-Security"])
	assert.Regexp(t, "[0-9a-f]{32}", err.(*influxdb3.ServerError).Headers["X-Influxdb-Request-Id"][0])
	assert.NotNil(t, err.(*influxdb3.ServerError).Headers["X-Influxdb-Build"][0])

}
