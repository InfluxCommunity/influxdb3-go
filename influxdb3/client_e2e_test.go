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
	"testing"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/influxdb3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteAndQueryExample(t *testing.T) {
	testId := time.Now().UnixNano()

	const avg1 = 23.2
	const max1 = 45.0
	const avg2 = 25.8
	const max2 = 46.0

	url := os.Getenv("TESTING_INFLUXDB_URL")
	token := os.Getenv("TESTING_INFLUXDB_TOKEN")
	database := os.Getenv("TESTING_INFLUXDB_DATABASE")

	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:  url,
		Token: token,
		Database: database,
	})

	require.NoError(t, err)
	defer client.Close()

	// Write test

	p := influxdb3.NewPointWithMeasurement("stat").
		AddTag("unit", "temperature").
		AddField("avg", avg1).
		AddField("max", max1).
		AddField("testId", testId).
		SetTimestamp(time.Now())
	err = client.WritePoints(context.Background(), p)
	require.NoError(t, err)

	sensorData := struct {
		Table  string    `lp:"measurement"`
		Unit   string    `lp:"tag,unit"`
		Avg    float64   `lp:"field,avg"`
		Max    float64   `lp:"field,max"`
		TestId int64     `lp:"field,testId"`
		Time   time.Time `lp:"timestamp"`
	}{"stat", "temperature", avg2, max2, testId, time.Now()}
	err = client.WriteData(context.Background(), sensorData)
	require.NoError(t, err)

	// Query test

	query := fmt.Sprintf(`
		SELECT *
		FROM "stat"
		WHERE
		time >= now() - interval '10 minute'
		AND
		"testId" = %d
		ORDER BY time
	`, testId)

	// retry query few times until data updates
	sleepTime := 2 * time.Second

	time.Sleep(sleepTime)
	iterator, err := client.Query(context.Background(), query)
	require.NoError(t, err)

	hasValue := iterator.Next()
	assert.True(t, hasValue)
	value := iterator.Value()
	assert.Equal(t, value["avg"], avg1)
	assert.Equal(t, value["max"], max1)

	hasValue = iterator.Next()
	assert.True(t, hasValue)
	value = iterator.Value()
	assert.Equal(t, value["avg"], avg2)
	assert.Equal(t, value["max"], max2)

	assert.False(t, iterator.Done())

	assert.False(t, iterator.Next())
	assert.True(t, iterator.Done())

	iterator, err = client.Query(context.Background(), "SHOW NAMESPACES")
	require.NoError(t, err)
	assert.NotNil(t, iterator.Raw())

	options := influxdb3.QueryOptions{
		QueryType: influxdb3.InfluxQL,
	}
	iterator, err = client.QueryWithOptions(context.Background(), &options, "SHOW MEASUREMENTS")
	require.NoError(t, err)
	assert.NotNil(t, iterator.Raw())
}
