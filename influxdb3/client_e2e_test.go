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
	err = client.WritePoints(context.Background(), database, p)
	require.NoError(t, err)

	sensorData := struct {
		Table  string    `lp:"measurement"`
		Unit   string    `lp:"tag,unit"`
		Avg    float64   `lp:"field,avg"`
		Max    float64   `lp:"field,max"`
		TestId int64     `lp:"field,testId"`
		Time   time.Time `lp:"timestamp"`
	}{"stat", "temperature", avg2, max2, testId, time.Now()}
	err = client.WriteData(context.Background(), database, sensorData)
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

	// retry query few times ultil data updates
	sleepTime := 2 * time.Second

	time.Sleep(sleepTime)
	iterator, err := client.Query(context.Background(), database, query)
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

	iterator, err = client.QueryInfluxQL(context.Background(), database, "SHOW MEASUREMENTS")
	require.NoError(t, err)
	assert.NotNil(t, iterator.Raw())
}
