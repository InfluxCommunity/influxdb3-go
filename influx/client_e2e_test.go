package influx_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/bonitoo-io/influxdb3-go/influx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteAndQueryExample(t *testing.T) {
	testId := time.Now().UnixNano()

	url := os.Getenv("TESTING_INFLUXDB_URL")
	token := os.Getenv("TESTING_INFLUXDB_TOKEN")
	bucket := os.Getenv("TESTING_INFLUXDB_BUCKET")

	client, err := influx.New(influx.Params{
		ServerURL: url,
		AuthToken: token,
	})

	require.NoError(t, err)
	defer client.Close()

	// Write test

	p := influx.NewPointWithMeasurement("stat").
		AddTag("unit", "temperature").
		AddField("avg", 23.2).
		AddField("max", 45.0).
		AddField("testId", testId).
		SetTimestamp(time.Now())
	err = client.WritePoints(context.Background(), bucket, p)
	require.NoError(t, err)

	sensorData := struct {
		Table  string    `lp:"measurement"`
		Unit   string    `lp:"tag,unit"`
		Avg    float64   `lp:"field,avg"`
		Max    float64   `lp:"field,max"`
		TestId int64     `lp:"field,testId"`
		Time   time.Time `lp:"timestamp"`
	}{"stat", "temperature", 22.3, 40.3, testId, time.Now()}
	err = client.WriteData(context.Background(), bucket, sensorData)
	require.NoError(t, err)

	// Query test
	query := fmt.Sprintf(`
		SELECT 1
		FROM "stat"
		WHERE
		time >= now() - interval '10 minute'
		AND
		"testId" = %d
	`, testId)

	// retry query few times ultil data updates
	maxTries := 10
	sleepTime := 500 * time.Millisecond

	success := false
	for try := 0; try < maxTries; try++ {
		time.Sleep(sleepTime)
		reader, err := client.Query(context.Background(), bucket, query, nil)
		require.NoError(t, err)

		lines := 0
		for reader.Next() {
			record := reader.Record()
			err = reader.Err()
			require.NoError(t, err)

			lines += int(record.NumRows())
		}
		if lines == 2 {
			success = true
			break
		}
	}
	assert.True(t, success)
}
