package main

import (
	"context"
	"fmt"
	"github.com/apache/arrow/go/v15/arrow"
	"os"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
)

func main() {
	// Retrieve credentials from environment variables.
	url := os.Getenv("INFLUX_URL")
	token := os.Getenv("INFLUX_TOKEN")
	database := os.Getenv("INFLUX_DATABASE")

	// Instantiate a client using your credentials.
	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     url,
		Token:    token,
		Database: database,
	})
	if err != nil {
		panic(err)
	}

	// Close the client when finished and raise any errors.
	defer func(client *influxdb3.Client) {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}(client)

	//
	// Write data to the 'stat' measurement
	//
	err = client.WritePoints(context.Background(), []*influxdb3.Point{
		influxdb3.NewPointWithMeasurement("stat").
			SetTag("location", "Paris").
			SetDoubleField("temperature", 23.2),
	})
	if err != nil {
		panic(err)
	}
	time.Sleep(1 * time.Second)

	err = client.WritePoints(context.Background(), []*influxdb3.Point{
		influxdb3.NewPointWithMeasurement("stat").
			SetTag("location", "Paris").
			SetDoubleField("temperature", 24.1),
	})
	if err != nil {
		panic(err)
	}
	time.Sleep(1 * time.Second)

	err = client.WritePoints(context.Background(), []*influxdb3.Point{
		influxdb3.NewPointWithMeasurement("stat").
			SetTag("location", "Paris").
			SetDoubleField("temperature", 23.9),
	})
	if err != nil {
		panic(err)
	}
	time.Sleep(1 * time.Second)

	//
	// Define a query using aggregate functions and grouping to downsample the data.
	//
	query := `
    SELECT
		DATE_BIN(INTERVAL '5 minutes', time) as window_start,
        location,
		AVG(temperature) as avg,
		MAX(temperature) as max
	  FROM stat
		WHERE
			time >= now() - interval '1 hour'
		GROUP BY window_start, location
		ORDER BY location, window_start
  `

	//
	// Execute the query and process the data.
	//

	iterator, err := client.Query(context.Background(), query)
	if err != nil {
		panic(err)
	}

	// Process the data as PointValues that can be used to write downsampled data back to the database.
	for iterator.Next() {
		row := iterator.AsPoints()
		timestamp := (row.GetField("window_start").(arrow.Timestamp)).ToTime(arrow.Nanosecond)
		location, _ := row.GetTag("location")
		avgValue := row.GetDoubleField("avg")
		maxValue := row.GetDoubleField("max")
		fmt.Printf("%s %s temperature: avg %.2f, max %.2f\n", timestamp.Format(time.RFC822), location, *avgValue, *maxValue)

		//
		// Write back downsampled data.
		//

		// Create a downsampled Point for the 'stat_downsampled' table.
		downsampledPoint, err := row.AsPointWithMeasurement("stat_downsampled")
		if err != nil {
			panic(err)
		}

		downsampledPoint = downsampledPoint.
			RemoveField("window_start").
			SetTimestampWithEpoch(timestamp.UnixNano())

		// Write the downsampled Point to the database.
		err = client.WritePoints(context.Background(), []*influxdb3.Point{downsampledPoint})
		if err != nil {
			panic(err)
		}
	}
}
