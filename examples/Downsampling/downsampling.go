package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/influxdb3"
	"github.com/apache/arrow/go/v14/arrow"
)

func main() {
	// Use env variables to initialize client
	url := os.Getenv("INFLUX_URL")
	token := os.Getenv("INFLUX_TOKEN")
	database := os.Getenv("INFLUX_DATABASE")

	// Create a new client using an InfluxDB server base URL and an authentication token
	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     url,
		Token:    token,
		Database: database,
	})
	if err != nil {
		panic(err)
	}

	// Close client at the end and escalate error if present
	defer func(client *influxdb3.Client) {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}(client)

	//
	// Write data
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
	// Query Downsampled data
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
	// Execute downsampling query into PointValues
	//
	iterator, err := client.Query(context.Background(), query)
	if err != nil {
		panic(err)
	}

	for iterator.Next() {
		row := iterator.AsPoints()
		timestamp := (row.GetField("window_start").(arrow.Timestamp)).ToTime(arrow.Nanosecond)
		location := row.GetStringField("location")
		avgValue := row.GetDoubleField("avg")
		maxValue := row.GetDoubleField("max")
		fmt.Printf("%s %s temperature: avg %.2f, max %.2f\n", timestamp.Format(time.RFC822), *location, *avgValue, *maxValue)

		//
		// write back downsampled date to 'stat_downsampled' measurement
		//
		downsampledPoint, err := row.AsPointWithMeasurement("stat_downsampled")
		if err != nil {
			panic(err)
		}

		downsampledPoint = downsampledPoint.
			RemoveField("window_start").
			SetTimestampWithEpoch(timestamp.UnixNano())

		err = client.WritePoints(context.Background(), []*influxdb3.Point{downsampledPoint})
		if err != nil {
			panic(err)
		}
	}
}
