package main

import (
	"context"
	"fmt"
	"github.com/apache/arrow/go/v13/arrow"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/influxdb3"
)

func main() {
	url := "https://us-east-1-1.aws.cloud2.influxdata.com"
	token := "my-token"
	database := "my-database"

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
	err = client.WritePoints(context.Background(), influxdb3.NewPointWithMeasurement("stat").
		SetTag("unit", "temperature").
		SetDoubleField("avg", 23.2).
		SetDoubleField("max", 45.0))
	if err != nil {
		panic(err)
	}
	time.Sleep(1 * time.Second)

	err = client.WritePoints(context.Background(), influxdb3.NewPointWithMeasurement("stat").
		SetTag("unit", "temperature").
		SetDoubleField("avg", 28.0).
		SetDoubleField("max", 40.3))
	if err != nil {
		panic(err)
	}
	time.Sleep(1 * time.Second)

	err = client.WritePoints(context.Background(), influxdb3.NewPointWithMeasurement("stat").
		SetTag("unit", "temperature").
		SetDoubleField("avg", 23.2).
		SetDoubleField("max", 45.0))
	if err != nil {
		panic(err)
	}
	time.Sleep(1 * time.Second)

	//
	// Query Downsampled data
	//
	query := `
    SELECT
		date_bin('5 minutes', "time") as window_start,
		AVG("avg") as avg,
		MAX("max") as max
	FROM "stat"
	WHERE
	  "time" >= now() - interval '1 hour'
  	GROUP BY window_start
	ORDER BY window_start ASC;
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
		timestamp := int64(row.GetField("window_start").(arrow.Timestamp))

		avgValue := row.GetDoubleField("avg")
		maxValue := row.GetDoubleField("max")
		fmt.Printf("%s: avg is %.2f, max is %.2f\n", time.Unix(0, timestamp), *avgValue, *maxValue)

		//
		// write back downsampled date to 'stat_downsampled' measurement
		//
		downsampledPoint, err := row.AsPointWithMeasurement("stat_downsampled")
		if err != nil {
			panic(err)
		}

		downsampledPoint = downsampledPoint.
			RemoveField("window_start").
			SetTimestampWithEpoch(timestamp)

		err = client.WritePoints(context.Background(), downsampledPoint)
		if err != nil {
			panic(err)
		}
	}

}
