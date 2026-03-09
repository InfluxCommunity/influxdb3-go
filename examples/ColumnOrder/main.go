package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
)

func main() {
	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     os.Getenv("INFLUX_HOST"),
		Token:    os.Getenv("INFLUX_TOKEN"),
		Database: os.Getenv("INFLUX_DATABASE"),
	})
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			panic(err)
		}
	}()

	// In InfluxDB 3 Enterprise, first write defines physical tag column order.
	// Put the most queried tags first (for example: region, then host).
	p := influxdb3.NewPointWithMeasurement("cpu_order_example").
		SetTag("service", "api").
		SetTag("host", "web-01").
		SetTag("region", "us-east").
		SetField("usage_user", 12.3).
		SetTimestamp(time.Now())

	err = client.WritePoints(
		context.Background(),
		[]*influxdb3.Point{p},
		influxdb3.WithTagOrder("region", "host", "service"),
	)
	if err != nil {
		panic(err)
	}

	// Query back the written data.
	query := `
	SELECT region, host, service, usage_user, time
	FROM cpu_order_example
	WHERE time >= now() - interval '5 minutes'
	AND region = 'us-east'
	AND host = 'web-01'
	`
	iterator, err := client.Query(context.Background(), query)
	if err != nil {
		panic(err)
	}
	for iterator.Next() {
		value := iterator.Value()
		fmt.Printf("region=%s host=%s service=%s usage_user=%v time=%v\n",
			value["region"], value["host"], value["service"], value["usage_user"], value["time"])
	}
}
