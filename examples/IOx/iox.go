package main

import (
	"context"
	"fmt"
	"github.com/apache/arrow/go/v15/arrow"
	"os"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/influxdb3"
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

	// Create point using full params constructor
	p := influxdb3.NewPoint("stat",
		map[string]string{"location": "Paris"},
		map[string]interface{}{
			"temperature": 24.5,
			"humidity":    40,
		},
		time.Now())

	// write point synchronously
	err = client.WritePoints(context.Background(), []*influxdb3.Point{p})
	if err != nil {
		panic(err)
	}

	// Create point using fluent style
	p = influxdb3.NewPointWithMeasurement("stat").
		SetTag("location", "London").
		SetField("temperature", 17.1).
		SetField("humidity", 65).
		SetTimestamp(time.Now())

	// write point synchronously
	err = client.WritePoints(context.Background(), []*influxdb3.Point{p})
	if err != nil {
		panic(err)
	}

	// Prepare custom type
	sensorData := struct {
		Table string    `lp:"measurement"`
		Unit  string    `lp:"tag,location"`
		Temp  float64   `lp:"field,temperature"`
		Humid int64     `lp:"field,humidity"`
		Time  time.Time `lp:"timestamp"`
	}{"stat", "Madrid", 33.8, 35, time.Now()}

	// Write point
	err = client.WriteData(context.Background(), []any{sensorData})
	if err != nil {
		panic(err)
	}

	// Or write directly line protocol
	line := fmt.Sprintf("stat,location=Berlin temperature=%f,humidity=%di", 20.1, 55)
	err = client.Write(context.Background(), []byte(line))
	if err != nil {
		panic(err)
	}

	// Prepare FlightSQL query
	query := `
    SELECT *
    FROM stat
    WHERE
	time >= now() - interval '5 minute'
    AND
    location IN ('Paris', 'London', 'Madrid')
  `

	// Run the query
	iterator, err := client.Query(context.Background(), query)
	if err != nil {
		panic(err)
	}
	for iterator.Next() {
		value := iterator.Value()
		fmt.Printf("%s at %v:\n", value["location"],
			(value["time"].(arrow.Timestamp)).ToTime(arrow.Nanosecond).Format(time.RFC822))
		fmt.Printf("  temperature: %f\n", value["temperature"])
		fmt.Printf("  humidity   : %d%%\n", value["humidity"])
	}
}
