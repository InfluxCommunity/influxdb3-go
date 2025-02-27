package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
)

func main() {
	// Retrieve credentials from environment variables.
	url := os.Getenv("INFLUX_URL")
	token := os.Getenv("INFLUX_TOKEN")
	database := os.Getenv("INFLUX_DATABASE")
	// (optional) Custom SSL root certificates file path
	sslRootsFilePath := os.Getenv("INFLUX_SSL_ROOTS_FILE_PATH")
	// (optional) Proxy URL
	proxyURL := os.Getenv("INFLUX_PROXY_URL")

	// Instantiate a client using your credentials.
	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:             url,
		Token:            token,
		Database:         database,
		SSLRootsFilePath: sslRootsFilePath,
		Proxy:            proxyURL,
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

	// Create a Point using the full params constructor.
	p := influxdb3.NewPoint("stat",
		map[string]string{"location": "Paris"},
		map[string]any{
			"temperature": 24.5,
			"humidity":    40,
		},
		time.Now())

	// Write the point synchronously.
	err = client.WritePoints(context.Background(), []*influxdb3.Point{p})
	if err != nil {
		panic(err)
	}

	// Create a Point using the fluent interface (method chaining).
	p = influxdb3.NewPointWithMeasurement("stat").
		SetTag("location", "London").
		SetField("temperature", 17.1).
		SetField("humidity", 65).
		SetTimestamp(time.Now())

	// Write the point synchronously.
	err = client.WritePoints(context.Background(), []*influxdb3.Point{p})
	if err != nil {
		panic(err)
	}

	// Construct data as a custom type.
	sensorData := struct {
		Table string    `lp:"measurement"`
		Unit  string    `lp:"tag,location"`
		Temp  float64   `lp:"field,temperature"`
		Humid int64     `lp:"field,humidity"`
		Time  time.Time `lp:"timestamp"`
	}{"stat", "Madrid", 33.8, 35, time.Now()}

	// Write the data.
	err = client.WriteData(context.Background(), []any{sensorData})
	if err != nil {
		panic(err)
	}

	// Provide data as a line protocol string.
	line := fmt.Sprintf("stat,location=Berlin temperature=%f,humidity=%di", 20.1, 55)

	// Write the line protocol string.
	err = client.Write(context.Background(), []byte(line))
	if err != nil {
		panic(err)
	}

	// Prepare an SQL query
	query := `
    SELECT *
    FROM stat
    WHERE time >= now() - interval '5 minutes'
    AND location IN ('Paris', 'London', 'Madrid')
  `

	// Run the query
	iterator, err := client.Query(context.Background(), query)
	if err != nil {
		panic(err)
	}
	for iterator.Next() {
		// The query iterator returns each row as a map[string]interface{}.
		// The keys are the column names, allowing you to access the values by column name.
		value := iterator.Value()
		fmt.Printf("%s at %v:\n", value["location"],
			(value["time"].(time.Time)).Format(time.RFC822))
		fmt.Printf("  temperature: %f\n", value["temperature"])
		fmt.Printf("  humidity   : %d%%\n", value["humidity"])
	}
}
