package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/influxdb3"
)

func main() {
	// Use env variables to initialize client
	url := os.Getenv("INFLUXDB_URL")
	token := os.Getenv("INFLUXDB_TOKEN")
	database := os.Getenv("INFLUXDB_DATABASE")

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
		map[string]string{"unit": "temperature"},
		map[string]interface{}{"avg": 24.5, "max": 45.0},
		time.Now())
	// write point synchronously
	err = client.WritePoints(context.Background(), p)
	if err != nil {
		panic(err)
	}
	// Create point using fluent style
	p = influxdb3.NewPointWithMeasurement("stat").
		SetTag("unit", "temperature").
		SetField("avg", 23.2).
		SetField("max", 45.0).
		SetTimestamp(time.Now())
	// write point synchronously
	err = client.WritePoints(context.Background(), p)
	if err != nil {
		panic(err)
	}
	// Prepare custom type
	sensorData := struct {
		Table string    `lp:"measurement"`
		Unit  string    `lp:"tag,unit"`
		Avg   float64   `lp:"field,avg"`
		Max   float64   `lp:"field,max"`
		Time  time.Time `lp:"timestamp"`
	}{"stat", "temperature", 22.3, 40.3, time.Now()}
	// Write point
	err = client.WriteData(context.Background(), sensorData)
	if err != nil {
		panic(err)
	}
	// Or write directly line protocol
	line := fmt.Sprintf("stat,unit=temperature avg=%f,max=%f", 23.5, 45.0)
	err = client.Write(context.Background(), []byte(line))
	if err != nil {
		panic(err)
	}

	// Prepare FlightSQL query
	query := `
    SELECT *
    FROM "stat"
    WHERE
    time >= now() - interval '5 minute'
    AND
    "unit" IN ('temperature')
  `

	iterator, err := client.Query(context.Background(), query)

	if err != nil {
		panic(err)
	}

	for iterator.Next() {
		value := iterator.Value()

		fmt.Printf("avg is %f\n", value["avg"])
		fmt.Printf("max is %f\n", value["max"])
	}

}
