package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/bonitoo-io/influxdb3-go/influx"
)

func main() {
	// Use env variables to initialize client
	url := os.Getenv("INFLUXDB_URL")
	token := os.Getenv("INFLUXDB_TOKEN")
	bucket := os.Getenv("INFLUXDB_BUCKET")

	// Create a new client using an InfluxDB server base URL and an authentication token
	client, err := influx.New(influx.Params{
		ServerURL: url,
		AuthToken: token,
	})

	if err != nil {
		panic(err)
	}
	// Close client at the end
	defer client.Close()

	// Create point using full params constructor
	p := influx.NewPoint("stat",
		map[string]string{"unit": "temperature"},
		map[string]interface{}{"avg": 24.5, "max": 45.0},
		time.Now())
	// write point synchronously
	err = client.WritePoints(context.Background(), bucket, p)
	if err != nil {
		panic(err)
	}
	// Create point using fluent style
	p = influx.NewPointWithMeasurement("stat").
		AddTag("unit", "temperature").
		AddField("avg", 23.2).
		AddField("max", 45.0).
		SetTimestamp(time.Now())
	// write point synchronously
	err = client.WritePoints(context.Background(), bucket, p)
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
	err = client.WriteData(context.Background(), bucket, sensorData)
	if err != nil {
		panic(err)
	}
	// Or write directly line protocol
	line := fmt.Sprintf("stat,unit=temperature avg=%f,max=%f", 23.5, 45.0)
	err = client.Write(context.Background(), bucket, []byte(line))
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

	reader, err := client.Query(context.Background(), bucket, query, nil)

	if err != nil {
		panic(err)
	}

	// Print out query results
	fmt.Println("QUERY results:")
	for reader.Next() {
		record := reader.Record()
		b, err := json.MarshalIndent(record, "", "  ")
		if err != nil {
			panic(err)
		}
		fmt.Println(string(b))

		if err := reader.Err(); err != nil {
			panic(err)
		}
	}
}
