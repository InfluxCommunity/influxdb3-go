package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/influxdb3"
	"github.com/InfluxCommunity/influxdb3-go/influxdb3/batching"
	"github.com/apache/arrow/go/v15/arrow"
)

func main() {
	// Create a random number generator
	r := rand.New(rand.NewSource(456))
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

	// Synchronous use

	// Create a Batcher with a size of 5
	b := batching.NewBatcher(batching.WithSize(5))

	// Simulate delay of a second
	t := time.Now().Add(-54 * time.Second)

	// Write 54 points synchronously to the batcher
	for i := 0; i < 54; i++ {
		p := influxdb3.NewPoint("stat",
			map[string]string{"location": "Paris"},
			map[string]any{
				"temperature": 15 + r.Float64()*20,
				"humidity":    30 + r.Int63n(40),
			},
			t)

		// Add the point to the batcher
		b.Add(p)

		// Update time
		t = t.Add(time.Second)

		// If the batcher is ready, write the batch to the client and reset the batcher
		if b.Ready() {
			err := client.WritePoints(context.Background(), b.Emit())
			if err != nil {
				panic(err)
			}
		}
	}

	// Write the final batch to the client
	err = client.WritePoints(context.Background(), b.Emit())
	if err != nil {
		panic(err)
	}

	// Asynchronous use

	// Create a batcher with a size of 5, a ready callback and an emit callback to write the batch to the client
	b = batching.NewBatcher(
		batching.WithSize(5),
		batching.WithReadyCallback(func() { fmt.Println("ready") }),
		batching.WithEmitCallback(func(points []*influxdb3.Point) {
			err = client.WritePoints(context.Background(), points)
			if err != nil {
				panic(err)
			}
		}),
	)

	// Simulate delay of a second
	t = time.Now().Add(-54 * time.Second)

	// Write 54 points synchronously to the batcher
	for i := 0; i < 54; i++ {
		p := influxdb3.NewPoint("stat",
			map[string]string{"location": "Madrid"},
			map[string]any{
				"temperature": 15 + r.Float64()*20,
				"humidity":    30 + r.Int63n(40),
			},
			t)

		// Add the point to the batcher
		b.Add(p)

		// Update time
		t = t.Add(time.Second)
	}

	// Write the final batch to the client
	err = client.WritePoints(context.Background(), b.Emit())
	if err != nil {
		panic(err)
	}

	// Prepare an SQL query
	query := `
    SELECT *
    FROM stat
    WHERE time >= now() - interval '5 minutes'
    AND location IN ('Paris', 'Madrid')
  `

	// Run the query
	iterator, err := client.Query(context.Background(), query)
	if err != nil {
		panic(err)
	}

	// Process the data
	for iterator.Next() {
		value := iterator.Value()
		fmt.Printf("%s at %v:\n", value["location"],
			(value["time"].(arrow.Timestamp)).ToTime(arrow.Nanosecond).Format(time.RFC822))
		fmt.Printf("  temperature: %f\n", value["temperature"])
		fmt.Printf("  humidity   : %d%%\n", value["humidity"])
	}
}
