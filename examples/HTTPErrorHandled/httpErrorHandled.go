package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
)

// Demonstrates working with HTTP response headers in ServerError
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

	// Attempt to write line protocol synchronously
	// N.B. faulty line protocol used here, because it
	// guarantees a server error, but errors can be thrown
	// for other reasons, such as 503 temporary unavailable
	// or even 429 too many requests.
	err = client.Write(context.Background(),
		[]byte("air,sensor=HRF03,device_ID=42 humidity=67.1,temperature="))

	if err != nil {
		logMessage := "WARNING write error: " + err.Error()
		logMessage += "\n   ServerError.Headers:\n"
		var svErr *influxdb3.ServerError
		errors.As(err, &svErr)
		for key, value := range svErr.Headers {
			logMessage += fmt.Sprintf("      %s: %s\n", key, value)
		}
		log.Println(logMessage)
	}
}
