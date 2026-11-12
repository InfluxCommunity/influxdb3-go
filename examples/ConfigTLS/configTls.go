package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
)

func main() {
	// Retrieve credentials and TLS settings from environment variables.
	// When using TLS/mTLS, the host URL must use the "https://" scheme.
	host := os.Getenv("INFLUX_HOST")
	token := os.Getenv("INFLUX_TOKEN")
	database := os.Getenv("INFLUX_DATABASE")

	// TLS Configuration:
	// Specify the path to a PEM file containing custom or self-signed Root CA certificates
	// to verify the InfluxDB server certificate.
	sslRootsFilePath := os.Getenv("INFLUX_SSL_ROOTS_FILE_PATH")

	// mTLS (Mutual TLS) Configuration:
	// In mTLS, both the client and server verify each other's certificates.
	// You can configure client credentials using either:
	//
	// Option A: PEM certificate and private key files
	clientCertPath := os.Getenv("INFLUX_CLIENT_CERT_PATH") // e.g. "/path/to/client.crt" or "/path/to/client.pem"
	clientKeyPath := os.Getenv("INFLUX_CLIENT_KEY_PATH")   // e.g. "/path/to/client.key"

	// Option B: PKCS#12 (.p12) certificate bundle and decryption password
	// clientCertPath := os.Getenv("INFLUX_CLIENT_CERT_PATH") // e.g. "/path/to/client.p12"
	clientKeyPassword := os.Getenv("INFLUX_CLIENT_KEY_PASSWORD") // e.g. "secret"

	timeout := 10 * time.Second

	config := influxdb3.ClientConfig{
		Host:     host,
		Token:    token,
		Database: database,

		// TLS: Custom Root CA certificates for server verification
		SSLRootsFilePath: sslRootsFilePath,

		// mTLS: Client certificate, private key, and optional password
		ClientCertificatePath: clientCertPath,
		ClientKeyPath:         clientKeyPath,
		ClientKeyFilePassword: clientKeyPassword,

		// Connection parameters
		WriteTimeout:          timeout,
		QueryTimeout:          2 * time.Minute,
		IdleConnectionTimeout: 90 * time.Second,
		MaxIdleConnections:    10,
	}

	// Instantiate the client with TLS/mTLS configuration.
	client, err := influxdb3.New(config)
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

	// Write data over the secure TLS/mTLS connection.
	p := influxdb3.NewPointWithMeasurement("tls_stat").
		SetTag("location", "datacenter-1").
		SetField("temperature", 22.4).
		SetField("secure", true).
		SetTimestamp(time.Now())

	err = client.WritePoints(context.Background(), []*influxdb3.Point{p})
	if err != nil {
		panic(err)
	}

	fmt.Println("Successfully wrote point over secure TLS/mTLS connection.")

	// Query data over the secure connection.
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	query := `
		SELECT *
		FROM tls_stat
		WHERE time >= now() - interval '5 minutes'
		ORDER BY time DESC
		LIMIT 1
	`
	iterator, err := client.Query(ctx, query)
	if err != nil {
		panic(err)
	}

	for iterator.Next() {
		value := iterator.Value()
		fmt.Printf("Query result - location: %s, temperature: %v, secure: %v, time: %v\n",
			value["location"],
			value["temperature"],
			value["secure"],
			value["time"],
		)
	}
}
