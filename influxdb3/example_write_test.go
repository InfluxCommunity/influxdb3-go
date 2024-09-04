/*
 The MIT License

 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

 The above copyright notice and this permission notice shall be included in
 all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 THE SOFTWARE.
*/

package influxdb3

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/influxdata/line-protocol/v2/lineprotocol"
)

func ExampleClient_Write() {
	client, err := NewFromEnv()
	if err != nil {
		log.Fatal()
	}
	defer client.Close()

	l0 := "cpu,host=localhost usage_user=16.75"
	l1 := "cpu,host=gw usage_user=2.90"
	data := []byte(strings.Join([]string{l0, l1}, "\n"))

	// write line protocol
	err = client.Write(context.Background(), data)
	if err != nil {
		log.Fatal()
	}

	// write line protocol with options
	err = client.Write(context.Background(), data, WithDatabase("another-database"), WithGzipThreshold(64))
	if err != nil {
		log.Fatal()
	}
}

func ExampleClient_WritePoints() {
	client, err := NewFromEnv()
	if err != nil {
		log.Fatal()
	}
	defer client.Close()

	p0 := NewPointWithMeasurement("cpu")
	p0.SetTag("host", "localhost")
	p0.SetField("usage_user", 16.75)
	p0.SetTimestamp(time.Now())
	points := []*Point{p0}

	// write points
	err = client.WritePoints(context.Background(), points)
	if err != nil {
		log.Fatal()
	}

	// write points with options
	err = client.WritePoints(context.Background(), points, WithPrecision(lineprotocol.Second))
	if err != nil {
		log.Fatal()
	}
}

func ExampleClient_WriteData() {
	type AirSensor struct {
		Measurement string    `lp:"measurement"`
		Sensor      string    `lp:"tag,sensor"`
		ID          string    `lp:"tag,device_id"`
		Temp        float64   `lp:"field,temperature"`
		Hum         int       `lp:"field,humidity"`
		Time        time.Time `lp:"timestamp"`
		Description string    `lp:"-"`
	}

	client, err := NewFromEnv()
	if err != nil {
		log.Fatal()
	}
	defer client.Close()

	p0 := AirSensor{
		"air",
		"SHT31",
		"10",
		23.5,
		55,
		time.Now(),
		"Room temp",
	}
	points := []any{&p0}

	// write points
	err = client.WriteData(context.Background(), points)
	if err != nil {
		log.Fatal()
	}

	// write points with options
	err = client.WriteData(context.Background(), points, WithDefaultTags(map[string]string{
		"version": "0.1",
	}))
	if err != nil {
		log.Fatal()
	}
}

func ExampleClient_severError() {
	client, err := NewFromEnv()
	if err != nil {
		log.Fatal()
	}
	defer client.Close()

	err = client.Write(context.Background(),
		[]byte("air,sensor=HRF03,device_ID=42 humidity=67.1,temperature="))

	if err != nil {
		log.Printf("WARN write failed: %s", err.Error())
		var svErr *ServerError
		errors.As(err, &svErr)
		log.Printf("   ServerError headers:")
		for key, val := range svErr.Headers {
			log.Printf("    %s = %s", key, val)
		}
	}
}
