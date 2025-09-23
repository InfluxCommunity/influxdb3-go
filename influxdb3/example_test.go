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

package influxdb3_test

import (
	"log"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
)

func ExampleNew() {
	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:             "https://us-east-1-1.aws.cloud2.influxdata.com",
		Token:            "my-token",
		Database:         "my-database",
		SSLRootsFilePath: "/path/to/certificates.pem",
		Proxy:            "http://localhost:8888",
		// Connection parameters:
		WriteTimeout:          10 * time.Second,
		IdleConnectionTimeout: 90 * time.Second,
		MaxIdleConnections:    10,
		QueryTimeout:          2 * time.Minute,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
}

func ExampleNewFromConnectionString() {
	client, err := influxdb3.NewFromConnectionString("https://us-east-1-1.aws.cloud2.influxdata.com/?token=my-token&database=my-database")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
}

func ExampleNewFromEnv() {
	client, err := influxdb3.NewFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
}
