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
	"context"
	"log"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"google.golang.org/grpc"
)

func ExampleClient_Query() {
	client, err := influxdb3.NewFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// configure client timeout via context
	clientTimeout := 10 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()

	// query
	iterator, _ := client.Query(ctx,
		`SELECT count(*) FROM weather WHERE time >= now() - interval '5 minutes'`)

	for iterator.Next() {
		// process the result
	}

	// query with custom header
	iterator, _ = client.Query(ctx,
		`SELECT count(*) FROM stat WHERE time >= now() - interval '5 minutes'`,
		influxdb3.WithHeader("X-trace-ID", "#0122"))

	for iterator.Next() {
		// process the result
	}
}

func ExampleClient_QueryWithParameters() {
	client, err := influxdb3.NewFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	// query
	iterator, _ := client.QueryWithParameters(ctx,
		`SELECT count(*) FROM weather WHERE location = $location AND time >= now() - interval '5 minutes'`,
		influxdb3.QueryParameters{
			"location": "sun-valley-1",
		})

	for iterator.Next() {
		// process the result
	}

	// query with custom header
	iterator, _ = client.QueryWithParameters(ctx,
		`SELECT count(*) FROM weather WHERE location = $location AND time >= now() - interval '5 minutes'`,
		influxdb3.QueryParameters{
			"location": "sun-valley-1",
		},
		influxdb3.WithHeader("X-trace-ID", "#0122"))

	for iterator.Next() {
		// process the result
	}
}

func ExampleClient_QueryWithOptions() {
	client, err := influxdb3.NewFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	qIter, _ := client.Query(context.Background(),
		`SELECT time,location,name FROM temp WHERE time >= now() - interval '1 hour'`,
		influxdb3.WithDatabase("building204"),
		influxdb3.WithPrecision(lineprotocol.Millisecond),
		influxdb3.WithGzipThreshold(1_000_000),
		influxdb3.WithGrpcCallOption(grpc.MaxCallRecvMsgSize(5_000_000)),
	)

	for qIter.Next() {
		// process the result
	}
}
