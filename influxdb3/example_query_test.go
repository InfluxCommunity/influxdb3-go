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
	"log"
)

func ExampleClient_Query() {
	client, err := NewFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// query
	iterator, _ := client.Query(context.Background(),
		"SELECT count(*) FROM weather WHERE time >= now() - interval '5 minutes'")

	for iterator.Next() {
		// process the result
	}

	// query with custom header
	iterator, _ = client.Query(context.Background(),
		"SELECT count(*) FROM stat WHERE time >= now() - interval '5 minutes'",
		WithHeader("X-trace-ID", "#0122"))

	for iterator.Next() {
		// process the result
	}
}

func ExampleClient_QueryWithParameters() {
	client, err := NewFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// query
	iterator, _ := client.QueryWithParameters(context.Background(),
		"SELECT count(*) FROM weather WHERE location = $location AND time >= now() - interval '5 minutes'",
		QueryParameters{
			"location": "sun-valley-1",
		})

	for iterator.Next() {
		// process the result
	}

	// query with custom header
	iterator, _ = client.QueryWithParameters(context.Background(),
		"SELECT count(*) FROM weather WHERE location = $location AND time >= now() - interval '5 minutes'",
		QueryParameters{
			"location": "sun-valley-1",
		},
		WithHeader("X-trace-ID", "#0122"))

	for iterator.Next() {
		// process the result
	}
}
