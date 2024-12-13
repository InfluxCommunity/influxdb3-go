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

package batching_test

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3/batching"
)

func Example_batcher() {
	// Create a random number generator
	r := rand.New(rand.NewSource(456))

	// Instantiate a client using your credentials.
	client, err := influxdb3.NewFromEnv()
	if err != nil {
		log.Fatal(err)
	}

	// Close the client when finished and raise any errors.
	defer client.Close()

	// Synchronous use

	// Create a Batcher with a size of 5
	b := batching.NewBatcher(batching.WithSize(5))

	// Simulate delay of a second
	t := time.Now().Add(-54 * time.Second)

	// Write 54 points synchronously to the batcher
	for range 54 {
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
				log.Fatal(err)
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
				log.Fatal(err)
			}
		}),
	)

	// Simulate delay of a second
	t = time.Now().Add(-54 * time.Second)

	// Write 54 points synchronously to the batcher
	for range 54 {
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
		log.Fatal(err)
	}
}

func Example_lineProtocol_batcher() {
	// Create a random number generator
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	// initialize data
	dataTemplate := "cpu,host=%s load=%.3f,reg=%d %d"
	syncHosts := []string{"r2d2", "c3po", "robbie"}
	const recordCount = 200

	var wErr error

	// Instantiate a client using your credentials.
	client, err := influxdb3.NewFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	defer func(client *influxdb3.Client) {
		err = client.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(client)

	// SYNCHRONOUS USAGE
	// create a new Line Protocol Batcher with a batch size of 4096 bytes
	slpb := batching.NewLPBatcher(batching.WithBufferSize(4096)) // Set buffer size

	// Simulate delay of a second
	t := time.Now().Add(-recordCount * time.Second)

	// create and emit records
	for range recordCount {
		slpb.Add(fmt.Sprintf(dataTemplate,
			syncHosts[rnd.Intn(len(syncHosts))],
			rnd.Float64()*150,
			rnd.Intn(32),
			t))

		t = t.Add(time.Second)

		if slpb.Ready() {
			wErr = client.Write(context.Background(), slpb.Emit())
			if wErr != nil {
				log.Fatal(wErr)
			}
		}
	}

	// write any remaining records in batcher to client
	wErr = client.Write(context.Background(), slpb.Emit())
	if wErr != nil {
		log.Fatal(wErr)
	}

	// ASYNCHRONOUS USAGE
	asyncHosts := []string{"Z80", "C64", "i8088"}
	// create a new Line Protocol Batcher with a batch size of 4096 bytes
	// ... a callback to handle when ready state reached and
	// ... a callback to handle emits of bytes
	alpb := batching.NewLPBatcher(batching.WithBufferSize(4096),
		batching.WithByteEmitReadyCallback(func() { fmt.Println("ready") }),
		batching.WithEmitBytesCallback(func(bytes []byte) {
			wErr := client.Write(context.Background(), bytes)
			if wErr != nil {
				log.Fatal(wErr)
			}
		}))

	// Simulate delay of a second
	t = time.Now().Add(-recordCount * time.Second)

	// create and add data to the batcher
	for range recordCount {
		alpb.Add(fmt.Sprintf(dataTemplate,
			asyncHosts[rnd.Intn(len(asyncHosts))],
			rnd.Float64()*150,
			rnd.Intn(32),
			t))

		// update time
		t = t.Add(time.Second)
	}

	// write any remaining records in batcher to client
	wErr = client.Write(context.Background(), alpb.Emit())
	if wErr != nil {
		log.Fatal(wErr)
	}
}
