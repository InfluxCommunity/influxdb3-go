package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"text/tabwriter"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/influxdb3"
	"github.com/InfluxCommunity/influxdb3-go/influxdb3/batching"
	"github.com/apache/arrow/go/v15/arrow"
)

const LineCount = 100

func main() {
	// PREPARE DATA
	// Create a random number generator
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Retrieve credentials from environment variables.
	url := os.Getenv("INFLUX_URL")
	token := os.Getenv("INFLUX_TOKEN")
	database := os.Getenv("INFLUX_DATABASE")

	// unmanned aquatic surface vehicle as source
	dataTemplate := "uasv,id=%s,location=%s speed=%f,bearing=%f,ticks=%di %d"
	ids := []string{"orca", "flipper", "gaston"}
	syncLocations := []string{"nice", "split", "goa", "cancun"}

	// Instantiate a client using your credentials.
	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     url,
		Token:    token,
		Database: database,
	})

	if err != nil {
		panic(err)
	}

	defer func(client *influxdb3.Client) {
		err := client.Close()
		if err != nil {
			slog.Error("Error closing client", err)
		}
	}(client)

	// SYNC WRITE BATCHES

	// create a new Line Protocol Batcher
	syncLpb := batching.NewLPBatcher(batching.WithBufferSize(4096)) // Set buffer size
	t := time.Now().Add(-LineCount * time.Second)

	// add new data into the batcher
	for range LineCount {
		syncLpb.Add(fmt.Sprintf(dataTemplate,
			ids[rnd.Intn(len(ids))],
			syncLocations[rnd.Intn(len(syncLocations))],
			rnd.Float64()*100,
			rnd.Float64()*360,
			rnd.Intn(100),
			t.UnixNano(),
		))
		t = t.Add(time.Second)

		// if ready state reached, emit a batch
		if syncLpb.Ready() {
			err = client.Write(context.Background(), syncLpb.Emit())
			if err != nil {
				slog.Error(err.Error())
			}
		}
	}

	// Write final batch to client
	err = client.Write(context.Background(), syncLpb.Emit())
	if err != nil {
		slog.Error(err.Error())
	}
	fmt.Printf("Sync Writes Done.  %d Bytes remaining in batcher buffer\n",
		syncLpb.CurrentLoadSize())

	//prepare SQL query
	queryTemplate := "SELECT * FROM \"uasv\"" +
		"\nWHERE time >= now() - interval '2 minutes'\n" +
		"AND location IN ('%s','%s')\norder by time"

	fmt.Println("Show selected values written by sync")
	DumpQueryResults(client.Query(context.Background(),
		fmt.Sprintf(queryTemplate, syncLocations[2], syncLocations[3])))

	fmt.Println()

	// ASYNC WRITE BATCHES
	asyncLpb := batching.NewLPBatcher(batching.WithBufferSize(4096), // Set buffer size
		batching.WithByteEmitReadyCallback(func() { fmt.Println("|-- ready to emit -->") }), // Set ready callback
		batching.WithEmitBytesCallback(func(bytes []byte) { // Set Callback to handle emitted bytes
			err = client.Write(context.Background(), bytes)
			if err != nil {
				slog.Error(err.Error())
			}
		}))

	asyncLocations := []string{"ibiza", "dubai", "phuket", "maui"}
	t = time.Now().Add(-LineCount * time.Second)

	// Add new data to Batcher
	for range LineCount {
		asyncLpb.Add(fmt.Sprintf(dataTemplate, ids[rnd.Intn(len(ids))],
			asyncLocations[rnd.Intn(len(asyncLocations))],
			rnd.Float64()*100,
			rnd.Float64()*360,
			rnd.Intn(100),
			t.UnixNano()))
		t = t.Add(time.Second)
	}

	// Write the remaining batch records to the client
	err = client.Write(context.Background(), asyncLpb.Emit())
	if err != nil {
		slog.Error(err.Error())
	}
	fmt.Printf("Async Writes Done.  %d Bytes remaining in batcher buffer\n",
		asyncLpb.CurrentLoadSize())

	fmt.Println("Show selected values written by async")
	DumpQueryResults(client.Query(context.Background(),
		fmt.Sprintf(queryTemplate, asyncLocations[0], asyncLocations[1])))

}

func DumpQueryResults(qiter *influxdb3.QueryIterator, err error) {
	if err != nil {
		slog.Error(err.Error())
	}
	tw := tabwriter.NewWriter(os.Stdout, 1, 1, 1, ' ', 0)
	defer func(tw *tabwriter.Writer) {
		err := tw.Flush()
		if err != nil {

		}
	}(tw)

	fmt.Fprintln(tw, "\nTime\tid\tlocation\tspeed\tbearing\tticks")
	for qiter.Next() {
		value := qiter.Value()
		t := (value["time"].(arrow.Timestamp)).ToTime(arrow.Nanosecond).Format(time.RFC3339)
		_, werr := fmt.Fprintf(tw, "%v\t%s\t%s\t%.1f\t%.2f\t%d\n", t,
			value["id"], value["location"], value["speed"], value["bearing"], value["ticks"])
		if werr != nil {
			slog.Error(werr.Error())
		}
	}
}
