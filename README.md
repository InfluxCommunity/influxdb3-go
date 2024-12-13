<p align="center">
    <img src="gopher.png" alt="Gopher" width="150px">
</p>
<p align="center">
    <a href="https://pkg.go.dev/github.com/InfluxCommunity/influxdb3-go">
        <img src="https://pkg.go.dev/badge/github.com/InfluxCommunity/influxdb3-go.svg" alt="Go Reference">
    </a>
    <a href="https://goreportcard.com/report/github.com/InfluxCommunity/influxdb3-go">
        <img src="https://goreportcard.com/badge/github.com/InfluxCommunity/influxdb3-go" alt="Go Report Card">
    </a>
    <a href="https://github.com/InfluxCommunity/influxdb3-go/actions/workflows/codeql-analysis.yml">
        <img src="https://github.com/InfluxCommunity/influxdb3-go/actions/workflows/codeql-analysis.yml/badge.svg?branch=main" alt="CodeQL analysis">
    </a>
    <a href="https://github.com/InfluxCommunity/influxdb3-go/actions/workflows/linter.yml">
        <img src="https://github.com/InfluxCommunity/influxdb3-go/actions/workflows/linter.yml/badge.svg" alt="Lint Code Base">
    </a>
    <a href="https://dl.circleci.com/status-badge/redirect/gh/InfluxCommunity/influxdb3-go/tree/main">
        <img src="https://dl.circleci.com/status-badge/img/gh/InfluxCommunity/influxdb3-go/tree/main.svg?style=svg" alt="CircleCI">
    </a>
    <a href="https://codecov.io/gh/InfluxCommunity/influxdb3-go">
        <img src="https://codecov.io/gh/InfluxCommunity/influxdb3-go/branch/main/graph/badge.svg" alt="Code Cov"/>
    </a>
    <a href="https://app.slack.com/huddle/TH8RGQX5Z/C02UDUPLQKA">
        <img src="https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social" alt="Community Slack">
    </a>
</p>

# InfluxDB v3 Go client

The `influxdb3` Go package provides an easy and convenient way to interact with [InfluxDB v3](https://www.influxdata.com/get-influxdb/), the time series data platform designed to handle high write and query workloads.

Use this package to write and query data stored in your InfluxDB v3 instance.
Query using SQL or InfluxQL and retrieve data in [Arrow Columnar Format](https://arrow.apache.org/docs/format/Columnar.html#format-ipc) using InfluxDB's native Flight RPC API.

We also offer this [Getting Started: InfluxDB 3.0 Go Client Library](https://www.youtube.com/watch?v=yr6a5U_ZkY8) video that you can use to learn more about the library and see code examples.

## Prerequisites

### InfluxDB v3 credentials

To use this client, you'll need the following credentials for writing and querying data in an InfluxDB v3 instance.

- your InfluxDB instance **Host URL**--for example, your Cloud Serverless region URL `https://us-east-1-1.aws.cloud2.influxdata.com/`.
- the name of the **database or Cloud Serverless bucket** where you want to write or query data.
- your **database token** or **Cloud Serverless API token** (generated by your InfluxDB instance) with read/write permission to the specified database.

## Install the package

### In a Go module

1. In your terminal, create a module for your project--for example:

   ```sh
   go mod init iot-starter && cd $_
   ```

2. Install the latest version of the InfluxDB client:

<!--pytest-codeblocks:cont-->

   ```sh
   go get github.com/InfluxCommunity/influxdb3-go/v2
   ```

### Outside a module (standalone)

```sh
go install github.com/InfluxCommunity/influxdb3-go@latest
```

## Usage

In a Go file, import the `influxdb3` package to use it in your code--for example:

```go
import (
  "context"
  "encoding/json"
  "fmt"
  "os"

  "github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
)
```

### Instantiate a client with InfluxDB credentials

`influxdb3.Client` is the entrypoint for writing or querying data in InfluxDB.

Choose one of the following ways to provide your InfluxDB credentials (URL, token, and database) and instantiate a client:

- [Instantiate using a configuration object](#instantiate-using-a-configuration-object)
- [Instantiate using a connection string](#instantiate-using-a-connection-string)
- [Instantiate using environment variables](#instantiate-using-environment-variables)

### Instantiate using a configuration object

Call the `influxdb3.New(config influxdb3.ClientConfig)` function with a `ClientConfig` struct that contains your [credentials](#influxdb-v3-credentials)--for example:

   ```go
   // Instantiate the client.
   client, err := influxdb3.New(influxdb3.ClientConfig{
       Host:     "https://cluster.influxdata.io/",
       Token:    "DATABASE_TOKEN",
       Database: "DATABASE_NAME",
   })
   ```

Replace the following with your own [credentials](#influxdb-v3-credentials):

- `https://cluster.influxdata.io/`: your InfluxDB host URL
- `DATABASE_TOKEN`: your InfluxDB database token or API token with read/write permission
- `DATABASE_NAME`: the name of your InfluxDB database or bucket
  Alternatively, you can use `WriteOptions` or `QueryOptions` to specify the database name.

### Instantiate using a connection string

Call the `influxdb3.NewFromConnectionString(connectionString string)` function with a connection string that contains your credentials in URL format--for example:

```go
// Instantiate the client.
client, err := influxdb3.NewFromConnectionString(
       "https://cluster.influxdata.io/?token=DATABASE_TOKEN&database=DATABASE_NAME"
)
```

Replace the following with your own [credentials](#influxdb-v3-credentials):

- `https://cluster.influxdata.io/`: your InfluxDB host URL
- `DATABASE_TOKEN`: your InfluxDB database token or API token with read/write permission
- `DATABASE_NAME`: the name of your InfluxDB database or bucket
  Alternatively, you can use `WriteOptions` or `QueryOptions` to specify the database name.

### Instantiate using environment variables

1. Set the following environment variables to store your InfluxDB credentials:

   <details>
     <summary>linux/macos</summary>

   ```sh
   export INFLUX_URL="https://cluster.influxdata.io/"
   export INFLUX_TOKEN="DATABASE_TOKEN"
   export INFLUX_DATABASE="DATABASE_NAME"
   ```

   </details>

   <details>
     <summary>windows</summary>

   ```powershell
   setx INFLUX_URL "https://cluster.influxdata.io/"
   setx INFLUX_TOKEN "DATABASE_TOKEN"
   setx INFLUX_DATABASE "DATABASE_NAME"
   ```

   </details>

   Replace the following with your own [credentials](#influxdb-v3-credentials):

   - `https://cluster.influxdata.io/`: your InfluxDB host URL
   - `DATABASE_TOKEN`: your InfluxDB database token or API token with read/write permission
   - `DATABASE_NAME`: the name of your InfluxDB database or bucket
     Alternatively, you can use `WriteOptions` or `QueryOptions` to specify the database name.

2. Call `influxdb3.Client.NewFromEnv()` to instantiate a client using the environment variables.

   ```go
   // Create a new client using INFLUX_* environment variables.
   client, err := influxdb3.NewFromEnv()
   ```

### Close the client

In your code, make sure to call `client.Close()` when you have finished using the client.
You can use Go's `defer` to close the client and raise any errors before your function exits--for example:

```go
// Create a new client using INFLUX_* environment variables.
client, err := influxdb3.NewFromEnv()

// Close the client when finished.
// Go calls the `defer` function before exiting.
defer func ()  {
    err := client.Close()
    if err != nil {
        panic(err)
    }
}()
```

### Write data

You can provide data to the `influxdb3` as [line protocol](https://docs.influxdata.com/influxdb/cloud-serverless/reference/syntax/line-protocol/), a `Point`, or a `struct`.
InfluxDB clients write (insert) all data as line protocol to InfluxDB.

#### Using line protocol

You can provide data as "raw" line protocol--for example:

```go
line := "stat,location=Paris temperature=23.5,humidity=45i"
err = client.Write(context.Background(), []byte(line))
```

#### Using a `Point`

You can build data as a `Point` and let `influxdb3` convert it to line protocol--for example:

```go
p1 := influxdb3.Point{
    influxdb3.NewPoint("stat",
        map[string]string{
            "location": "Paris",
        },
        map[string]any{
            "temperature": 24.5,
            "humidity":    40,
        },
        time.Now(),
    ),
}
points := []*influxdb3.Point{p1}
err = client.WritePoints(context.Background(), points)
```

#### Using an annotated struct

You can build data as a `struct` and let `influxdb3` convert it to line protocol--for example:

```go
s1 := struct {
    Measurement string    `lp:"measurement"`
    Sensor      string    `lp:"tag,location"`
    Temp        float64   `lp:"field,temperature"`
    Hum         int       `lp:"field,humidity"`
    Time        time.Time `lp:"timestamp"`
    Description string    `lp:"-"`
}{
    "stat",
    "Paris",
    23.5,
    55,
    time.Now(),
    "Paris weather conditions",
}
data := []any{s1}
err = client.WriteData(context.Background(), data)
```

### Query

Use SQL or InfluxQL to query an InfluxDB v3 database or Cloud Serverless bucket to retrieve data.
The client can return query results in the following formats: structured `PointValues` object, key-value pairs, or Arrow Columnar Format.

By default, the client sends the query as SQL.

`influxdb3` provides an iterator for processing data rows--for example:

```go
// Query using SQL.
query := `
    SELECT *
    FROM stat
    WHERE
        time >= now() - interval '5 minutes'
        AND
        location IN ('Paris')
`

iterator, err := client.Query(context.Background(), query)

if err != nil {
    panic(err)
}

// Process the result.
for iterator.Next() {
    // The query iterator returns each row as a map[string]interface{}.
    // The keys are the column names, allowing you to access the values by column name.
    value := iterator.Value()

    fmt.Printf("temperature in Paris is %f\n", value["temperature"])
    fmt.Printf("humidity in Paris is %d%%\n", value["humidity"])
}
```

To query with InfluxQL, call the `Query()` function and specify the `influxdb3.WithQueryType(influxdb3.InfluxQL)` option--for example:

```go
// Query using InfluxQL.
query := `
    SELECT *
    FROM stat
    WHERE
        time >= now() - 5m
        AND
        location IN ('Paris')
`

// Specify the InfluxQL QueryType in options.
iterator, err := client.Query(context.Background(), query, influxdb3.WithQueryType(influxdb3.InfluxQL))

if err != nil {
    panic(err)
}

// Process the result.
```

To use parameterized queries with SQL or InfluxQL,
call the `QueryWithParameters()` function and pass the query text and a `QueryParameters` struct that defines parameter name-value pairs.

### Parameterized query with SQL

```go
// Specify $parameter placeholders in WHERE predicate expressions.
query := `
    SELECT *
    FROM stat
    WHERE
        time >= now() - interval '5 minutes'
        AND
        location = $location
`

// Assign parameter names to values.
parameters := influxdb3.QueryParameters{
    "location": "Paris",
}

iterator, err := client.QueryWithParameters(context.Background(), query, parameters)

// Process the result.
```

#### Parameterized query with InfluxQL

When using InfluxQL, pass the `influxdb3.WithQueryType(influxdb3.InfluxQL)` option.

```go
// Specify $parameter placeholders in WHERE predicate expressions.
query := `
    SELECT *
    FROM stat
    WHERE
        time >= now() - '5m'
        AND
        location = $location
`

// Assign parameter names to values.
parameters := influxdb3.QueryParameters{
    "location": "Paris",
}

// Specify the query type for an InfluxQL query.
iterator, err := client.QueryWithParameters(context.Background(), query, parameters,
 influxdb3.WithQueryType(influxdb3.InfluxQL))

// Process the result.
```

For more information, see the [InfluxDB documentation](https://docs.influxdata.com/).

## Run examples

See the [`examples` folder](./examples/README.md) for complete code examples that you can run.

To run the examples, do the following:

1. Follow instructions to [Install in a Go module](#install-in-a-go-module).
2. Clone this repository.
3. Change to the [`examples`](./examples/README.md) folder.
4. [Set environment variables](#instantiate-using-environment-variables) or edit the example file to [instantiate a client with your credentials](#instantiate-a-client-with-influxdb-credentials).
5. Run the Go file--for example, in your terminal:

   ```sh
   go run ./IOx/iox.go
   ```

## Feedback

For help, please use our [Community Slack](https://app.slack.com/huddle/TH8RGQX5Z/C02UDUPLQKA)
or [Community Page](https://community.influxdata.com/).

Submit bugs or issues to the repository on GitHub: <https://github.com/InfluxCommunity/influxdb3-go>

## Contribution

To contribute to this project, fork the repository on GitHub and send a pull request to the `main` branch.

## License

The InfluxDB v3 Go Client is released under the [MIT License](https://opensource.org/licenses/MIT)
