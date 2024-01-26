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

# InfluxDB 3 Go Client

The go package that provides an easy and convenient way to interact with InfluxDB 3.
This package supports both writing data to InfluxDB and querying data using the FlightSQL client,
which allows you to execute SQL queries against InfluxDB IOx.

## Installation

Add the latest version of the client package to your project dependencies:

```sh
go get github.com/InfluxCommunity/influxdb3-go
```

## Usage

Client can be instantiated using
* `influxb3.ClientConfig`
* environment variables
* connection string

### Environment variables

Set environment variables:

* `INFLUX_URL` region of your influxdb cloud e.g. *`https://us-east-1-1.aws.cloud2.influxdata.com/`*
* `INFLUX_TOKEN` read/write token generated in cloud
* `INFLUX_DATABASE` name of database e.g .*`my-database`*

<details>
  <summary>linux/macos</summary>

```sh
export INFLUX_URL="<url>"
export INFLUX_TOKEN="<token>"
export INFLUX_DATABASE="<database>"
```

</details>

<details>
  <summary>windows</summary>

```powershell
setx INFLUX_URL "<url>"
setx INFLUX_TOKEN "<token>"
setx INFLUX_DATABASE "<database>"
```

</details>

To get started with influxdb client import `influxdb3-go` package.

```go
import (
  "context"
  "encoding/json"
  "fmt"
  "os"

  "github.com/InfluxCommunity/influxdb3-go/influxdb3"
)
```

Create `influxdb3.Client` with `New` function. Make sure to `Close` the client at the end.

```go
// Create a new client using INFLUX_* environment variables
client, err := influxdb3.New()

// Close client at the end and escalate an error if occurs
defer func ()  {
    err := client.Close()
    if err != nil {
        panic(err)
    }
}()
```

### Write data

The `client` can insert data using [line-protocol](https://docs.influxdata.com/influxdb/cloud-serverless/reference/syntax/line-protocol/):

```go
line := "stat,location=Paris temperature=23.5,humidity=45i"
err = client.Write(context.Background(), []byte(line))
```

The `client` can also write points

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

and/or annotated structs

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

Use FlightSQL to query and print result.

```go
query := `
    SELECT *
    FROM stat
    WHERE
        time >= now() - interval '5 minute'
        AND
        location IN ('Paris')
`

iterator, err := client.Query(context.Background(), query)
if err != nil {
    panic(err)
}

for iterator.Next() {
    value := iterator.Value()

    fmt.Printf("temperature in Paris is %f\n", value["temperature"])
    fmt.Printf("humidity in Paris is %d%%\n", value["humidity"])
}
```

Queries can be parameterized:

```go
query := `
    SELECT *
    FROM stat
    WHERE
        time >= now() - interval '5 minute'
        AND
        location = $location
`
parameters := influxdb3.QueryParameters{
    "location": "Paris",
}

iterator, err := client.QueryWithParameters(context.Background(), query, parameters)

// process result
```

## Examples

Prepare environment like in [Usage](#usage) and check ['examples'](./examples/README.md) folder.

## Feedback

If you need help, please use our [Community Slack](https://app.slack.com/huddle/TH8RGQX5Z/C02UDUPLQKA)
or [Community Page](https://community.influxdata.com/).

New features and bugs can be reported on GitHub: <https://github.com/InfluxCommunity/influxdb3-go>

## Contribution

If you would like to contribute code you can do through GitHub by forking the repository and sending a pull request into
the `main` branch.

## License

The InfluxDB 3 Go Client is released under the [MIT License](https://opensource.org/licenses/MIT).
which allows you to execute SQL queries on InfluxDB IOx.
