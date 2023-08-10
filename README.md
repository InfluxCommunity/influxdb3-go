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

Add the latest version of the client package to your project dependencies (`go.mod`):

```sh
go get github.com/InfluxCommunity/influxdb3-go
```

## Usage

set environment variables:

- `INFLUXDB_URL` region of your influxdb cloud e.g. *`https://us-east-1-1.aws.cloud2.influxdata.com/`*
- `INFLUXDB_TOKEN` read/write token generated in cloud
- `INFLUXDB_DATABASE` name of database e.g .*`my-database`*

<details>
  <summary>linux/macos</summary>

```sh
export INFLUXDB_URL="<url>"
export INFLUXDB_DATABASE="<database>"
export INFLUXDB_TOKEN="<token>"
```

</details>

<details>
  <summary>windows</summary>

```powershell
setx INFLUXDB_URL "<url>"
setx INFLUXDB_DATABASE "<database>"
setx INFLUXDB_TOKEN "<token>"
```

</details>

To get started with influxdb client import `influxdb3-go` package.

```go
import (
  "context"
  "encoding/json"
  "fmt"
  "os"

  "github.com/InfluxCommunity/influxdb3-go/influx"
)
```

Create `influxdb3.Client` with `New` function. Make sure to `Close` client after with `defer` keyword.

```go
url := os.Getenv("INFLUXDB_URL")
token := os.Getenv("INFLUXDB_TOKEN")
database := os.Getenv("INFLUXDB_DATABASE")

// Create a new client using an InfluxDB server base URL and an authentication token
client, err := influxdb3.New(influxdb3.ClientConfig{
    Host: url,
    Token: token,
    Database: database,
})
// Close client at the end and escalate error if present
defer func (client *influxdb3.Client)  {
    err := client.Close()
    if err != nil {
        panic(err)
    }
}(client)
```

The `client` can be now used to insert data using [line-protocol](https://docs.influxdata.com/influxdb/cloud-serverless/reference/syntax/line-protocol/).

```go
line := "stat,unit=temperature avg=23.5,max=45.0"
err = client.Write(context.Background(), []byte(line))
```

Fetch data using FlightSQL query and print result.

```go
query := `
        SELECT *
        FROM "stat"
        WHERE
        time >= now() - interval '5 minute'
        AND
        "unit" IN ('temperature')
`;

iterator, err := client.Query(context.Background(), query)

if err != nil {
    panic(err)
}

for iterator.Next() {
    value := iterator.Value()

    fmt.Printf("avg is %f\n", value["avg"])
    fmt.Printf("max is %f\n", value["max"])
}
```

## Example

Prepare environment like in [Usage](#usage) and run `go run ./example/main.go`.

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
