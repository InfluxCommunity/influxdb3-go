<p align="center">
    <a href="https://pkg.go.dev/github.com/bonitoo-io/influxdb-client-go-3">
        <img src="https://pkg.go.dev/badge/github.com/bonitoo-io/influxdb-client-go-3.svg" alt="Go Reference">
    </a>
    <a href="https://goreportcard.com/report/github.com/bonitoo-io/influxdb-client-go-3">
        <img src="https://goreportcard.com/badge/github.com/bonitoo-io/influxdb-client-go-3" alt="Go Report Card">
    </a>
    <a href="https://github.com/bonitoo-io/influxdb-client-go-3/actions/workflows/codeql-analysis.yml">
        <img src="https://github.com/bonitoo-io/influxdb-client-go-3/actions/workflows/codeql-analysis.yml/badge.svg?branch=main" alt="CodeQL analysis">
    </a>
    <a href="https://github.com/bonitoo-io/influxdb-client-go-3/actions/workflows/linter.yml">
        <img src="https://github.com/bonitoo-io/influxdb-client-go-3/actions/workflows/linter.yml/badge.svg" alt="Lint Code Base">
    </a>
    <a href="https://app.slack.com/huddle/TH8RGQX5Z/C02UDUPLQKA">
        <img src="https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social" alt="Community Slack">
    </a>
</p>

# InfluxDB 3 Go Client

The go package that provides a simple and convenient way to interact with InfluxDB 3.
This package supports both writing data to InfluxDB and querying data using the FlightSQL client,
which allows you to execute SQL queries on InfluxDB IOx.

## Installation

Add the latest version of the client package to your project dependencies (`go.mod`).

```sh
go get github.com/bonitoo-io/influxdb-client-go-3
```

## Usage

To start with the client, import the `influx` package and create a `influx.Client` by the `NewClient` function:

```go
import (
"github.com/bonitoo-io/influxdb-client-go-3/influx"
)

client, err := influx.NewClient(influx.configs.ClientConfigs{
Host:       "https://eu-central-1-1.aws.cloud2.influxdata.com/",
Database:   "my-database",
Token:      "my-token",
})

```

to insert data you can use code like this:

```go
// TBD
```

to query your data you can use code like this:

```go
// TBD
```

## Feedback

For help please use, please use our [Community Slack](https://app.slack.com/huddle/TH8RGQX5Z/C02UDUPLQKA) or [Community Page](https://community.influxdata.com/).

New features and bugs can be reported on GitHub: https://github.com/bonitoo-io/influxdb-client-go-3

## Contribution

If you would like to contribute code you can do through GitHub by forking the repository and sending a pull request into the `main` branch.

## License

The InfluxDB 3 Go Client is released under the [MIT License](https://opensource.org/licenses/MIT).