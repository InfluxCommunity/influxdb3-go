<p align="center">
    <img src="gopher.png" alt="Gopher" width="150px">
</p>
<p align="center">
    <a href="https://pkg.go.dev/github.com/bonitoo-io/influxdb3-go">
        <img src="https://pkg.go.dev/badge/github.com/bonitoo-io/influxdb3-go.svg" alt="Go Reference">
    </a>
    <a href="https://goreportcard.com/report/github.com/bonitoo-io/influxdb3-go">
        <img src="https://goreportcard.com/badge/github.com/bonitoo-io/influxdb3-go" alt="Go Report Card">
    </a>
    <a href="https://github.com/bonitoo-io/influxdb3-go/actions/workflows/codeql-analysis.yml">
        <img src="https://github.com/bonitoo-io/influxdb3-go/actions/workflows/codeql-analysis.yml/badge.svg?branch=main" alt="CodeQL analysis">
    </a>
    <a href="https://github.com/bonitoo-io/influxdb3-go/actions/workflows/linter.yml">
        <img src="https://github.com/bonitoo-io/influxdb3-go/actions/workflows/linter.yml/badge.svg" alt="Lint Code Base">
    </a>
    <a href="https://dl.circleci.com/status-badge/redirect/gh/bonitoo-io/influxdb3-go/tree/main">
        <img src="https://dl.circleci.com/status-badge/img/gh/bonitoo-io/influxdb3-go/tree/main.svg?style=svg" alt="CircleCI">
    </a>
    <a href="https://codecov.io/gh/bonitoo-io/influxdb3-go">
        <img src="https://codecov.io/gh/bonitoo-io/influxdb3-go/branch/main/graph/badge.svg" alt="Code Cov"/>
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
go get github.com/bonitoo-io/influxdb3-go
```

## Usage

To start with the client, import the `influx` package and create a `influx.Client` by the `NewClient` function:

```go
import (
    "github.com/bonitoo-io/influxdb3-go/influx"
    "github.com/bonitoo-io/influxdb3-go/influx/configs"
)

host := "https://eu-central-1-1.aws.cloud2.influxdata.com/"
database := "my-database"
token := "my-token"

client, err := NewClient(configs.ClientConfigs{
    Host:     &host,
    Database: &database,
    Token:    &token},
)
```

to insert data, you can use code like this:

```go
// TBD
```

to query your data, you can use code like this:

```go
// TBD
```

## Feedback

If you need help, please use our [Community Slack](https://app.slack.com/huddle/TH8RGQX5Z/C02UDUPLQKA)
or [Community Page](https://community.influxdata.com/).

New features and bugs can be reported on GitHub: <https://github.com/bonitoo-io/influxdb3-go>

## Contribution

If you would like to contribute code you can do through GitHub by forking the repository and sending a pull request into
the `main` branch.

## License

The InfluxDB 3 Go Client is released under the [MIT License](https://opensource.org/licenses/MIT).
which allows you to execute SQL queries on InfluxDB IOx.

### Basic Example

set environment values : `INFLUXDB_REGION` `INFLUXDB_TOKEN` `INFLUXDB_BUCKET`

run `go run ./example/main.go`

