## 0.10.0 [unreleased]

### Features

1. [#100](https://github.com/InfluxCommunity/influxdb3-go/pull/100): Expose HTTP Response headers in `ServerError`

### Bug Fixes

1. [#94](https://github.com/InfluxCommunity/influxdb3-go/pull/94): Resource leak from unclosed `Response`
1. [#97](https://github.com/InfluxCommunity/influxdb3-go/pull/97): Style and performance improvements discovered by `golangci-lint`
1. [#98](https://github.com/InfluxCommunity/influxdb3-go/pull/98): Cloud Dedicated database creation ignores the name given by an argument

### CI

1. [#95](https://github.com/InfluxCommunity/influxdb3-go/pull/95): Add `golangci-lint` to CI

## 0.9.0 [2024-08-12]

### Features

1. [#87](https://github.com/InfluxCommunity/influxdb3-go/pull/87): Add Cloud Dedicated database creation support
1. [#91](https://github.com/InfluxCommunity/influxdb3-go/pull/91): Add Edge (OSS) authentication support.

### Bug Fixes

1. [#89](https://github.com/InfluxCommunity/influxdb3-go/pull/89): InfluxDB Edge (OSS) error handling

## 0.8.0 [2024-06-24]

### Features

1. [#85](https://github.com/InfluxCommunity/influxdb3-go/pull/85): Add standard `user-agent` header to gRPC requests.
1. [#86](https://github.com/InfluxCommunity/influxdb3-go/pull/86): Add Serverless bucket creation support

## 0.7.0 [2024-04-16]

### Features

1. [#74](https://github.com/InfluxCommunity/influxdb3-go/pull/74): Use `log/slog` to print debug information instead of `fmt.Printf`
1. [#76](https://github.com/InfluxCommunity/influxdb3-go/pull/76): Add custom headers support for queries (gRPC requests)

### Bug Fixes

1. [#71](https://github.com/InfluxCommunity/influxdb3-go/pull/71): Rename `FlightSQL` constant to `SQL`

### Others

1. [#68](https://github.com/InfluxCommunity/influxdb3-go/pull/68): Upgrade Go version to 1.22.

## 0.6.0 [2024-03-01]

### Features

1. [#56](https://github.com/InfluxCommunity/influxdb3-go/pull/56): Add support for named query parameters

### Bug Fixes

1. [#59](https://github.com/InfluxCommunity/influxdb3-go/pull/59): Export Default Tags from package

## 0.5.0 [2023-12-05]

### Features

1. [#50](https://github.com/InfluxCommunity/influxdb3-go/pull/50): Default Tags for Writes

## 0.4.0 [2023-11-03]

### Features

1. [#45](https://github.com/InfluxCommunity/influxdb3-go/pull/45): Add structured query support

### Docs

1. [#45](https://github.com/InfluxCommunity/influxdb3-go/pull/45): Add downsampling example

## 0.3.0 [2023-10-02]

### Features

1. [#36](https://github.com/InfluxCommunity/influxdb3-go/pull/36): Add client creation from connection string
and environment variables.

### Bug Fixes

1. [#37](https://github.com/InfluxCommunity/influxdb3-go/pull/37): `runtime error` for iterating Arrow Record without rows

## 0.2.0 [2023-08-11]

### Features

1. [#30](https://github.com/InfluxCommunity/influxdb3-go/pull/30): Add custom HTTP headers support

### Breaking Changes

1. [#31](https://github.com/InfluxCommunity/influxdb3-go/pull/31): Changed package to `influxdb3`.
Renamed config types and some options.

## 0.1.0 [2023-06-09]

- initial release of new client version
- write using v2 api
- query using SQL
- query using influxQL
