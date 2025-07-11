# Change Log

## 2.9.0 [unreleased]

### Features

1. [#169](https://github.com/InfluxCommunity/influxdb3-go/pull/169): Support user-defined type converter function for writes points.
2. [#171](https://github.com/InfluxCommunity/influxdb3-go/pull/171): Add function to get InfluxDB version.

## 2.8.0 [2025-06-18]

### Features

1. [#159](https://github.com/InfluxCommunity/influxdb3-go/pull/159): Support fast writes without waiting for WAL
   persistence:
   - New write option (`WriteOptions.NoSync`) added: `true` value means faster write but without the confirmation that
     the data was persisted. Default value: `false`.
   - **Supported by self-managed InfluxDB 3 Core and Enterprise servers only!**
   - Also configurable via connection string query parameter (`writeNoSync`).
   - Also configurable via environment variable (`INFLUX_WRITE_NO_SYNC`).
   - Long precision string values added from v3 HTTP API: `"nanosecond"`, `"microsecond"`, `"millisecond"`, `"second"` (
     in addition to the existing `"ns"`, `"us"`, `"ms"`, `"s"`).

### Bug Fixes

1. [#164](https://github.com/InfluxCommunity/influxdb3-go/pull/164): Updates
   - Go version to 1.23.9
   - `golang.org/x/net` to v0.38.0
2. [#166](https://github.com/InfluxCommunity/influxdb3-go/pull/166): Upgrades dependency `apache/arrow/go` to `apache/arrow-go` v18.

## 2.7.0 [2025-05-15]

### Bug Fixes

1. [#158](https://github.com/InfluxCommunity/influxdb3-go/pull/158): Refactor Batcher and LPBatcher:
   - Fields and methods using `capacity` renamed to `initialCapacity`.
   - Log messages when buffer data is not being emitted are simplified.
   - `SetCapacity` methods on both structures are now deprecated.
   - `WithCapacity` and `WithBufferCapacity` options are now deprecated.

## 2.6.0 [2025-04-15]

### Features

1. [#155](https://github.com/InfluxCommunity/influxdb3-go/pull/155): Batcher warnings when buffer data is not being automatically emitted changed to level `Debug` in slog.

## 2.5.0 [2025-04-10]

### Features

1. [#146](https://github.com/InfluxCommunity/influxdb3-go/pull/146): Add error field to QueryIterator to hold first possible error encountered when retrieving records from the flight Reader.
1. [#147](https://github.com/InfluxCommunity/influxdb3-go/pull/147): Ability to pass `grpc.CallOption` functions to the underlying flight Client.
1. [#149](https://github.com/InfluxCommunity/influxdb3-go/pull/149): Add default configuration for the built-in HTTP
   client and expose new configurable parameters:
   - `Timeout` - The overall time limit for requests made by the Client. A negative value means no timeout. Default
     value: 10 seconds.
   - `IdleConnectionTimeout` - Maximum time an idle connection will remain idle before closing itself. A negative value
     means no timeout. Default value: 90 seconds.
   - `MaxIdleConnections`  - Maximum number of idle connections. It sets both `transport.MaxIdleConn` and
     `transport.MaxIdleConnsPerHost` to the same value. A negative value means no limit. Default value: 100.
1. [#154](https://github.com/InfluxCommunity/influxdb3-go/pull/154): Export functions `NewQueryIterator` and
   `NewPointValueIterator` to simplify testing.

## 2.4.0 [2025-03-26]

### Features

1. [#141](https://github.com/InfluxCommunity/influxdb3-go/pull/141): Add proxy and custom SSL root certificate support.

## 2.3.0 [2025-02-20]

### Features

1. [#131](https://github.com/InfluxCommunity/influxdb3-go/pull/131): Add new PointValueIterator based on google
   guidelines [Guidelines](https://github.com/googleapis/google-cloud-go/wiki/Iterator-Guidelines)

## 2.2.0 [2025-02-03]

### Bug fixes

1. [#134](https://github.com/InfluxCommunity/influxdb3-go/pull/134): Reduce minimal Go version to 1.22, remove unnecessary toolchain constraints.

## 2.1.0 [2025-01-16]

### Bug fixes

1. [#127](https://github.com/InfluxCommunity/influxdb3-go/pull/127): LPBatcher now returns first line of the internal buffer when the line length exceeds the batch size.

## 2.0.0 [2024-12-13]

### Breaking Changes

:warning: **This is a breaking change release.**

> Previously, the Query API did not respect the metadata type for columns returned from InfluxDB v3. This release fixes this issue. As a result, the type of some columns may differ from previous versions. For example, the timestamp column will now be `time.Time` instead of `arrow.Timestamp`.

Update steps:

1. Update library: `go get github.com/InfluxCommunity/influxdb3-go/v2/influxdb3`
1. Update import path in Go files to `github.com/InfluxCommunity/influxdb3-go/v2/influxdb3`.

### Features

1. [#114](https://github.com/InfluxCommunity/influxdb3-go/pull/114): Query API respects metadata types for columns returned from InfluxDB v3.
   Tags are mapped as a "string", timestamp as "time.Time", and fields as their respective types:
   - iox::column_type::field::integer: => int64
   - iox::column_type::field::uinteger: => uint64
   - iox::column_type::field::float: => float64
   - iox::column_type::field::string: => string
   - iox::column_type::field::boolean: => bool

## 1.0.0 [2024-11-15]

:warning: **The v1.0.0 release had a malformed module path regarding the [Go Module Requirements](https://go.dev/ref/mod#major-version-suffixes). For a Go Module project, you need to use version 2 of the client.**

### Breaking Changes

:warning: **This is a breaking change release.**

> Previously, the Query API did not respect the metadata type for columns returned from InfluxDB v3. This release fixes this issue. As a result, the type of some columns may differ from previous versions. For example, the timestamp column will now be `time.Time` instead of `arrow.Timestamp`.

### Features

1. [#114](https://github.com/InfluxCommunity/influxdb3-go/pull/114): Query API respects metadata types for columns returned from InfluxDB v3.
   Tags are mapped as a "string", timestamp as "time.Time", and fields as their respective types:
    - iox::column_type::field::integer: => int64
    - iox::column_type::field::uinteger: => uint64
    - iox::column_type::field::float: => float64
    - iox::column_type::field::string: => string
    - iox::column_type::field::boolean: => bool

## 0.14.0 [2024-11-11]

### Features

1. [#112](https://github.com/InfluxCommunity/influxdb3-go/pull/112): Adds `LPBatcher` for lineprotocol batching following the model of the Point `Batcher`.

### Bug Fixes

1. [#113](https://github.com/InfluxCommunity/influxdb3-go/pull/113): Honor struct tags on WriteData, avoid panic for unexported fields

## 0.13.0 [2024-10-22]

### Features

1. [#108](https://github.com/InfluxCommunity/influxdb3-go/pull/108): Allow Request.GetBody to be set when writing gzipped data to make calls more resilient.
1. [#111](https://github.com/InfluxCommunity/influxdb3-go/pull/111): Support tabs in tag values.

## 0.12.0 [2024-10-02]

### Features

1. [#107](https://github.com/InfluxCommunity/influxdb3-go/pull/107): Add `Batcher` to simplify the process of writing data in batches.

## 0.11.0 [2024-09-27]

### Bug Fixes

1. [#105](https://github.com/InfluxCommunity/influxdb3-go/pull/105): Support newlines in tag values.
1. [#106](https://github.com/InfluxCommunity/influxdb3-go/pull/106): Close `resp.Body` after HTTP error response is encountered.

## 0.10.0 [2024-09-13]

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
