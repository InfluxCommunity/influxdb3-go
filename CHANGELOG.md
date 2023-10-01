## 0.4.0 [unreleased]

### Features

1. [#42](https://github.com/InfluxCommunity/influxdb3-go/pull/42): Add type-safe conversion for line-protocol

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
- query using flightSQL
- query using influxQL
