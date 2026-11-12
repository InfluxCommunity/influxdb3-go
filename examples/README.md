# Examples

- [Write and query data](Basic/basic.go) - A complete Go example that demonstrates the different ways of writing data, and then queries your data stored in InfluxDB v3.
- [Control first-write tag column order](ColumnOrder/main.go) - A focused example showing `WithTagOrder(...)` to prioritize tag order on the first write.
- [Downsampling](Downsampling/downsampling.go) - A complete Go example that uses a downsampling query and then writes downsampled data back to a different table.
- [HTTP Error Handling](HTTPErrorHandled/httpErrorHandled.go) - A complete Go example for reading HTTP headers in case of an server error occurs.
- [Batching write](Batching/batching.go) - A complete Go example that demonstrates how to write Point data in batches.
- [Line protocol batching write](LPBatching/lpBatching.go) - A complete GO example demonstrating how to write line protocol data in batches.
- [Configure TLS and mTLS](ConfigTLS/configTls.go) - A complete Go example demonstrating how to configure custom root TLS certificates and mTLS with client certificates.
- [Pre-configured HTTP Client](WithPreConfigHttp/with_pre_configured_http_client.go) - A complete Go example demonstrating how to use a pre-configured HTTP client.
