package influx

import (
	"github.com/influxdata/line-protocol/v2/lineprotocol"
)

// WriteOptions holds options for write
type WriteOptions struct {
	// Precision to use in writes for timestamp.
	// Default `lineprotocol.Nanosecond`
	Precision lineprotocol.Precision

	// Write body larger than the threshold is gzipped. 0 to don't gzip at all
	GzipThreshold int
}

// DefaultWriteOptions specifies default write param
var DefaultWriteOptions = WriteOptions{
	Precision:     lineprotocol.Nanosecond,
	GzipThreshold: 1_000,
}
