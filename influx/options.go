package influx

import (
	"github.com/influxdata/line-protocol/v2/lineprotocol"
)

const (
	// ConsistencyOne requires at least one data node acknowledged a write.
	ConsistencyOne Consistency = "one"

	// ConsistencyAll requires all data nodes to acknowledge a write.
	ConsistencyAll Consistency = "all"

	// ConsistencyQuorum requires a quorum of data nodes to acknowledge a write.
	ConsistencyQuorum Consistency = "quorum"

	// ConsistencyAny allows for hinted hand off, potentially no write happened yet.
	ConsistencyAny Consistency = "any"
)

// Consistency defines enum for allows consistency values for InfluxDB Enterprise, as explained  https://docs.influxdata.com/enterprise_influxdb/v1.9/concepts/clustering/#write-consistency
type Consistency string

// WriteParams holds configuration properties for write
type WriteParams struct {
	// Precision to use in writes for timestamp.
	// Default lineprotocol.Nanosecond
	Precision lineprotocol.Precision
	// Write body larger than the threshold is gzipped. 0 to don't gzip at all
	GzipThreshold int
	// InfluxDB Enterprise write consistency as explained in https://docs.influxdata.com/enterprise_influxdb/v1.9/concepts/clustering/#write-consistency
	Consistency Consistency
}

// DefaultWriteParams specifies default write param
var DefaultWriteParams = WriteParams{
	Precision:     lineprotocol.Nanosecond,
	GzipThreshold: 1_000,
}
