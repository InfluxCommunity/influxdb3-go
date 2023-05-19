package influx

import (
	"time"

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
	// Maximum time before write expires
	ExpirationTime int
	// Maximum number of points sent to server in single request, used by PointsWriter. Default 5000
	BatchSize int
	// Maximum size of batch in bytes, used by PointsWriter. Default 50_000_000.
	MaxBatchBytes int
	// Interval, in ms, used by PointsWriter, in which is buffer flushed if it has not been already written (by reaching batch size) . Default 1000ms
	FlushInterval int
	// Precision to use in writes for timestamp.
	// Default lineprotocol.Nanosecond
	Precision lineprotocol.Precision
	// Tags added to each point during writing. If a point already has a tag with the same key, it is left unchanged.
	DefaultTags map[string]string
	// Write body larger than the threshold is gzipped. 0 to don't gzip at all
	GzipThreshold int
	// WriteFailed is called to inform about an error occurred during writing procedure.
	// It can be called when point encoding fails, sending batch over network fails.
	// Params:
	//   error - write error.
	//   lines - failed batch of lines. nil in case of error occur before sending, e.g. in case of conversion error
	//   attempt - count of already failed attempts to write the lines (1 ... maxRetries+1). 0 if error occur before sending, e.g. in case of conversion error
	//   expires - expiration time for the lines to be retried in millis since epoch. 0 if error occur before sending, e.g. in case of conversion error
	WriteFailed func(err error, lines []byte, expires time.Time) bool
	// InfluxDB Enterprise write consistency as explained in https://docs.influxdata.com/enterprise_influxdb/v1.9/concepts/clustering/#write-consistency
	Consistency Consistency
}

// DefaultWriteParams specifies default write param
var DefaultWriteParams = WriteParams{
	ExpirationTime: 180_000,
	BatchSize:     5_000,
	MaxBatchBytes: 50_000_000,
	FlushInterval: 60_000,
	Precision:     lineprotocol.Nanosecond,
	GzipThreshold: 1_000,
}
