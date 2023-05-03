// Modified generated code.
package model

import "time"

// Error defines model for Error.
type Error struct {
	// code is the machine-readable error code.
	Code ErrorCode `json:"code"`

	// Stack of errors that occurred during processing of the request. Useful for debugging.
	Err *string `json:"err,omitempty"`

	// Human-readable message.
	Message *string `json:"message,omitempty"`

	// Describes the logical code operation when the error occurred. Useful for debugging.
	Op *string `json:"op,omitempty"`
}

// code is the machine-readable error code.
type ErrorCode string



// Defines values for ErrorCode.
const (
	ErrorCodeConflict ErrorCode = "conflict"

	ErrorCodeEmptyValue ErrorCode = "empty value"

	ErrorCodeForbidden ErrorCode = "forbidden"

	ErrorCodeInternalError ErrorCode = "internal error"

	ErrorCodeInvalid ErrorCode = "invalid"

	ErrorCodeMethodNotAllowed ErrorCode = "method not allowed"

	ErrorCodeNotFound ErrorCode = "not found"

	ErrorCodeRequestTooLarge ErrorCode = "request too large"

	ErrorCodeTooManyRequests ErrorCode = "too many requests"

	ErrorCodeUnauthorized ErrorCode = "unauthorized"

	ErrorCodeUnavailable ErrorCode = "unavailable"

	ErrorCodeUnprocessableEntity ErrorCode = "unprocessable entity"

	ErrorCodeUnsupportedMediaType ErrorCode = "unsupported media type"
)

// PostDeleteParams defines parameters for PostDelete.
type PostDeleteParams struct {
	// The organization to delete data from.
	// If you pass both `orgID` and `org`, they must both be valid.
	//
	// #### InfluxDB Cloud
	//
	// - Doesn't require `org` or `orgID`.
	// - Deletes data from the bucket in the organization associated with the authorization (API token).
	//
	// #### InfluxDB OSS
	//
	// - Requires either `org` or `orgID`.
	Org *string `json:"org,omitempty"`

	// The name or ID of the bucket to delete data from.
	// If you pass both `bucket` and `bucketID`, `bucketID` takes precedence.
	Bucket *string `json:"bucket,omitempty"`

	// The ID of the organization to delete data from.
	// If you pass both `orgID` and `org`, they must both be valid.
	//
	// #### InfluxDB Cloud
	//
	// - Doesn't require `org` or `orgID`.
	// - Deletes data from the bucket in the organization associated with the authorization (API token).
	//
	// #### InfluxDB OSS
	//
	// - Requires either `org` or `orgID`.
	OrgID *string `json:"orgID,omitempty"`

	// The ID of the bucket to delete data from.
	// If you pass both `bucket` and `bucketID`, `bucketID` takes precedence.
	BucketID *string `json:"bucketID,omitempty"`

	// OpenTracing span context
	ZapTraceSpan *string `json:"Zap-Trace-Span,omitempty"`
}

// PostDeleteAllParams defines type for all parameters for PostDelete.
type PostDeleteAllParams struct {
	PostDeleteParams

	Body PostDeleteJSONRequestBody
}


// Ready defines model for Ready.
type Ready struct {
	Started *time.Time   `json:"started,omitempty"`
	Status  *ReadyStatus `json:"status,omitempty"`
	Up      *string      `json:"up,omitempty"`
}

// ReadyStatus defines model for Ready.Status.
type ReadyStatus string

// GetHealthParams defines parameters for GetHealth.
type GetHealthParams struct {
	// OpenTracing span context
	ZapTraceSpan *string `json:"Zap-Trace-Span,omitempty"`
}

// HealthCheck defines model for HealthCheck.
type HealthCheck struct {
	Checks  *[]HealthCheck    `json:"checks,omitempty"`
	Commit  *string           `json:"commit,omitempty"`
	Message *string           `json:"message,omitempty"`
	Name    string            `json:"name"`
	Status  HealthCheckStatus `json:"status"`
	Version *string           `json:"version,omitempty"`
}

// HealthCheckStatus defines model for HealthCheck.Status.
type HealthCheckStatus string

// GetReadyParams defines parameters for GetReady.
type GetReadyParams struct {
	// OpenTracing span context
	ZapTraceSpan *string `json:"Zap-Trace-Span,omitempty"`
}


// GetFlagsParams defines parameters for GetFlags.
type GetFlagsParams struct {
	// OpenTracing span context
	ZapTraceSpan *string `json:"Zap-Trace-Span,omitempty"`
}

// PostDeleteJSONRequestBody defines body for PostDelete for application/json ContentType.
type PostDeleteJSONRequestBody PostDeleteJSONBody

// PostDeleteJSONBody defines parameters for PostDelete.
type PostDeleteJSONBody DeletePredicateRequest


// The delete predicate request.
type DeletePredicateRequest struct {
	// An expression in [delete predicate syntax](https://docs.influxdata.com/influxdb/v2.3/reference/syntax/delete-predicate/).
	Predicate *string `json:"predicate,omitempty"`

	// A timestamp ([RFC3339 date/time format](https://docs.influxdata.com/influxdb/v2.3/reference/glossary/#rfc3339-timestamp)).
	// The earliest time to delete from.
	Start time.Time `json:"start"`

	// A timestamp ([RFC3339 date/time format](https://docs.influxdata.com/influxdb/v2.3/reference/glossary/#rfc3339-timestamp)).
	// The latest time to delete from.
	Stop time.Time `json:"stop"`
}
