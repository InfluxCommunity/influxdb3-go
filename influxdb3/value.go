/*
 The MIT License

 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

 The above copyright notice and this permission notice shall be included in
 all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 THE SOFTWARE.
*/

package influxdb3

import (
	"fmt"
	"time"

	"github.com/influxdata/line-protocol/v2/lineprotocol"
)

// NativeType are unions of type sets that can converted to [lineprotocol.NewValue].
//
// [lineprotocol.NewValue]: https://pkg.go.dev/github.com/influxdata/line-protocol/v2/lineprotocol#NewValue
type NativeType interface {
	float64 | int64 | uint64 | string | []byte | bool
}

// Float [Float] is IEEE-754 64-bit floating-point numbers. Default numerical type. InfluxDB supports scientific notation in float field values.
//
// [Float]: https://docs.influxdata.com/influxdb/cloud-serverless/reference/syntax/line-protocol/#float
type Float interface {
	~float32 | ~float64
}

// Integer [Integer] is signed 64-bit integers.
//
// [Integer]: https://docs.influxdata.com/influxdb/cloud-serverless/reference/syntax/line-protocol/#integer
type Integer interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64
}

// UInteger [UInteger] is unsigned 64-bit integers.
//
// [UInteger]: https://docs.influxdata.com/influxdb/cloud-serverless/reference/syntax/line-protocol/#uinteger
type UInteger interface {
	~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

// String [String] is plain text string. Length limit 64KB.
//
// [String]: https://docs.influxdata.com/influxdb/cloud-serverless/reference/syntax/line-protocol/#string
type String interface {
	~string | ~[]byte
}

// Boolean [Boolean] is true or false values.
//
// [Boolean]: https://docs.influxdata.com/influxdb/cloud-serverless/reference/syntax/line-protocol/#boolean
type Boolean interface {
	~bool
}

// NewValueFromNative is a convenient function for creating a [lineprotocol.Value] from NativeType.
//
// Parameters:
//   - v: The value of the field value.
//
// Returns:
//   - The created [lineprotocol.Value].
//
// [lineprotocol.Value]: https://pkg.go.dev/github.com/influxdata/line-protocol/v2/lineprotocol#Value
func NewValueFromNative[N NativeType](v N) lineprotocol.Value {
	return lineprotocol.MustNewValue(v)
}

// NewValueFromFloat is a convenient function for creating a [lineprotocol.Value] from Float.
// Non-finite floating-point field values (+/- infinity and NaN from IEEE 754) are not currently supported.
//
// Parameters:
//   - v: The value of the Float value.
//
// Returns:
//   - The created [lineprotocol.Value].
//
// [lineprotocol.Value]: https://pkg.go.dev/github.com/influxdata/line-protocol/v2/lineprotocol#Value
func NewValueFromFloat[F Float](v F) lineprotocol.Value {
	data, ok := lineprotocol.FloatValue(float64(v))
	if !ok {
		panic(fmt.Errorf("invalid float value for NewValueFromFloat: %T (%#v)", v, v))
	}
	return data
}

// NewValueFromInt is a convenient function for creating a [lineprotocol.Value] from Integer.
//
// Parameters:
//   - v: The value of the Integer value.
//
// Returns:
//   - The created [lineprotocol.Value].
//
// [lineprotocol.Value]: https://pkg.go.dev/github.com/influxdata/line-protocol/v2/lineprotocol#Value
func NewValueFromInt[I Integer](v I) lineprotocol.Value {
	return lineprotocol.IntValue(int64(v))
}

// NewValueFromUInt is a convenient function for creating a [lineprotocol.Value] from UInteger.
//
// Parameters:
//   - v: The value of the UInteger value.
//
// Returns:
//   - The created [lineprotocol.Value].
//
// [lineprotocol.Value]: https://pkg.go.dev/github.com/influxdata/line-protocol/v2/lineprotocol#Value
func NewValueFromUInt[U UInteger](v U) lineprotocol.Value {
	return lineprotocol.UintValue(uint64(v))
}

// NewValueFromString is a convenient function for creating a [lineprotocol.Value] from String.
// Non-UTF-8 string field values are not currently supported.
//
// Parameters:
//   - v: The value of the String value.
//
// Returns:
//   - The created [lineprotocol.Value].
//
// [lineprotocol.Value]: https://pkg.go.dev/github.com/influxdata/line-protocol/v2/lineprotocol#Value
func NewValueFromString[S String](v S) lineprotocol.Value {
	data, ok := lineprotocol.StringValue(string(v))
	if !ok {
		panic(fmt.Errorf("invalid utf-8 string value for NewValueFromString: %T (%#v)", v, v))
	}
	return data
}

// NewValueFromStringer is a convenient function for creating a [lineprotocol.Value] from [fmt.Stringer].
//
// Parameters:
//   - v: The value of the [fmt.Stringer] value.
//
// Returns:
//   - The created [lineprotocol.Value].
//
// [lineprotocol.Value]: https://pkg.go.dev/github.com/influxdata/line-protocol/v2/lineprotocol#Value
func NewValueFromStringer[S fmt.Stringer](v S) lineprotocol.Value {
	return NewValueFromString(v.String())
}

// NewValueFromBoolean is a convenient function for creating a [lineprotocol.Value] from Boolean.
//
// Parameters:
//   - v: The value of the Boolean value.
//
// Returns:
//   - The created [lineprotocol.Value].
//
// [lineprotocol.Value]: https://pkg.go.dev/github.com/influxdata/line-protocol/v2/lineprotocol#Value
func NewValueFromBoolean[B Boolean](v B) lineprotocol.Value {
	return lineprotocol.BoolValue(bool(v))
}

// NewValueFromTime is a convenient function for creating a [lineprotocol.Value] from [time.Time].
//
// Parameters:
//   - v: The value of the [time.Time] value.
//
// Returns:
//   - The created [lineprotocol.Value].
//
// [lineprotocol.Value]: https://pkg.go.dev/github.com/influxdata/line-protocol/v2/lineprotocol#Value
func NewValueFromTime(v time.Time) lineprotocol.Value {
	return NewValueFromString(v.Format(time.RFC3339Nano))
}
