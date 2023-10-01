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
	"sort"
	"time"

	"github.com/influxdata/line-protocol/v2/lineprotocol"
)

// Tag holds the keys and values for a bunch of Tag k/v pairs.
type Tag struct {
	Key   string
	Value string
}

// Field holds the keys and values for a bunch of Metric Field k/v pairs where Value can be a uint64, int64, int, float32, float64, string, or bool.
type Field struct {
	Key   string
	Value lineprotocol.Value
}

// Point is represents InfluxDB time series point, holding tags and fields
type Point struct {
	Measurement string
	Tags        []Tag
	Fields      []Field
	Timestamp   time.Time
}

// NewPointWithMeasurement is a convenient function for creating a Point with the given measurement name for later adding data.
//
// Parameters:
//   - measurement: The measurement name for the Point.
//
// Returns:
//   - The created Point.
func NewPointWithMeasurement(measurement string) *Point {
	return &Point{
		Measurement: measurement,
	}
}

// NewPoint is a convenient function for creating a Point with the given measurement name, tags, fields, and timestamp.
//
// Parameters:
//   - measurement: The measurement name for the Point.
//   - tags: The tags for the Point.
//   - fields: The fields for the Point.
//   - ts: The timestamp for the Point.
//
// Returns:
//   - The created Point.
func NewPoint(measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time) *Point {
	m := &Point{
		Measurement: measurement,
		Timestamp:   ts,
	}
	if len(tags) > 0 {
		m.Tags = make([]Tag, 0, len(tags))
		for k, v := range tags {
			m.AddTag(k, v)
		}
		m.SortTags()
	}
	if len(fields) > 0 {
		m.Fields = make([]Field, 0, len(fields))
		for k, v := range fields {
			m.AddField(k, v)
		}
		m.SortFields()
	}
	return m
}

// SortTags orders the tags of a Point alphanumerically by key.
// This function is a helper to keep the tags sorted when creating a Point manually.
//
// Returns:
//   - The updated Point with sorted tags.
func (m *Point) SortTags() *Point {
	sort.Slice(m.Tags, func(i, j int) bool { return m.Tags[i].Key < m.Tags[j].Key })
	return m
}

// SortFields orders the fields of a Point alphanumerically by key.
// This function is a helper to keep the fields sorted when creating a Point manually.
//
// Returns:
//   - The updated Point with sorted fields.
func (m *Point) SortFields() *Point {
	sort.Slice(m.Fields, func(i, j int) bool { return m.Fields[i].Key < m.Fields[j].Key })
	return m
}

// AddTag adds a tag to the Point.
//
// Parameters:
//   - k: The key of the tag.
//   - v: The value of the tag.
//
// Returns:
//   - The updated Point with the tag added.
func (m *Point) AddTag(k, v string) *Point {
	for i, tag := range m.Tags {
		if k == tag.Key {
			m.Tags[i].Value = v
			return m
		}
	}
	m.Tags = append(m.Tags, Tag{Key: k, Value: v})
	return m
}

// AddField adds a field to the Point.
//
// Parameters:
//   - k: The key of the field.
//   - v: The value of the field.
//
// Returns:
//   - The updated Point with the field added.
func (m *Point) AddField(k string, v interface{}) *Point {
	val, _ := lineprotocol.NewValue(convertField(v))
	for i, field := range m.Fields {
		if k == field.Key {
			m.Fields[i].Value = val
			return m
		}
	}

	m.Fields = append(m.Fields, Field{Key: k, Value: val})
	return m
}

// AddFieldFromValue adds a [lineprotocol.Value] to the Point.
//
// Parameters:
//   - k: The key of the field.
//   - v: The value of the line protocol format.
//
// Returns:
//   - The updated Point with the field added.
//
// [lineprotocol.Value]: https://pkg.go.dev/github.com/influxdata/line-protocol/v2/lineprotocol#Value
func (m *Point) AddFieldFromValue(k string, v lineprotocol.Value) *Point {
	for i, field := range m.Fields {
		if k == field.Key {
			m.Fields[i].Value = v
			return m
		}
	}

	m.Fields = append(m.Fields, Field{Key: k, Value: v})
	return m
}

// AddField adds a field to the Point.
//
// Parameters:
//   - k: The key of the field.
//   - v: The value of the field.
//
// Returns:
//   - The updated Point with the field added.
func (m *Point) SetTimestamp(t time.Time) *Point {
	m.Timestamp = t
	return m
}

// MarshalBinary converts the Point to its binary representation in line protocol format.
//
// Parameters:
//   - precision: The precision to use for timestamp encoding in line protocol format.
//
// Returns:
//   - The binary representation of the Point in line protocol format.
//   - An error, if any.
func (m *Point) MarshalBinary(precision lineprotocol.Precision) ([]byte, error) {
	var enc lineprotocol.Encoder
	enc.SetPrecision(precision)
	enc.StartLine(m.Measurement)
	m.SortTags()
	for _, t := range m.Tags {
		enc.AddTag(t.Key, t.Value)
	}
	m.SortFields()
	for _, f := range m.Fields {
		enc.AddField(f.Key, f.Value)
	}
	enc.EndLine(m.Timestamp)
	if err := enc.Err(); err != nil {
		return nil, fmt.Errorf("encoding error: %v", err)
	}
	return enc.Bytes(), nil
}

// convertField converts any primitive type to types supported by line protocol
func convertField(v interface{}) interface{} {
	switch v := v.(type) {
	case bool, int64, uint64, string, float64:
		return v
	case int:
		return int64(v)
	case uint:
		return uint64(v)
	case []byte:
		return string(v)
	case int32:
		return int64(v)
	case int16:
		return int64(v)
	case int8:
		return int64(v)
	case uint32:
		return uint64(v)
	case uint16:
		return uint64(v)
	case uint8:
		return uint64(v)
	case float32:
		return float64(v)
	case time.Time:
		return v.Format(time.RFC3339Nano)
	case time.Duration:
		return v.String()
	default:
		return fmt.Sprintf("%v", v)
	}
}
