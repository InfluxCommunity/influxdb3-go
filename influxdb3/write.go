package influxdb3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/influxdb3/gzip"
)

// WritePoints writes all the given points to the server into the given database.
// The data is written synchronously.
//
// Parameters:
//   - ctx: The context.Context to use for the request.
//   - database: The database to write the points to.
//   - points: The points to write.
//
// Returns:
//   - An error, if any.
func (c *Client) WritePoints(ctx context.Context, database string, points ...*Point) error {
	var buff []byte
	for _, p := range points {
		bts, err := p.MarshalBinary(c.config.WriteOptions.Precision)
		if err != nil {
			return err
		}
		buff = append(buff, bts...)
	}
	return c.Write(ctx, database, buff)
}

// Write writes line protocol record(s) to the server into the given database.
// Multiple records must be separated by the new line character (\n).
// The data is written synchronously.
//
// Parameters:
//   - ctx: The context.Context to use for the request.
//   - database: The database to write the records to.
//   - buff: The line protocol record(s) to write.
//
// Returns:
//   - An error, if any.
func (c *Client) Write(ctx context.Context, database string, buff []byte) error {
	var body io.Reader
	var err error
	u, _ := c.apiURL.Parse("write")
	params := u.Query()
	params.Set("org", c.config.Organization)
	params.Set("bucket", database)
	params.Set("precision", c.config.WriteOptions.Precision.String())
	u.RawQuery = params.Encode()
	body = bytes.NewReader(buff)
	headers := http.Header{"Content-Type": {"application/json"}}
	if c.config.WriteOptions.GzipThreshold > 0 && len(buff) >= c.config.WriteOptions.GzipThreshold {
		body, err = gzip.CompressWithGzip(body)
		if err != nil {
			return fmt.Errorf("unable to compress write body: %w", err)
		}
		headers["Content-Encoding"] = []string{"gzip"}
	}
	_, err = c.makeAPICall(ctx, httpParams{
		endpointURL: u,
		httpMethod:  "POST",
		headers:     headers,
		queryParams: u.Query(),
		body:        body,
	})
	return err
}

// WriteData encodes fields of custom points into line protocol
// and writes line protocol record(s) to the server into the given database.
// Each custom point must be annotated with 'lp' prefix and values measurement, tag, field, or timestamp.
// A valid point must contain a measurement and at least one field.
// The points are written synchronously.
//
// A field with a timestamp must be of type time.Time.
//
// Example usage:
//
//	type TemperatureSensor struct {
//	    Measurement  string    `lp:"measurement"`
//	    Sensor       string    `lp:"tag,sensor"`
//	    ID           string    `lp:"tag,device_id"`
//	    Temp         float64   `lp:"field,temperature"`
//	    Hum          int       `lp:"field,humidity"`
//	    Time         time.Time `lp:"timestamp"`
//	    Description  string    `lp:"-"`
//	}
//
// Parameters:
//   - ctx: The context.Context to use for the request.
//   - database: The database to write the points to.
//   - points: The custom points to encode and write.
//
// Returns:
//   - An error, if any.
func (c *Client) WriteData(ctx context.Context, database string, points ...interface{}) error {
	var buff []byte
	for _, p := range points {
		byts, err := encode(p, c.config.WriteOptions)
		if err != nil {
			return fmt.Errorf("error encoding point: %w", err)
		}
		buff = append(buff, byts...)
	}

	return c.Write(ctx, database, buff)
}

func encode(x interface{}, options WriteOptions) ([]byte, error) {
	if err := checkContainerType(x, false, "point"); err != nil {
		return nil, err
	}
	t := reflect.TypeOf(x)
	v := reflect.ValueOf(x)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}
	fields := reflect.VisibleFields(t)

	var point Point

	for _, f := range fields {
		name := f.Name
		if tag, ok := f.Tag.Lookup("lp"); ok {
			if tag == "-" {
				continue
			}
			parts := strings.Split(tag, ",")
			if len(parts) > 2 {
				return nil, fmt.Errorf("multiple tag attributes are not supported")
			}
			typ := parts[0]
			if len(parts) == 2 {
				name = parts[1]
			}
			switch typ {
			case "measurement":
				if point.Measurement != "" {
					return nil, fmt.Errorf("multiple measurement fields")
				}
				point.Measurement = v.FieldByIndex(f.Index).String()
			case "tag":
				point.AddTag(name, v.FieldByIndex(f.Index).String())
			case "field":
				point.AddField(name, v.FieldByIndex(f.Index).Interface())
			case "timestamp":
				if f.Type != timeType {
					return nil, fmt.Errorf("cannot use field '%s' as a timestamp", f.Name)
				}
				point.Timestamp = v.FieldByIndex(f.Index).Interface().(time.Time)
			default:
				return nil, fmt.Errorf("invalid tag %s", typ)
			}
		}
	}
	if point.Measurement == "" {
		return nil, fmt.Errorf("no struct field with tag 'measurement'")
	}
	if len(point.Fields) == 0 {
		return nil, fmt.Errorf("no struct field with tag 'field'")
	}
	return point.MarshalBinary(options.Precision)
}
