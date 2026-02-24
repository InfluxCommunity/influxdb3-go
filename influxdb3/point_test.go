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
	"bytes"
	"fmt"
	"math"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type ia int
type Int8 int8
type Int16 int16
type Int32 int32
type Int64 int64
type Uint8 uint8
type Uint16 uint16
type Uint32 uint32
type Uint64 uint64
type Float32 float32
type Float64 float64
type String string
type Bool bool

type st struct {
	d float64
	b bool
}

func (s st) String() string {
	return fmt.Sprintf("%.2f d %v", s.d, s.b)
}

func TestConvert(t *testing.T) {
	obj := []struct {
		val       any
		targetVal any
	}{
		{int(-5), int64(-5)},
		{int8(5), int64(5)},
		{int16(-51), int64(-51)},
		{int32(5), int64(5)},
		{int64(555), int64(555)},
		{uint(5), uint64(5)},
		{uint8(55), uint64(55)},
		{uint16(51), uint64(51)},
		{uint32(555), uint64(555)},
		{uint64(5555), uint64(5555)},
		{"a", "a"},
		{true, true},
		{float32(1.2), float64(1.2)},
		{float64(2.2), float64(2.2)},
		{ia(4), "4"},
		{[]string{"a", "b"}, "[a b]"},
		{map[int]string{1: "a", 2: "b"}, "map[1:a 2:b]"},
		{struct {
			a int
			s string
		}{0, "x"}, "{0 x}"},
		{st{1.22, true}, "1.22 d true"},
		{[]byte("test"), "test"},
		{time.Date(2022, 12, 13, 14, 15, 16, 0, time.UTC), "2022-12-13T14:15:16Z"},
		{12*time.Hour + 11*time.Minute + 10*time.Second, "12h11m10s"},
	}
	for _, tv := range obj {
		t.Run(reflect.TypeOf(tv.val).String(), func(t *testing.T) {
			v := convertField(tv.val)
			assert.Equal(t, reflect.TypeOf(tv.targetVal), reflect.TypeOf(v))
			if f, ok := tv.targetVal.(float64); ok {
				val := reflect.ValueOf(tv.val)
				ft := reflect.TypeFor[float64]()
				assert.True(t, val.Type().ConvertibleTo(ft))
				valf := val.Convert(ft)
				assert.Less(t, math.Abs(f-valf.Float()), 1e-6)
			} else {
				assert.EqualValues(t, tv.targetVal, v)
			}
		})
	}
}

func TestPoint(t *testing.T) {
	p := NewPoint(
		"test",
		map[string]string{
			"id":        "10ad=",
			"ven=dor":   "AWS",
			`host"name`: `ho\st "a"`,
			`x\" x`:     "a b",
		},
		map[string]any{
			"float64":  80.1234567,
			"float32":  float32(80.0),
			"int":      -1234567890,
			"int8":     int8(-34),
			"int16":    int16(-3456),
			"int32":    int32(-34567),
			"int64":    int64(-1234567890),
			"uint":     uint(12345677890),
			"uint8":    uint8(34),
			"uint16":   uint16(3456),
			"uint32":   uint32(34578),
			"uint 64":  uint64(41234567890),
			"bo\\ol":   false,
			`"string"`: `six, "seven", eight`,
			"stri=ng":  `six=seven\, eight`,
			"time":     time.Date(2020, time.March, 20, 10, 30, 23, 123456789, time.UTC),
			"duration": 4*time.Hour + 24*time.Minute + 3*time.Second,
		},
		time.Unix(60, 70))
	// Test duplicate tag and duplicate field
	p.SetTag("ven=dor", "GCP").SetField("uint32", uint32(345780))

	line, err := p.MarshalBinary(Nanosecond)
	require.NoError(t, err)
	assert.EqualValues(t, `test,host"name=ho\st\ "a",id=10ad\=,ven\=dor=GCP,x\"\ x=a\ b "string"="six, \"seven\", eight",bo\ol=false,duration="4h24m3s",float32=80,float64=80.1234567,int=-1234567890i,int16=-3456i,int32=-34567i,int64=-1234567890i,int8=-34i,stri\=ng="six=seven\\, eight",time="2020-03-20T10:30:23.123456789Z",uint=12345677890u,uint\ 64=41234567890u,uint16=3456u,uint32=345780u,uint8=34u 60000000070`+"\n", string(line)) //nolint
}

func TestPointTags(t *testing.T) {
	p := NewPoint("test", map[string]string{
		"tag1": "a",
		"tag2": "b",
	}, nil, time.Unix(60, 70))
	tagnames := (p.GetTagNames())
	sort.Strings(tagnames)
	assert.EqualValues(t, []string{"tag1", "tag2"}, tagnames)
	p.RemoveTag("tag1")
	tag, _ := p.GetTag("tag2")
	assert.Equal(t, "b", tag)
	assert.EqualValues(t, []string{"tag2"}, p.GetTagNames())
	p.SetTag("empty_value", "")
	assert.Equal(t, []string{"tag2"}, p.GetTagNames())
}

func TestPointDefaultTags(t *testing.T) {
	p := NewPoint("test", map[string]string{
		"tag1": "a",
		"tag3": "c",
	}, map[string]any{
		"float64": 80.1234567,
	}, time.Unix(60, 70))
	defaultTags := map[string]string{
		"tag2": "b",
		"tag3": "f",
	}

	line, err := p.MarshalBinary(Nanosecond)
	require.NoError(t, err)
	assert.EqualValues(t, `test,tag1=a,tag3=c float64=80.1234567 60000000070`+"\n", string(line))

	line, err = p.MarshalBinaryWithDefaultTags(Nanosecond, defaultTags)
	require.NoError(t, err)
	assert.EqualValues(t, `test,tag1=a,tag2=b,tag3=c float64=80.1234567 60000000070`+"\n", string(line))

	p.RemoveTag("tag3")

	line, err = p.MarshalBinary(Nanosecond)
	require.NoError(t, err)
	assert.EqualValues(t, `test,tag1=a float64=80.1234567 60000000070`+"\n", string(line))

	line, err = p.MarshalBinaryWithDefaultTags(Nanosecond, defaultTags)
	require.NoError(t, err)
	assert.EqualValues(t, `test,tag1=a,tag2=b,tag3=f float64=80.1234567 60000000070`+"\n", string(line))
}

func TestPointWithEscapedTags(t *testing.T) {
	p := NewPoint("test",
		map[string]string{
			"tag1":    "new\nline and space",
			"tag2":    "escaped\\nline and space",
			"ambiTag": "ambiguous\ntag",
			"tabTag1": "drink\tTab",
			"tabTag2": "Tab\\tulator",
		},
		map[string]any{
			"fVal": 41.3,
		}, time.Unix(60, 70))

	defaultTags := map[string]string{
		"defTag1": "default\nline and space",
		"defTag2": "escaped\\ndefault line and space",
		"ambiTag": "default\nambiguous\ntag",
	}

	line, err := p.MarshalBinary(Nanosecond)
	require.NoError(t, err)
	assert.EqualValues(t,
		"test,ambiTag=ambiguous\\ntag,tabTag1=drink\\tTab,tabTag2=Tab\\tulator,"+
			"tag1=new\\nline\\ and\\ space,tag2=escaped\\nline\\ and\\ space fVal=41.3 60000000070\n",
		string(line))

	line, err = p.MarshalBinaryWithDefaultTags(Nanosecond, defaultTags)
	require.NoError(t, err)
	assert.EqualValues(t,
		"test,ambiTag=ambiguous\\ntag,defTag1=default\\nline\\ and\\ space,"+
			"defTag2=escaped\\ndefault\\ line\\ and\\ space,tabTag1=drink\\tTab,tabTag2=Tab\\tulator,"+
			"tag1=new\\nline\\ and\\ space,tag2=escaped\\nline\\ and\\ space fVal=41.3 60000000070\n",
		string(line))

	pInvalid := NewPoint("test", map[string]string{
		"tag\nbroken": "tag\nvalue with space",
	}, map[string]any{
		"fVal": 17.2,
	}, time.Unix(60, 70))

	_, err = pInvalid.MarshalBinary(Nanosecond)
	require.Error(t, err)
	assert.EqualValues(t, "encoding error: invalid tag key \"tag\\nbroken\"", err.Error())
}

func TestPointEscapeCompatibilityCases(t *testing.T) {
	cases := []struct {
		name        string
		measurement string
		tags        map[string]string
		fields      map[string]any
		expected    string
		wantErr     string
	}{
		{
			name:        "measurement with space",
			measurement: "h2 o",
			tags:        map[string]string{"location": "europe"},
			fields:      map[string]any{"level": 2},
			expected:    "h2\\ o,location=europe level=2i\n",
		},
		{
			name:        "measurement with comma",
			measurement: "h2,o",
			tags:        map[string]string{"location": "europe"},
			fields:      map[string]any{"level": 2},
			expected:    "h2\\,o,location=europe level=2i\n",
		},
		{
			name:        "measurement with carriage return",
			measurement: "h2\ro",
			tags:        map[string]string{"location": "europe"},
			fields:      map[string]any{"level": 2},
			expected:    "h2\\ro,location=europe level=2i\n",
		},
		{
			name:        "equal sign escaping in tag and field keys",
			measurement: "h=2o",
			tags:        map[string]string{"l=ocation": "e=urope"},
			fields:      map[string]any{"l=evel": 2},
			expected:    "h=2o,l\\=ocation=e\\=urope l\\=evel=2i\n",
		},
		{
			name:        "tag key control characters are rejected",
			measurement: "h2o",
			tags: map[string]string{
				"new\nline": "new\nline",
			},
			fields:  map[string]any{"level": 2},
			wantErr: "encoding error: invalid tag key",
		},
		{
			name:        "field key control characters are rejected",
			measurement: "h2o",
			tags:        map[string]string{"location": "europe"},
			fields: map[string]any{
				"new\nline": 2,
			},
			wantErr: "encoding error: invalid field key",
		},
		{
			name:        "string field escapes backslash",
			measurement: "h2o",
			tags:        map[string]string{"location": "europe"},
			fields:      map[string]any{"level": "string esc\\ape value"},
			expected:    "h2o,location=europe level=\"string esc\\\\ape value\"\n",
		},
		{
			name:        "string field escapes double quote",
			measurement: "h2o",
			tags:        map[string]string{"location": "europe"},
			fields:      map[string]any{"level": "string esc\"ape value"},
			expected:    "h2o,location=europe level=\"string esc\\\"ape value\"\n",
		},
		{
			name:        "string field escapes newline carriage return and tab",
			measurement: "escapee",
			tags:        nil,
			fields:      map[string]any{"sVal": "greetings\nearthlings\rfrom\tthe ship"},
			expected:    "escapee sVal=\"greetings\\nearthlings\\rfrom\\tthe ship\"\n",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := NewPoint(tc.measurement, tc.tags, tc.fields, time.Time{})
			line, err := p.MarshalBinary(Nanosecond)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.EqualValues(t, tc.expected, string(line))
		})
	}
}

func TestPointFields(t *testing.T) {
	p := NewPoint("test", nil, map[string]any{
		"field1": 10,
		"field2": true,
	}, time.Unix(60, 70))
	fieldNames := p.GetFieldNames()
	sort.Strings(fieldNames)
	assert.EqualValues(t, []string{"field1", "field2"}, fieldNames)
	p.RemoveField("field1")
	assert.Equal(t, true, p.GetField("field2"))
	assert.EqualValues(t, []string{"field2"}, p.GetFieldNames())
}

func TestFieldValues(t *testing.T) {
	p := NewPoint("test", map[string]string{
		"tag1": "a",
	}, nil, time.Unix(60, 70))

	p.SetDoubleField("double", 1.2).
		SetIntegerField("int", int64(1)).
		SetUIntegerField("uint", uint64(42)).
		SetStringField("string", "a").
		SetBooleanField("bool", true)

	assert.InDelta(t, 1.2, *p.GetDoubleField("double"), 0.0)
	assert.Equal(t, int64(1), *p.GetIntegerField("int"))
	assert.Equal(t, uint64(42), *p.GetUIntegerField("uint"))
	assert.Equal(t, "a", *p.GetStringField("string"))
	assert.True(t, *p.GetBooleanField("bool"))
}

func TestCopy(t *testing.T) {
	point := NewPoint("test", map[string]string{
		"tag1": "a",
		"tag2": "b",
	}, map[string]any{
		"field1": 10,
		"field2": true,
	}, time.Unix(60, 70))
	pointCopy := point.Copy()

	assert.EqualValues(t, point, pointCopy)
}

func TestPoint_SetTimestamp(t *testing.T) {
	p := NewPoint("test", nil, nil, time.Unix(60, 70))
	p.SetTimestamp(time.Unix(60, 80))
	assert.Equal(t, time.Unix(60, 80), p.Values.Timestamp)
	p.SetTimestampWithEpoch(99)
	assert.Equal(t, time.Unix(0, 99), p.Values.Timestamp)
}

func TestFromValuesMissingMeasurement(t *testing.T) {
	values := &PointValues{}
	_, err := FromValues(values)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "missing measurement")
}

func TestMarshalBinaryMissingMeasurement(t *testing.T) {
	cases := []struct {
		name  string
		point *Point
	}{
		{
			name:  "nil point",
			point: nil,
		},
		{
			name:  "nil values",
			point: &Point{},
		},
		{
			name: "empty measurement in values",
			point: &Point{
				Values: &PointValues{
					Tags:   map[string]string{"host": "h1"},
					Fields: map[string]any{"value": 1},
				},
			},
		},
		{
			name: "new point values with empty measurement",
			point: func() *Point {
				p := NewPointWithPointValues(NewPointValues(""))
				p.SetField("value", 1)
				return p
			}(),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.point.MarshalBinary(Nanosecond)
			require.Error(t, err)
			assert.ErrorContains(t, err, "encoding error: missing measurement")
		})
	}
}

func TestFieldConverterValid(t *testing.T) {
	validConverterFunc := func(v any) any {
		switch v := v.(type) {
		case Int8:
			return int64(v)
		case Int16:
			return int64(v)
		case Int32:
			return int64(v)
		case Int64:
			return int64(v)
		case Uint8:
			return uint64(v)
		case Uint16:
			return uint64(v)
		case Uint32:
			return uint64(v)
		case Uint64:
			return uint64(v)
		case Float32:
			return float64(v)
		case Float64:
			return float64(v)
		case String:
			return string(v)
		case Bool:
			return bool(v)
		case time.Time:
			return v.Format(time.RFC3339Nano)
		case time.Duration:
			return v.String()
		}
		return v
	}
	point := createPointWithNamedType(validConverterFunc)

	binary, err := point.MarshalBinary(Nanosecond)
	assert.NoError(t, err)
	line := "measurement " +
		"bool=true,duration=\"12h11m10s\",float32=9,float64=10," +
		"int16=2i,int32=3i,int64=4i,int8=1i,string=\"11\"," +
		"time=\"2022-12-13T14:15:16Z\",uint16=6u,uint32=7u,uint64=8u,uint8=5u\n"

	assert.Equal(t, line, string(binary))
}

func TestFieldConverterInvalid(t *testing.T) {
	invalidConverterFunc := func(v any) any { return v }
	point := createPointWithNamedType(invalidConverterFunc)

	binary, err := point.MarshalBinary(Nanosecond)
	assert.Contains(t, err.Error(), "invalid value for field")
	assert.Nil(t, binary)
}

func createPointWithNamedType(converter func(any) any) *Point {
	point := NewPointWithMeasurement("measurement")
	point.WithFieldConverter(converter)

	point.SetField("int8", Int8(1))
	point.SetField("int16", Int16(2))
	point.SetField("int32", Int32(3))
	point.SetField("int64", Int64(4))
	point.SetField("uint8", Uint8(5))
	point.SetField("uint16", Uint16(6))
	point.SetField("uint32", Uint32(7))
	point.SetField("uint64", Uint64(8))
	point.SetField("float32", Float32(9))
	point.SetField("float64", Float64(10))
	point.SetField("string", String("11"))
	point.SetField("bool", Bool(true))
	point.SetField("time", time.Date(2022, 12, 13, 14, 15, 16, 0, time.UTC))
	point.SetField("duration", 12*time.Hour+11*time.Minute+10*time.Second)

	return point
}

func TestPointTimePrecisionConversions(t *testing.T) {
	p := NewPoint("test", nil, map[string]any{
		"field": 1,
	}, time.Unix(60, 70))

	cases := []struct {
		precision Precision
		expected  string
	}{
		{Nanosecond, "test field=1i 60000000070\n"},
		{Microsecond, "test field=1i 60000000\n"},
		{Millisecond, "test field=1i 60000\n"},
		{Second, "test field=1i 60\n"},
	}

	for _, tc := range cases {
		t.Run(tc.precision.String(), func(t *testing.T) {
			line, err := p.MarshalBinary(tc.precision)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, string(line))
		})
	}
}

func TestPointInvalidPrecisionPanics(t *testing.T) {
	p := NewPoint("test", nil, map[string]any{
		"field": 1,
	}, time.Unix(60, 70))

	assert.PanicsWithError(t, "unknown precision value 99", func() {
		_, _ = p.MarshalBinary(Precision(99))
	})
}

func TestPointOnlyNonFiniteFieldsReturnsEmpty(t *testing.T) {
	p := NewPoint("test", nil, map[string]any{
		"a": math.NaN(),
		"b": math.Inf(1),
		"c": float32(math.Inf(-1)),
	}, time.Unix(60, 70))

	line, err := p.MarshalBinary(Nanosecond)
	require.NoError(t, err)
	assert.EqualValues(t, "", string(line))
}

func TestPointNilFieldsAreOmitted(t *testing.T) {
	var nilBytes []byte
	var nilMap map[string]string

	p := NewPoint("test", nil, map[string]any{
		"a": nil,
		"b": nilBytes,
		"c": nilMap,
		"d": 1,
	}, time.Unix(60, 70))

	line, err := p.MarshalBinary(Nanosecond)
	require.NoError(t, err)
	assert.EqualValues(t, "test d=1i 60000000070\n", string(line))
}

func TestPointDefaultTagsDedupAndSkipEmpty(t *testing.T) {
	p := NewPoint("test", map[string]string{
		"tag1": "a",
		"tag3": "c",
	}, map[string]any{
		"field": 1,
	}, time.Unix(60, 70))

	line, err := p.MarshalBinaryWithDefaultTags(Nanosecond, map[string]string{
		"":     "skip-empty-key",
		"tag2": "b",
		"tag3": "ignored-because-point-tag-exists",
		"tag4": "",
	})
	require.NoError(t, err)
	assert.EqualValues(t, "test,tag1=a,tag2=b,tag3=c field=1i 60000000070\n", string(line))
}

func TestPointTagOrder(t *testing.T) {
	p := NewPoint("test", map[string]string{
		"host":   "h1",
		"region": "us-east",
		"rack":   "r1",
	}, map[string]any{
		"field": 1,
	}, time.Unix(60, 70))

	line, err := p.marshalBinaryWithOptions(Nanosecond, nil, []string{"region", "host"})
	require.NoError(t, err)
	assert.EqualValues(t, "test,region=us-east,host=h1,rack=r1 field=1i 60000000070\n", string(line))
}

func TestPointTagOrderWithDefaultTags(t *testing.T) {
	p := NewPoint("test", map[string]string{
		"host":   "h1",
		"region": "us-east",
	}, map[string]any{
		"field": 1,
	}, time.Unix(60, 70))

	defaultTags := map[string]string{
		"rack":   "r1",
		"zone":   "z1",
		"region": "ignored-by-point",
	}

	line, err := p.marshalBinaryWithOptions(Nanosecond, defaultTags, []string{"", "region", "host", "rack", "host", "missing"})
	require.NoError(t, err)
	assert.EqualValues(t, "test,region=us-east,host=h1,rack=r1,zone=z1 field=1i 60000000070\n", string(line))
}

func TestPointNilAndEmptyDefaultTagsSameOutput(t *testing.T) {
	p := NewPoint("test", map[string]string{
		"tag1": "a",
	}, map[string]any{
		"field": 1,
	}, time.Unix(60, 70))

	lineNil, err := p.MarshalBinaryWithDefaultTags(Nanosecond, nil)
	require.NoError(t, err)

	lineEmpty, err := p.MarshalBinaryWithDefaultTags(Nanosecond, map[string]string{})
	require.NoError(t, err)

	assert.EqualValues(t, string(lineNil), string(lineEmpty))
	assert.EqualValues(t, "test,tag1=a field=1i 60000000070\n", string(lineNil))
}

func TestPointEscapeCoverage(t *testing.T) {
	p := NewPoint("me as,ure", map[string]string{
		"ta g,=": "va l,=",
	}, map[string]any{
		"fi eld,=": `qu"o\te`,
	}, time.Time{})

	line, err := p.MarshalBinary(Nanosecond)
	require.NoError(t, err)
	assert.EqualValues(t, `me\ as\,ure,ta\ g\,\==va\ l\,\= fi\ eld\,\=="qu\"o\\te"`+"\n", string(line))
}

func TestPointSerializerCoverageCases(t *testing.T) {
	t.Run("default tag key control characters are rejected", func(t *testing.T) {
		p := NewPoint("test", map[string]string{
			"tag": "a",
		}, map[string]any{
			"field": 1,
		}, time.Unix(60, 70))

		_, err := p.MarshalBinaryWithDefaultTags(Nanosecond, map[string]string{
			"bad\nkey": "x",
		})
		require.Error(t, err)
		assert.ErrorContains(t, err, "encoding error: invalid tag key")
	})

	t.Run("appendFieldValue supports all types", func(t *testing.T) {
		cases := []struct {
			name     string
			value    any
			expected string
			wantErr  string
		}{
			{name: "float64", value: float64(1.5), expected: "1.5"},
			{name: "float32", value: float32(1.25), expected: "1.25"},
			{name: "int", value: int(-1), expected: "-1i"},
			{name: "int8", value: int8(-2), expected: "-2i"},
			{name: "int16", value: int16(-3), expected: "-3i"},
			{name: "int32", value: int32(-4), expected: "-4i"},
			{name: "int64", value: int64(-5), expected: "-5i"},
			{name: "uint", value: uint(1), expected: "1u"},
			{name: "uint8", value: uint8(2), expected: "2u"},
			{name: "uint16", value: uint16(3), expected: "3u"},
			{name: "uint32", value: uint32(4), expected: "4u"},
			{name: "uint64", value: uint64(5), expected: "5u"},
			{name: "bool true", value: true, expected: "true"},
			{name: "bool false", value: false, expected: "false"},
			{name: "string", value: "text", expected: `"text"`},
			{name: "bytes", value: []byte(`x"y`), expected: `"x\"y"`},
			{name: "invalid type", value: struct{}{}, wantErr: "invalid value for field"},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				var sb bytes.Buffer
				err := appendFieldValue(&sb, "f", tc.value)
				if tc.wantErr != "" {
					require.Error(t, err)
					assert.ErrorContains(t, err, tc.wantErr)
					return
				}
				require.NoError(t, err)
				assert.Equal(t, tc.expected, sb.String())
			})
		}
	})

	t.Run("isNotDefined handles float32 NaN and finite", func(t *testing.T) {
		assert.True(t, isNotDefined(float32(math.NaN())))
		assert.False(t, isNotDefined(float32(1.25)))
	})
}
