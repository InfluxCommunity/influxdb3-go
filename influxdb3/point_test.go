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
	"math"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/influxdata/line-protocol/v2/lineprotocol"
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
		val       interface{}
		targetVal interface{}
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
				ft := reflect.TypeOf(float64(0))
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
		map[string]interface{}{
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

	line, err := p.MarshalBinary(lineprotocol.Nanosecond)
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
	}, map[string]interface{}{
		"float64": 80.1234567,
	}, time.Unix(60, 70))
	defaultTags := map[string]string{
		"tag2": "b",
		"tag3": "f",
	}

	line, err := p.MarshalBinary(lineprotocol.Nanosecond)
	require.NoError(t, err)
	assert.EqualValues(t, `test,tag1=a,tag3=c float64=80.1234567 60000000070`+"\n", string(line))

	line, err = p.MarshalBinaryWithDefaultTags(lineprotocol.Nanosecond, defaultTags)
	require.NoError(t, err)
	assert.EqualValues(t, `test,tag1=a,tag2=b,tag3=c float64=80.1234567 60000000070`+"\n", string(line))

	p.RemoveTag("tag3")

	line, err = p.MarshalBinary(lineprotocol.Nanosecond)
	require.NoError(t, err)
	assert.EqualValues(t, `test,tag1=a float64=80.1234567 60000000070`+"\n", string(line))

	line, err = p.MarshalBinaryWithDefaultTags(lineprotocol.Nanosecond, defaultTags)
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
		map[string]interface{}{
			"fVal": 41.3,
		}, time.Unix(60, 70))

	defaultTags := map[string]string{
		"defTag1": "default\nline and space",
		"defTag2": "escaped\\ndefault line and space",
		"ambiTag": "default\nambiguous\ntag",
	}

	line, err := p.MarshalBinary(lineprotocol.Nanosecond)
	require.NoError(t, err)
	assert.EqualValues(t,
		"test,ambiTag=ambiguous\\ntag,tabTag1=drink\\tTab,tabTag2=Tab\\tulator,"+
			"tag1=new\\nline\\ and\\ space,tag2=escaped\\nline\\ and\\ space fVal=41.3 60000000070\n",
		string(line))

	line, err = p.MarshalBinaryWithDefaultTags(lineprotocol.Nanosecond, defaultTags)
	require.NoError(t, err)
	assert.EqualValues(t,
		"test,ambiTag=ambiguous\\ntag,defTag1=default\\nline\\ and\\ space,"+
			"defTag2=escaped\\ndefault\\ line\\ and\\ space,tabTag1=drink\\tTab,tabTag2=Tab\\tulator,"+
			"tag1=new\\nline\\ and\\ space,tag2=escaped\\nline\\ and\\ space fVal=41.3 60000000070\n",
		string(line))

	pInvalid := NewPoint("test", map[string]string{
		"tag\nbroken": "tag\nvalue with space",
	}, map[string]interface{}{
		"fVal": 17.2,
	}, time.Unix(60, 70))

	_, err = pInvalid.MarshalBinary(lineprotocol.Nanosecond)
	require.Error(t, err)
	assert.EqualValues(t, "encoding error: invalid tag key \"tag\\nbroken\"", err.Error())
}

func TestPointFields(t *testing.T) {
	p := NewPoint("test", nil, map[string]interface{}{
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
	}, map[string]interface{}{
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

func TestFieldConverterValid(t *testing.T) {
	validConverterFunc := func(v interface{}) interface{} {
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
	point := createPointWithNamedType(&validConverterFunc)

	binary, err := point.MarshalBinary(lineprotocol.Nanosecond)
	assert.NoError(t, err)
	line := "measurement " +
		"bool=true,duration=\"12h11m10s\",float32=9,float64=10," +
		"int16=2i,int32=3i,int64=4i,int8=1i,string=\"11\"," +
		"time=\"2022-12-13T14:15:16Z\",uint16=6u,uint32=7u,uint64=8u,uint8=5u\n"

	assert.Equal(t, line, string(binary))
}

func TestFieldConverterInvalid(t *testing.T) {
	invalidConverterFunc := func(v interface{}) interface{} { return v }
	point := createPointWithNamedType(&invalidConverterFunc)

	binary, err := point.MarshalBinary(lineprotocol.Nanosecond)
	assert.Contains(t, err.Error(), "unsupported type:")
	assert.Nil(t, binary)
}

func createPointWithNamedType(converter *func(interface{}) interface{}) *Point {
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
