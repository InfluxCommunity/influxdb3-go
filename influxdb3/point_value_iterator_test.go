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
	"errors"
	"slices"
	"testing"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/flight"
	"github.com/apache/arrow/go/v15/arrow/float16"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestPointValueIterator(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "f0", Type: arrow.PrimitiveTypes.Int64},
		{Name: "f1", Type: arrow.PrimitiveTypes.Uint8},
		{Name: "f2", Type: arrow.PrimitiveTypes.Int8},
		{Name: "f3", Type: arrow.PrimitiveTypes.Uint16},
		{Name: "f4", Type: arrow.PrimitiveTypes.Int16},
		{Name: "f5", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "f6", Type: arrow.BinaryTypes.LargeBinary},
		{Name: "f7", Type: arrow.BinaryTypes.LargeString},
		{Name: "f8", Type: arrow.BinaryTypes.Binary},
		{Name: "f9", Type: arrow.PrimitiveTypes.Date32},
		{Name: "f10", Type: arrow.PrimitiveTypes.Date64},
		{Name: "f11", Type: arrow.FixedWidthTypes.Float16},
		{Name: "f12", Type: arrow.PrimitiveTypes.Float32},
		{Name: "f13", Type: arrow.FixedWidthTypes.Time32s},
		{Name: "f14", Type: arrow.FixedWidthTypes.Time64us},
		{Name: "f15", Type: arrow.FixedWidthTypes.MonthInterval},
		{Name: "f16", Type: arrow.FixedWidthTypes.DayTimeInterval},
		{Name: "f17", Type: arrow.FixedWidthTypes.Duration_s},
	}, nil)

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	defer writer.Close()

	rb := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer rb.Release()
	rec := rb.NewRecord() // first record is empty
	_ = writer.Write(rec)

	rb.Field(0).(*array.Int64Builder).Append(0)
	rb.Field(1).(*array.Uint8Builder).Append(1)
	rb.Field(2).(*array.Int8Builder).Append(2)
	rb.Field(3).(*array.Uint16Builder).Append(3)
	rb.Field(4).(*array.Int16Builder).Append(4)
	rb.Field(5).(*array.Uint32Builder).Append(5)
	rb.Field(6).(*array.BinaryBuilder).Append([]byte{6})
	rb.Field(7).(*array.LargeStringBuilder).Append("7")
	rb.Field(8).(*array.BinaryBuilder).Append([]byte{8})
	rb.Field(9).(*array.Date32Builder).Append(arrow.Date32(int32(9)))
	rb.Field(10).(*array.Date64Builder).Append(arrow.Date64(int64(10)))
	rb.Field(11).(*array.Float16Builder).Append(float16.New(11))
	rb.Field(12).(*array.Float32Builder).Append(float32(12))
	rb.Field(13).(*array.Time32Builder).Append(arrow.Time32(int32(13)))
	rb.Field(14).(*array.Time64Builder).Append(arrow.Time64(int64(14)))
	rb.Field(15).(*array.MonthIntervalBuilder).Append(arrow.MonthInterval(int32(15)))
	rb.Field(16).(*array.DayTimeIntervalBuilder).AppendNull()
	rb.Field(17).(*array.DurationBuilder).Append(arrow.Duration(int64(17)))

	rec = rb.NewRecord()
	_ = writer.Write(rec)

	rb.Field(0).(*array.Int64Builder).Append(0)
	rb.Field(1).(*array.Uint8Builder).Append(1)
	rb.Field(2).(*array.Int8Builder).Append(2)
	rb.Field(3).(*array.Uint16Builder).Append(3)
	rb.Field(4).(*array.Int16Builder).Append(4)
	rb.Field(5).(*array.Uint32Builder).Append(5)
	rb.Field(6).(*array.BinaryBuilder).Append([]byte{6})
	rb.Field(7).(*array.LargeStringBuilder).Append("7")
	rb.Field(8).(*array.BinaryBuilder).Append([]byte{8})
	rb.Field(9).(*array.Date32Builder).Append(arrow.Date32(int32(9)))
	rb.Field(10).(*array.Date64Builder).Append(arrow.Date64(int64(10)))
	rb.Field(11).(*array.Float16Builder).Append(float16.New(11))
	rb.Field(12).(*array.Float32Builder).Append(float32(12))
	rb.Field(13).(*array.Time32Builder).Append(arrow.Time32(int32(13)))
	rb.Field(14).(*array.Time64Builder).Append(arrow.Time64(int64(14)))
	rb.Field(15).(*array.MonthIntervalBuilder).Append(arrow.MonthInterval(int32(15)))
	rb.Field(16).(*array.DayTimeIntervalBuilder).AppendNull()
	rb.Field(17).(*array.DurationBuilder).Append(arrow.Duration(int64(17)))

	rec = rb.NewRecord()
	_ = writer.Write(rec)

	reader := ipc.NewMessageReader(&buf)

	ipcReader, err := ipc.NewReaderFromMessageReader(
		&testMessagesReader{
			r: reader,
		})
	assert.NoError(t, err)

	fReader := &flight.Reader{Reader: ipcReader}
	it := newPointValueIterator(fReader)

	var resultSet0 []int64
	var resultSet1 []interface{}
	var resultSet2 []interface{}
	var resultSet3 []interface{}
	var resultSet4 []interface{}
	var resultSet5 []interface{}
	var resultSet6 []interface{}
	var resultSet7 []interface{}
	var resultSet8 []interface{}
	var resultSet9 []interface{}
	var resultSet10 []interface{}
	var resultSet11 []interface{}
	var resultSet12 []interface{}
	var resultSet13 []interface{}
	var resultSet14 []interface{}
	var resultSet15 []interface{}
	var resultSet16 []interface{}
	var resultSet17 []interface{}

	for {
		pointValues, err := it.Next()
		if errors.Is(err, Done) {
			break
		}
		assert.NotNil(t, pointValues)
		assert.NoError(t, err)

		resultSet0 = append(resultSet0, *pointValues.GetIntegerField("f0"))
		resultSet1 = append(resultSet1, pointValues.GetField("f1"))
		resultSet2 = append(resultSet2, pointValues.GetField("f2"))
		resultSet3 = append(resultSet3, pointValues.GetField("f3"))
		resultSet4 = append(resultSet4, pointValues.GetField("f4"))
		resultSet5 = append(resultSet5, pointValues.GetField("f5"))
		resultSet6 = append(resultSet6, pointValues.GetField("f6"))
		resultSet7 = append(resultSet7, pointValues.GetField("f7"))
		resultSet8 = append(resultSet8, pointValues.GetField("f8"))
		resultSet9 = append(resultSet9, pointValues.GetField("f9"))
		resultSet10 = append(resultSet10, pointValues.GetField("f10"))
		resultSet11 = append(resultSet11, pointValues.GetField("f11"))
		resultSet12 = append(resultSet12, pointValues.GetField("f12"))
		resultSet13 = append(resultSet13, pointValues.GetField("f13"))
		resultSet14 = append(resultSet14, pointValues.GetField("f14"))
		resultSet15 = append(resultSet15, pointValues.GetField("f15"))
		resultSet16 = append(resultSet16, pointValues.GetField("f16"))
		resultSet17 = append(resultSet17, pointValues.GetField("f17"))
	}

	assert.True(t, slices.Equal([]int64{0, 0}, resultSet0))

	assert.True(t, resultSet1[0] == uint8(1))
	assert.True(t, resultSet1[1] == uint8(1))

	assert.True(t, resultSet2[0] == int8(2))
	assert.True(t, resultSet2[1] == int8(2))

	assert.True(t, resultSet3[0] == uint16(3))
	assert.True(t, resultSet3[1] == uint16(3))

	assert.True(t, resultSet4[0] == int16(4))
	assert.True(t, resultSet4[1] == int16(4))

	assert.True(t, resultSet5[0] == uint32(5))
	assert.True(t, resultSet5[1] == uint32(5))

	assert.True(t, resultSet6[0].([]uint8)[0] == uint8(6))
	assert.True(t, resultSet6[1].([]uint8)[0] == uint8(6))

	assert.True(t, resultSet7[0] == "7")
	assert.True(t, resultSet7[1] == "7")

	assert.True(t, resultSet8[0].([]uint8)[0] == uint8(8))
	assert.True(t, resultSet8[1].([]uint8)[0] == uint8(8))

	assert.True(t, resultSet9[0] == arrow.Date32(int32(9)))
	assert.True(t, resultSet9[1] == arrow.Date32(int32(9)))

	assert.True(t, resultSet10[0] == arrow.Date64(int64(10)))
	assert.True(t, resultSet10[1] == arrow.Date64(int64(10)))

	assert.True(t, resultSet11[0].(float16.Num).Uint16() == 18816)
	assert.True(t, resultSet11[1].(float16.Num).Uint16() == 18816)

	assert.True(t, resultSet12[0] == float32(12))
	assert.True(t, resultSet12[1] == float32(12))

	assert.True(t, resultSet13[0] == arrow.Time32(int32(13)))
	assert.True(t, resultSet13[1] == arrow.Time32(int32(13)))

	assert.True(t, resultSet14[0] == arrow.Time64(int64(14)))
	assert.True(t, resultSet14[1] == arrow.Time64(int64(14)))

	assert.True(t, resultSet15[0] == arrow.MonthInterval(int32(15)))
	assert.True(t, resultSet15[1] == arrow.MonthInterval(int32(15)))

	assert.True(t, resultSet16[0] == nil)
	assert.True(t, resultSet16[1] == nil)

	assert.True(t, resultSet17[0] == arrow.Duration(int64(17)))
	assert.True(t, resultSet17[1] == arrow.Duration(int64(17)))

	pointValues, err := it.Next()
	assert.Equal(t, 2, it.Index())
	assert.Equal(t, err, Done)
	assert.Nil(t, pointValues)
}
