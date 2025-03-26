package influxdb3

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/flight"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/stretchr/testify/assert"
)

type testMessagesReader struct {
	r ipc.MessageReader
}

func (r *testMessagesReader) Message() (*ipc.Message, error) {
	return r.r.Message()
}
func (r *testMessagesReader) Release() {}
func (r *testMessagesReader) Retain()  {}

func TestQueryIteratorEmptyRecord(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "f1", Type: arrow.PrimitiveTypes.Int32},
	}, nil)
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))

	rb := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	rec := rb.NewRecord() // first record is empty
	err := writer.Write(rec)
	assert.NoError(t, err)

	rb.Field(0).(*array.Int32Builder).AppendValues([]int32{1}, nil)
	rec = rb.NewRecord() // second record is not empty
	err = writer.Write(rec)
	assert.NoError(t, err)

	err = writer.Close()
	assert.NoError(t, err)

	reader := ipc.NewMessageReader(&buf)

	ipcReader, err := ipc.NewReaderFromMessageReader(
		&testMessagesReader{
			r: reader,
		})
	assert.NoError(t, err)

	fReader := &flight.Reader{Reader: ipcReader}
	it := newQueryIterator(fReader)

	count := 0
	for it.Next() {
		assert.Equal(t, 1, it.record.Column(0).(*array.Int32).Len())
		assert.Equal(t, int32(1), it.record.Column(0).(*array.Int32).Value(0))
		assert.Equal(t, 0, count)
		count++
	}
	assert.Equal(t, 1, count)
}

func TestQueryIteratorError(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "f1", Type: arrow.PrimitiveTypes.Int32},
	}, nil)
	var buf bytes.Buffer
	// buf := bytes.NewBuffer(make([]byte, 64))
	//buf := make([]byte, 4)
	// test_err := errors.New("test error")
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	rb := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	rec := rb.NewRecord() // first record is empty
	err := writer.Write(rec)
	assert.NoError(t, err)

	rb.Field(0).(*array.Int32Builder).AppendValues([]int32{42, 3, 21}, nil)
	rec = rb.NewRecord() // second record is not empty
	err = writer.Write(rec)
	assert.NoError(t, err)

	// rb.Field(0).(*array.Int32Builder).AppendValues([]int32{21}, nil)
	// rec2 := rb.NewRecord() // second record is not empty
	// err = writer.Write(rec2)
	// assert.NoError(t, err)

	reader := ipc.NewMessageReader(&buf)

	ipcReader, err := ipc.NewReaderFromMessageReader(
		&testMessagesReader{
			r: reader,
		})
	assert.NoError(t, err)

	fReader := &flight.Reader{Reader: ipcReader}
	it := newQueryIterator(fReader)
	fmt.Printf("\nDEBUG it.Next() %v\n", it.Next())
	fmt.Printf("\nDEBUG it %+v\n", it)
	fmt.Printf("\nDEBUG it.Next() %v\n", it.Next())
	fmt.Printf("\nDEBUG it %+v\n", it)

}
