package influxdb3

import (
	"bytes"
	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/flight"
	"github.com/apache/arrow/go/v13/arrow/ipc"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/stretchr/testify/assert"
	"testing"
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
	assert.Nil(t, err)

	rb.Field(0).(*array.Int32Builder).AppendValues([]int32{1}, nil)
	rec = rb.NewRecord() // second record is not empty
	err = writer.Write(rec)
	assert.Nil(t, err)

	err = writer.Close()
	assert.Nil(t, err)

	reader := ipc.NewMessageReader(&buf)

	ipcReader, err := ipc.NewReaderFromMessageReader(
		&testMessagesReader{
			r: reader,
		})
	assert.Nil(t, err)

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
