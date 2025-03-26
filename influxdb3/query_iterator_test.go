package influxdb3

import (
	"bytes"
	"errors"
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

type ErrorMessageMockReader struct {
	counter      int
	errorMessage string
}

func (emmr *ErrorMessageMockReader) Message() (*ipc.Message, error) {
	if emmr.counter == 0 {
		emmr.counter++
		// return schema message
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "f1", Type: arrow.PrimitiveTypes.Int32},
		}, nil)
		var buf bytes.Buffer
		writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
		if err := writer.Close(); err != nil {
			return nil, err
		}
		reader := ipc.NewMessageReader(&buf)
		return reader.Message()
	}
	return nil, errors.New(emmr.errorMessage)
}

func (emmr *ErrorMessageMockReader) Release() {}

func (emmr *ErrorMessageMockReader) Retain() {}

func TestQueryIteratorError(t *testing.T) {
	errorMessage := "TEST ERROR"

	mockReader, newMsgErr := ipc.NewReaderFromMessageReader(&ErrorMessageMockReader{errorMessage: errorMessage})

	if newMsgErr != nil {
		t.Fatal(newMsgErr)
	}

	fReader := &flight.Reader{Reader: mockReader}

	testIT := newQueryIterator(fReader)
	assert.False(t, testIT.Next(), "iterator should have no next record")
	assert.Equal(t, testIT.Err().Error(), errorMessage)
}
