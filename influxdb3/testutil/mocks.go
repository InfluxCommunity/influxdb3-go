// Package testutil provides basic utilities for testing the client.
package testutil

import (
	"bytes"
	"errors"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/ipc"
)

type ErrorMessageMockReader struct {
	Counter      int
	ErrorMessage string
}

func (emmr *ErrorMessageMockReader) Message() (*ipc.Message, error) {
	if emmr.Counter == 0 {
		emmr.Counter++
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
	return nil, errors.New(emmr.ErrorMessage)
}

func (emmr *ErrorMessageMockReader) Release() {}

func (emmr *ErrorMessageMockReader) Retain() {}
