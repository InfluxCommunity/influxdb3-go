// Package testutil provides basic utilities for testing the client.
package testutil

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/decimal128"
	"github.com/apache/arrow/go/v15/arrow/decimal256"
	"github.com/apache/arrow/go/v15/arrow/flight"
	"github.com/apache/arrow/go/v15/arrow/float16"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

var BlobSize int64 = 4098

var Records = make(map[string][]arrow.Record)

type MockFlightServer struct {
	flight.BaseFlightServer
}

func writeBlob(fs flight.FlightService_DoGetServer, size int64) error {
	recs := MakeBlobRecords("test", size)

	w := flight.NewRecordWriter(fs, ipc.WithSchema(recs[0].Schema()))

	for _, r := range recs {
		err := w.Write(r)
		if err != nil {
			return err
		}
	}

	return nil
}

func (f *MockFlightServer) DoGet(tkt *flight.Ticket, fs flight.FlightService_DoGetServer) error {
	bt, btErr := BlobTicketFromJSONBytes(tkt.GetTicket())
	if btErr == nil {
		if bt.Name == "blob" {
			return writeBlob(fs, bt.Size)
		}
	}

	_, qtErr := SQLQueryTicketFromJSONBytes(tkt.GetTicket())

	if qtErr == nil {
		return writeBlob(fs, BlobSize)
	}

	recs, ok := Records[string(tkt.GetTicket())]
	if !ok {
		return status.Error(codes.NotFound, "flight not found")
	}

	w := flight.NewRecordWriter(fs, ipc.WithSchema(recs[0].Schema()))
	for _, r := range recs {
		err := w.Write(r)
		if err != nil {
			return err
		}
	}

	return nil
}

//nolint:all
func StartMockServer(t *testing.T) *flight.Server {
	mockServer := MockFlightServer{}
	s := flight.NewServerWithMiddleware([]flight.ServerMiddleware{})
	err := s.Init("localhost:0")
	if err != nil {
		assert.Fail(t, err.Error())
	}
	s.RegisterFlightService(&mockServer)

	go func() {
		err := s.Serve()
		if err != nil {
			assert.Fail(t, err.Error())
		}
	}()

	return &s
}

type BlobTicket struct {
	Name string
	Size int64
}

func NewBlobTicket(size int64) *BlobTicket {
	return &BlobTicket{Name: "blob", Size: size}
}

func (bt *BlobTicket) ToJSONString() string {
	return fmt.Sprintf(`{"Name": %q,"Size":%d}`, bt.Name, bt.Size)
}

func (bt *BlobTicket) ToJSONBytes() []byte {
	return []byte(bt.ToJSONString())
}

func BlobTicketFromJSONBytes(jsBytes []byte) (*BlobTicket, error) {
	s := string(jsBytes)
	m := map[string]any{}
	err := json.Unmarshal([]byte(s), &m)
	if err != nil {
		return nil, err
	}
	if m["Size"] == nil {
		return nil, errors.New("BlobTicket from json does not contain a size")
	}

	if m["Name"] == nil {
		return nil, errors.New("BlobTicket from json does not contain a name")
	}

	f, ok := m["Size"].(float64)
	if !ok {
		f = -1.0
	}
	return &BlobTicket{Name: m["Name"].(string), Size: int64(int(f))}, nil
}

type SQLQueryTicket struct {
	Database  string
	QueryType string
	SQLQuery  string
}

func NewSQLQueryTicket(database string, queryType string, query string) *SQLQueryTicket {
	return &SQLQueryTicket{Database: database, QueryType: queryType, SQLQuery: query}
}

func SQLQueryTicketFromJSONBytes(jsBytes []byte) (*SQLQueryTicket, error) {
	s := string(jsBytes)
	m := map[string]any{}
	err := json.Unmarshal([]byte(s), &m)
	if err != nil {
		return nil, err
	}

	if m["database"] == nil {
		return nil, errors.New("SQLQueryTicket from json does not contain a database")
	}

	if m["query_type"] == nil {
		return nil, errors.New("SQLQueryTicket from json does not contain a query_type")
	}

	if m["sql_query"] == nil {
		return nil, errors.New("SQLQueryTicket from json does not contain a sql_query")
	}

	return &SQLQueryTicket{Database: m["database"].(string),
			QueryType: m["query_type"].(string),
			SQLQuery:  m["sql_query"].(string)},
		nil
}

type ServAuth struct{}

func (a *ServAuth) Authenticate(c flight.AuthConn) error {
	tok, err := c.Read()
	if errors.Is(err, io.EOF) {
		return nil
	}

	if string(tok) != "foobar" {
		return errors.New("novalid")
	}

	if err != nil {
		return err
	}

	return c.Send([]byte("baz"))
}

func (a *ServAuth) IsValid(token string) (interface{}, error) {
	if token == "baz" {
		return "bar", nil
	}
	return "", errors.New("novalid")
}

func MakeBlobRecords(name string, size int64) []arrow.Record {
	mem := memory.NewGoAllocator()
	meta := arrow.NewMetadata([]string{"blob"}, []string{"blob_val"})

	schema := arrow.NewSchema([]arrow.Field{
		{Name: name, Type: arrow.PrimitiveTypes.Uint8, Nullable: false},
	}, &meta)

	data := make([]uint8, size)
	mask := make([]bool, size)

	for i := range data {
		//nolint:all
		data[i] = byte(rand.Intn(256))
		mask[i] = true
	}

	chunks := [][]arrow.Array{
		{
			arrayOf(mem, data, mask),
		},
	}

	defer func() {
		for _, chunk := range chunks {
			for _, col := range chunk {
				col.Release()
			}
		}
	}()

	recs := make([]arrow.Record, len(chunks))
	for i, chunk := range chunks {
		recs[i] = array.NewRecord(schema, chunk, -1)
	}

	Records[name] = recs
	return recs
}

func MakeConstantRecords() []arrow.Record {
	mem := memory.NewGoAllocator()

	meta := arrow.NewMetadata([]string{"data", "reference", "val"},
		[]string{"d_val", "r_val", "v_val"})

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "k1", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "k2", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "k3", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
	}, &meta)

	chunks := [][]arrow.Array{
		{
			arrayOf(mem, []string{"temp", "temp", "temp"}, []bool{true, true, true}),
			arrayOf(mem, []string{"kitchen", "common", "foyer"}, []bool{true, true, true}),
			arrayOf(mem, []float64{36.9, 25.7, 9.8}, []bool{true, true, true}),
		},
	}

	defer func() {
		for _, chunk := range chunks {
			for _, col := range chunk {
				col.Release()
			}
		}
	}()

	recs := make([]arrow.Record, len(chunks))
	for i, chunk := range chunks {
		recs[i] = array.NewRecord(schema, chunk, -1)
	}

	Records["constants"] = recs
	return recs
}

// copied from arrow-go/flight/flight_test.go

//nolint:all
func arrayOf(mem memory.Allocator, a interface{}, valids []bool) arrow.Array {
	if mem == nil {
		mem = memory.NewGoAllocator()
	}

	switch a := a.(type) {
	case []bool:
		bldr := array.NewBooleanBuilder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewBooleanArray()

	case []int8:
		bldr := array.NewInt8Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewInt8Array()

	case []int16:
		bldr := array.NewInt16Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewInt16Array()

	case []int32:
		bldr := array.NewInt32Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewInt32Array()

	case []int64:
		bldr := array.NewInt64Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewInt64Array()

	case []uint8:
		bldr := array.NewUint8Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewUint8Array()

	case []uint16:
		bldr := array.NewUint16Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewUint16Array()

	case []uint32:
		bldr := array.NewUint32Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewUint32Array()

	case []uint64:
		bldr := array.NewUint64Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewUint64Array()

	case []float16.Num:
		bldr := array.NewFloat16Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewFloat16Array()

	case []float32:
		bldr := array.NewFloat32Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewFloat32Array()

	case []float64:
		bldr := array.NewFloat64Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewFloat64Array()

	case []decimal128.Num:
		bldr := array.NewDecimal128Builder(mem, &arrow.Decimal128Type{Precision: 10, Scale: 1})
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		aa := bldr.NewDecimal128Array()
		return aa

	case []decimal256.Num:
		bldr := array.NewDecimal256Builder(mem, &arrow.Decimal256Type{Precision: 72, Scale: 2})
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		aa := bldr.NewDecimal256Array()
		return aa

	case []string:
		bldr := array.NewStringBuilder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewStringArray()

	case [][]byte:
		bldr := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewBinaryArray()

	case []arrow.Date32:
		bldr := array.NewDate32Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewArray()

	case []arrow.Date64:
		bldr := array.NewDate64Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewArray()

	case []arrow.MonthInterval:
		bldr := array.NewMonthIntervalBuilder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewArray()

	case []arrow.DayTimeInterval:
		bldr := array.NewDayTimeIntervalBuilder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewArray()

	case []arrow.MonthDayNanoInterval:
		bldr := array.NewMonthDayNanoIntervalBuilder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewArray()

	default:
		panic(fmt.Errorf("arrdata: invalid data slice type %T", a))
	}
}
