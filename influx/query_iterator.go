package influx

import (
	"fmt"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/flight"
)

type QueryIterator struct {
	reader *flight.Reader
	// Current record
	record arrow.Record
	// Index of row of current object in current record
	indexInRecord int
	// Total index of current object
	i int64
	// Current object
	current map[string]interface{}
	// Done
	done bool
}

func newQueryIterator(reader *flight.Reader) *QueryIterator {
	return &QueryIterator{
		reader:        reader,
		record:        nil,
		indexInRecord: -1,
		i:             -1,
		current:       nil,
	}
}

func (i *QueryIterator) Next() bool {
	if i.done {
		return false
	}
	i.indexInRecord++
	i.i++
	if i.record == nil || i.indexInRecord >= int(i.record.NumRows()) {
		if !i.reader.Next() {
			i.done = true
			return false
		}
		i.record = i.reader.Record()
		i.indexInRecord = 0
	}

	schema := i.reader.Schema()
	obj := make(map[string]interface{}, len(i.record.Columns()))

	for ci, col := range i.record.Columns() {
		name := schema.Field(ci).Name
		value, err := getArrowValue(col, i.indexInRecord)

		if err != nil {
			panic(err)
		}
		obj[name] = value
	}

	i.current = obj

	return true
}

func (i *QueryIterator) Value() map[string]interface{} {
	return i.current
}

func (i *QueryIterator) Index() interface{} {
	return i.i
}

func (i *QueryIterator) Done() bool {
	return i.done
}

func (i *QueryIterator) Raw() *flight.Reader {
	return i.reader
}

func getArrowValue(arrayNoType arrow.Array, i int) (interface{}, error) {
	switch arrayNoType.DataType().ID() {
	case arrow.NULL:
		return nil, nil
	case arrow.BOOL:
		return arrayNoType.(*array.Boolean).Value(i), nil
	case arrow.UINT8:
		return arrayNoType.(*array.Uint8).Value(i), nil
	case arrow.INT8:
		return arrayNoType.(*array.Int8).Value(i), nil
	case arrow.UINT16:
		return arrayNoType.(*array.Uint16).Value(i), nil
	case arrow.INT16:
		return arrayNoType.(*array.Int16).Value(i), nil
	case arrow.UINT32:
		return arrayNoType.(*array.Uint32).Value(i), nil
	case arrow.INT32:
		return arrayNoType.(*array.Int32).Value(i), nil
	case arrow.UINT64:
		return arrayNoType.(*array.Uint64).Value(i), nil
	case arrow.INT64:
		return arrayNoType.(*array.Int64).Value(i), nil
	case arrow.FLOAT16:
		return arrayNoType.(*array.Float16).Value(i), nil
	case arrow.FLOAT32:
		return arrayNoType.(*array.Float32).Value(i), nil
	case arrow.FLOAT64:
		return arrayNoType.(*array.Float64).Value(i), nil
	case arrow.STRING:
		return arrayNoType.(*array.String).Value(i), nil
	case arrow.BINARY:
		return arrayNoType.(*array.Binary).Value(i), nil
	case arrow.FIXED_SIZE_BINARY:
		return arrayNoType.(*array.FixedSizeBinary).Value(i), nil
	case arrow.DATE32:
		return arrayNoType.(*array.Date32).Value(i), nil
	case arrow.DATE64:
		return arrayNoType.(*array.Date64).Value(i), nil
	case arrow.TIMESTAMP:
		return arrayNoType.(*array.Timestamp).Value(i), nil
	case arrow.TIME32:
		return arrayNoType.(*array.Time32).Value(i), nil
	case arrow.TIME64:
		return arrayNoType.(*array.Time64).Value(i), nil
	case arrow.INTERVAL_MONTHS:
		return arrayNoType.(*array.MonthInterval).Value(i), nil
	case arrow.INTERVAL_DAY_TIME:
		return arrayNoType.(*array.DayTimeInterval).Value(i), nil
	case arrow.DECIMAL128:
		return arrayNoType.(*array.Decimal128).Value(i), nil
	case arrow.DECIMAL256:
		return arrayNoType.(*array.Decimal256).Value(i), nil
	// case arrow.LIST:
	// 	return arrayNoType.(*array.List).Value(i), nil
	// case arrow.STRUCT:
	// 	return arrayNoType.(*array.Struct).Value(i), nil
	// case arrow.SPARSE_UNION:
	// 	return arrayNoType.(*array.SparseUnion).Value(i), nil
	// case arrow.DENSE_UNION:
	// 	return arrayNoType.(*array.DenseUnion).Value(i), nil
	// case arrow.DICTIONARY:
	// 	return arrayNoType.(*array.Dictionary).Value(i), nil
	// case arrow.MAP:
	// 	return arrayNoType.(*array.Map).Value(i), nil
	// case arrow.EXTENSION:
	// 	return arrayNoType.(*array.ExtensionArrayBase).Value(i), nil
	// case arrow.FIXED_SIZE_LIST:
	// 	return arrayNoType.(*array.FixedSizeList).Value(i), nil
	case arrow.DURATION:
		return arrayNoType.(*array.Duration).Value(i), nil
	case arrow.LARGE_STRING:
		return arrayNoType.(*array.LargeString).Value(i), nil
	case arrow.LARGE_BINARY:
		return arrayNoType.(*array.LargeBinary).Value(i), nil
	// case arrow.LARGE_LIST:
	// 	return arrayNoType.(*array.LargeList).Value(i), nil
	case arrow.INTERVAL_MONTH_DAY_NANO:
		return arrayNoType.(*array.MonthDayNanoInterval).Value(i), nil
	// case arrow.RUN_END_ENCODED:
	// 	return arrayNoType.(*array.RunEndEncoded).Value(i), nil

	default:
		return nil, fmt.Errorf("not supported data type: %s", arrayNoType.DataType().ID().String())

	}
}
