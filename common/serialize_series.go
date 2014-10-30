package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"

	log "code.google.com/p/log4go"
	"github.com/influxdb/influxdb/protocol"
)

var (
	TRUE  = true
	FALSE = false
)

type TimePrecision int

const (
	MicrosecondPrecision TimePrecision = iota
	MillisecondPrecision
	SecondPrecision
)

func init() {
}

func removeField(fields []string, name string) []string {
	index := -1
	for idx, field := range fields {
		if field == name {
			index = idx
			break
		}
	}

	if index == -1 {
		return fields
	}

	return append(fields[:index], fields[index+1:]...)
}

func removeTimestampFieldDefinition(fields []string) []string {
	fields = removeField(fields, "time")
	return removeField(fields, "sequence_number")
}

type ApiSeries interface {
	GetName() string
	GetColumns() []string
	GetPoints() [][]interface{}
}

func hasDuplicates(ss []string) bool {
	m := make(map[string]struct{}, len(ss))
	for _, s := range ss {
		if _, ok := m[s]; ok {
			return true
		}
		m[s] = struct{}{}
	}
	return false
}

func ConvertToDataStoreSeries(s ApiSeries, precision TimePrecision) (*protocol.Series, error) {
	points := make([]*protocol.Point, 0, len(s.GetPoints()))
	if hasDuplicates(s.GetColumns()) {
		return nil, fmt.Errorf("Cannot have duplicate field names")
	}

	for _, point := range s.GetPoints() {
		if len(point) != len(s.GetColumns()) {
			return nil, fmt.Errorf("invalid payload")
		}

		values := make([]*protocol.FieldValue, 0, len(point))
		var timestamp *int64
		var sequence *uint64

		for idx, field := range s.GetColumns() {

			value := point[idx]
			if field == "time" {
				switch x := value.(type) {
				case json.Number:
					f, err := x.Float64()
					if err != nil {
						return nil, err
					}
					_timestamp := int64(f)
					switch precision {
					case SecondPrecision:
						_timestamp *= 1000
						fallthrough
					case MillisecondPrecision:
						_timestamp *= 1000
					}

					timestamp = &_timestamp
					continue
				default:
					return nil, fmt.Errorf("time field must be float but is %T (%v)", value, value)
				}
			}

			if field == "sequence_number" {
				switch x := value.(type) {
				case json.Number:
					f, err := x.Float64()
					if err != nil {
						return nil, err
					}
					_sequenceNumber := uint64(f)
					sequence = &_sequenceNumber
					continue
				default:
					return nil, fmt.Errorf("sequence_number field must be float but is %T (%v)", value, value)
				}
			}

			switch v := value.(type) {
			case string:
				values = append(values, &protocol.FieldValue{StringValue: &v})
			case json.Number:
				i, err := v.Int64()
				if err == nil {
					values = append(values, &protocol.FieldValue{Int64Value: &i})
					break
				}
				f, err := v.Float64()
				if err != nil {
					return nil, err
				}
				values = append(values, &protocol.FieldValue{DoubleValue: &f})
			case bool:
				values = append(values, &protocol.FieldValue{BoolValue: &v})
			case nil:
				values = append(values, &protocol.FieldValue{IsNull: &TRUE})
			default:
				// if we reached this line then the dynamic type didn't match
				return nil, fmt.Errorf("Unknown type %T", value)
			}
		}
		points = append(points, &protocol.Point{
			Values:         values,
			Timestamp:      timestamp,
			SequenceNumber: sequence,
		})
	}

	fields := removeTimestampFieldDefinition(s.GetColumns())

	series := &protocol.Series{
		Name:   protocol.String(s.GetName()),
		Fields: fields,
		Points: points,
	}
	return series, nil
}

// takes a slice of protobuf series and convert them to the format
// that the http api expect
func SerializeSeries(memSeries map[string]*protocol.Series, precision TimePrecision) []*SerializedSeries {
	serializedSeries := []*SerializedSeries{}

	for _, series := range memSeries {
		includeSequenceNumber := true
		if len(series.Points) > 0 && series.Points[0].SequenceNumber == nil {
			includeSequenceNumber = false
		}

		columns := []string{"time"}
		if includeSequenceNumber {
			columns = append(columns, "sequence_number")
		}
		for _, field := range series.Fields {
			columns = append(columns, field)
		}

		points := [][]interface{}{}
		for _, row := range series.Points {
			timestamp := int64(0)
			if t := row.Timestamp; t != nil {
				timestamp = *row.GetTimestampInMicroseconds()
				switch precision {
				case SecondPrecision:
					timestamp /= 1000
					fallthrough
				case MillisecondPrecision:
					timestamp /= 1000
				}
			}

			rowValues := []interface{}{timestamp}
			s := uint64(0)
			if includeSequenceNumber {
				if row.SequenceNumber != nil {
					s = row.GetSequenceNumber()
				}
				rowValues = append(rowValues, s)
			}
			for _, value := range row.Values {
				if value == nil {
					rowValues = append(rowValues, nil)
					continue
				}
				v, ok := value.GetValue()
				if !ok {
					rowValues = append(rowValues, nil)
					log.Warn("Infinite or NaN value encountered")
					continue
				}
				rowValues = append(rowValues, v)
			}
			points = append(points, rowValues)
		}

		serializedSeries = append(serializedSeries, &SerializedSeries{
			Name:    *series.Name,
			Columns: columns,
			Points:  points,
		})
	}

	//	SortSerializedSeries(serializedSeries)
	return serializedSeries
}

func SerializeSeriesJson(memSeries map[string]*protocol.Series, precision TimePrecision, numberOfSeries uint64) []byte {
	var (
		buf               bytes.Buffer
		memSeriesFirstRun bool
		pointsFirstRun    bool
		cnt               uint64
	)
	cnt = 0
	buf.Grow(128 * 1024)
	if numberOfSeries != 1 {
		buf.WriteString("[")
	}

	memSeriesFirstRun = true
	for _, series := range memSeries {
		cnt++
		if !memSeriesFirstRun {
			buf.WriteString(",")
		} else {
			memSeriesFirstRun = false
		}
		buf.WriteString("{\"name\":\"")
		buf.WriteString(*series.Name)
		buf.WriteString("\",\"columns\":[")
		pointsFirstRun = true

		includeSequenceNumber := true
		if len(series.Points) > 0 && series.Points[0].SequenceNumber == nil {
			includeSequenceNumber = false
		}

		buf.WriteString("\"time\"")
		if includeSequenceNumber {
			buf.WriteString(",\"sequence_number\"")
		}
		for _, field := range series.Fields {
			buf.WriteString(",\"")
			buf.WriteString(field)
			buf.WriteString("\"")
		}
		buf.WriteString("],\"points\":[")

		for _, row := range series.Points {
			if !pointsFirstRun {
				buf.WriteString(",")
			} else {
				pointsFirstRun = false
			}
			buf.WriteString("[")
			timestamp := int64(0)
			if t := row.Timestamp; t != nil {
				timestamp = *row.GetTimestampInMicroseconds()
				switch precision {
				case SecondPrecision:
					timestamp /= 1000
					fallthrough
				case MillisecondPrecision:
					timestamp /= 1000
				}
			}

			buf.WriteString(strconv.FormatInt(timestamp, 10))
			s := uint64(0)
			if includeSequenceNumber {
				if row.SequenceNumber != nil {
					s = row.GetSequenceNumber()
				}
				buf.WriteString(",")
				buf.WriteString(strconv.FormatUint(s, 10))
			}

			for _, value := range row.Values {
				buf.WriteString(",")

				if value == nil {
					buf.WriteString("null")
					continue
				}
				_, ok := value.GetValue()
				if !ok {
					buf.WriteString("null")
					log.Warn("Infinite or NaN value encountered")
					continue
				}

				if value.StringValue != nil {
					buf.WriteString("\"")
					buf.WriteString(*value.StringValue)
					buf.WriteString("\"")
				} else if value.DoubleValue != nil {
					buf.WriteString(strconv.FormatFloat(*value.DoubleValue, 'f', 6, 64))
				} else if value.Int64Value != nil {
					buf.WriteString(strconv.FormatInt(*value.Int64Value, 10))
				} else {
					buf.WriteString("null")
				}
			}
			buf.WriteString("]")
		}

		buf.WriteString("]}")
		if numberOfSeries > 0 && cnt > numberOfSeries {
			break
		}
	}
	if numberOfSeries != 1 {
		buf.WriteString("]")
	}

	return buf.Bytes()
}
