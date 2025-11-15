package iceberg

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestNewDataFileReader(t *testing.T) {
	logger, _ := zap.NewProduction()
	batchSize := 5000

	reader := NewDataFileReader(nil, batchSize, logger)

	assert.NotNil(t, reader)
	assert.Equal(t, batchSize, reader.batchSize)
	assert.Equal(t, logger, reader.logger)
}

func TestDataFileReader_ExtractValue(t *testing.T) {
	logger, _ := zap.NewProduction()
	reader := NewDataFileReader(nil, 1000, logger)

	pool := memory.NewGoAllocator()

	tests := []struct {
		name     string
		buildArr func() arrow.Array
		index    int
		expected interface{}
	}{
		{
			name: "boolean array",
			buildArr: func() arrow.Array {
				builder := array.NewBooleanBuilder(pool)
				defer builder.Release()
				builder.AppendValues([]bool{true, false, true}, nil)
				return builder.NewBooleanArray()
			},
			index:    0,
			expected: true,
		},
		{
			name: "int32 array",
			buildArr: func() arrow.Array {
				builder := array.NewInt32Builder(pool)
				defer builder.Release()
				builder.AppendValues([]int32{10, 20, 30}, nil)
				return builder.NewInt32Array()
			},
			index:    1,
			expected: int32(20),
		},
		{
			name: "int64 array",
			buildArr: func() arrow.Array {
				builder := array.NewInt64Builder(pool)
				defer builder.Release()
				builder.AppendValues([]int64{100, 200, 300}, nil)
				return builder.NewInt64Array()
			},
			index:    2,
			expected: int64(300),
		},
		{
			name: "float32 array",
			buildArr: func() arrow.Array {
				builder := array.NewFloat32Builder(pool)
				defer builder.Release()
				builder.AppendValues([]float32{1.1, 2.2, 3.3}, nil)
				return builder.NewFloat32Array()
			},
			index:    1,
			expected: float32(2.2),
		},
		{
			name: "float64 array",
			buildArr: func() arrow.Array {
				builder := array.NewFloat64Builder(pool)
				defer builder.Release()
				builder.AppendValues([]float64{1.11, 2.22, 3.33}, nil)
				return builder.NewFloat64Array()
			},
			index:    0,
			expected: float64(1.11),
		},
		{
			name: "string array",
			buildArr: func() arrow.Array {
				builder := array.NewStringBuilder(pool)
				defer builder.Release()
				builder.AppendValues([]string{"hello", "world", "test"}, nil)
				return builder.NewStringArray()
			},
			index:    1,
			expected: "world",
		},
		{
			name: "binary array",
			buildArr: func() arrow.Array {
				builder := array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)
				defer builder.Release()
				builder.AppendValues([][]byte{[]byte("data1"), []byte("data2")}, nil)
				return builder.NewBinaryArray()
			},
			index:    0,
			expected: []byte("data1"),
		},
		{
			name: "null value",
			buildArr: func() arrow.Array {
				builder := array.NewInt32Builder(pool)
				defer builder.Release()
				builder.AppendNull()
				builder.Append(20)
				return builder.NewInt32Array()
			},
			index:    0,
			expected: nil,
		},
		{
			name: "date32 array",
			buildArr: func() arrow.Array {
				builder := array.NewDate32Builder(pool)
				defer builder.Release()
				// Date32 represents days since Unix epoch
				builder.AppendValues([]arrow.Date32{0, 100, 1000}, nil)
				return builder.NewDate32Array()
			},
			index:    0,
			expected: time.Unix(0, 0).UTC(),
		},
		{
			name: "timestamp array - seconds",
			buildArr: func() arrow.Array {
				builder := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Second})
				defer builder.Release()
				builder.AppendValues([]arrow.Timestamp{1000, 2000, 3000}, nil)
				return builder.NewTimestampArray()
			},
			index:    1,
			expected: time.Unix(2000, 0).UTC(),
		},
		{
			name: "timestamp array - milliseconds",
			buildArr: func() arrow.Array {
				builder := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Millisecond})
				defer builder.Release()
				builder.AppendValues([]arrow.Timestamp{1000, 2000, 3000}, nil)
				return builder.NewTimestampArray()
			},
			index:    0,
			expected: time.Unix(0, 1000*1e6).UTC(),
		},
		{
			name: "timestamp array - microseconds",
			buildArr: func() arrow.Array {
				builder := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Microsecond})
				defer builder.Release()
				builder.AppendValues([]arrow.Timestamp{1000, 2000, 3000}, nil)
				return builder.NewTimestampArray()
			},
			index:    0,
			expected: time.Unix(0, 1000*1e3).UTC(),
		},
		{
			name: "timestamp array - nanoseconds",
			buildArr: func() arrow.Array {
				builder := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Nanosecond})
				defer builder.Release()
				builder.AppendValues([]arrow.Timestamp{1000, 2000, 3000}, nil)
				return builder.NewTimestampArray()
			},
			index:    0,
			expected: time.Unix(0, 1000).UTC(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			arr := tt.buildArr()
			defer arr.Release()

			result := reader.extractValue(arr, tt.index)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDataFileReader_ExtractValueList(t *testing.T) {
	logger, _ := zap.NewProduction()
	reader := NewDataFileReader(nil, 1000, logger)

	pool := memory.NewGoAllocator()

	// Create a list array
	listBuilder := array.NewListBuilder(pool, arrow.PrimitiveTypes.Int32)
	defer listBuilder.Release()

	valueBuilder := listBuilder.ValueBuilder().(*array.Int32Builder)

	// First list: [1, 2, 3]
	listBuilder.Append(true)
	valueBuilder.Append(1)
	valueBuilder.Append(2)
	valueBuilder.Append(3)

	// Second list: [4, 5]
	listBuilder.Append(true)
	valueBuilder.Append(4)
	valueBuilder.Append(5)

	listArr := listBuilder.NewListArray()
	defer listArr.Release()

	// Test first list
	result := reader.extractValue(listArr, 0)
	assert.NotNil(t, result)
	listResult, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 3, len(listResult))
	assert.Equal(t, int32(1), listResult[0])
	assert.Equal(t, int32(2), listResult[1])
	assert.Equal(t, int32(3), listResult[2])

	// Test second list
	result = reader.extractValue(listArr, 1)
	assert.NotNil(t, result)
	listResult, ok = result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 2, len(listResult))
	assert.Equal(t, int32(4), listResult[0])
	assert.Equal(t, int32(5), listResult[1])
}

func TestDataFileReader_ExtractValueStruct(t *testing.T) {
	logger, _ := zap.NewProduction()
	reader := NewDataFileReader(nil, 1000, logger)

	pool := memory.NewGoAllocator()

	// Create struct type
	structType := arrow.StructOf(
		arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
	)

	structBuilder := array.NewStructBuilder(pool, structType)
	defer structBuilder.Release()

	idBuilder := structBuilder.FieldBuilder(0).(*array.Int32Builder)
	nameBuilder := structBuilder.FieldBuilder(1).(*array.StringBuilder)

	// First struct: {id: 1, name: "Alice"}
	structBuilder.Append(true)
	idBuilder.Append(1)
	nameBuilder.Append("Alice")

	// Second struct: {id: 2, name: "Bob"}
	structBuilder.Append(true)
	idBuilder.Append(2)
	nameBuilder.Append("Bob")

	structArr := structBuilder.NewStructArray()
	defer structArr.Release()

	// Test first struct
	result := reader.extractValue(structArr, 0)
	assert.NotNil(t, result)
	structResult, ok := result.(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, int32(1), structResult["id"])
	assert.Equal(t, "Alice", structResult["name"])

	// Test second struct
	result = reader.extractValue(structArr, 1)
	assert.NotNil(t, result)
	structResult, ok = result.(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, int32(2), structResult["id"])
	assert.Equal(t, "Bob", structResult["name"])
}

func TestDataFileReader_ConvertArrowRecordToRecords(t *testing.T) {
	logger, _ := zap.NewProduction()
	reader := NewDataFileReader(nil, 1000, logger)

	pool := memory.NewGoAllocator()

	// Create schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "value", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	// Build record
	idBuilder := array.NewInt32Builder(pool)
	defer idBuilder.Release()
	idBuilder.AppendValues([]int32{1, 2, 3}, nil)
	idArr := idBuilder.NewInt32Array()
	defer idArr.Release()

	nameBuilder := array.NewStringBuilder(pool)
	defer nameBuilder.Release()
	nameBuilder.AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)
	nameArr := nameBuilder.NewStringArray()
	defer nameArr.Release()

	valueBuilder := array.NewFloat64Builder(pool)
	defer valueBuilder.Release()
	valueBuilder.AppendValues([]float64{1.1, 2.2, 3.3}, nil)
	valueArr := valueBuilder.NewFloat64Array()
	defer valueArr.Release()

	record := array.NewRecord(schema, []arrow.Array{idArr, nameArr, valueArr}, 3)
	defer record.Release()

	// Convert to nebula records
	nebulaRecords, err := reader.convertArrowRecordToRecords(record, schema)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(nebulaRecords))

	// Verify first record
	assert.Equal(t, int32(1), nebulaRecords[0].Data["id"])
	assert.Equal(t, "Alice", nebulaRecords[0].Data["name"])
	assert.Equal(t, float64(1.1), nebulaRecords[0].Data["value"])

	// Verify second record
	assert.Equal(t, int32(2), nebulaRecords[1].Data["id"])
	assert.Equal(t, "Bob", nebulaRecords[1].Data["name"])
	assert.Equal(t, float64(2.2), nebulaRecords[1].Data["value"])

	// Verify third record
	assert.Equal(t, int32(3), nebulaRecords[2].Data["id"])
	assert.Equal(t, "Charlie", nebulaRecords[2].Data["name"])
	assert.Equal(t, float64(3.3), nebulaRecords[2].Data["value"])

	// Clean up
	for _, rec := range nebulaRecords {
		rec.Release()
	}
}

func TestDataFileReader_ConvertArrowRecordToRecordsWithNulls(t *testing.T) {
	logger, _ := zap.NewProduction()
	reader := NewDataFileReader(nil, 1000, logger)

	pool := memory.NewGoAllocator()

	// Create schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32},
			{Name: "name", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	// Build record with nulls
	idBuilder := array.NewInt32Builder(pool)
	defer idBuilder.Release()
	idBuilder.Append(1)
	idBuilder.AppendNull()
	idBuilder.Append(3)
	idArr := idBuilder.NewInt32Array()
	defer idArr.Release()

	nameBuilder := array.NewStringBuilder(pool)
	defer nameBuilder.Release()
	nameBuilder.AppendNull()
	nameBuilder.Append("Bob")
	nameBuilder.Append("Charlie")
	nameArr := nameBuilder.NewStringArray()
	defer nameArr.Release()

	record := array.NewRecord(schema, []arrow.Array{idArr, nameArr}, 3)
	defer record.Release()

	// Convert to nebula records
	nebulaRecords, err := reader.convertArrowRecordToRecords(record, schema)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(nebulaRecords))

	// Verify first record (id=1, name=null)
	assert.Equal(t, int32(1), nebulaRecords[0].Data["id"])
	assert.Nil(t, nebulaRecords[0].Data["name"])

	// Verify second record (id=null, name="Bob")
	assert.Nil(t, nebulaRecords[1].Data["id"])
	assert.Equal(t, "Bob", nebulaRecords[1].Data["name"])

	// Verify third record (id=3, name="Charlie")
	assert.Equal(t, int32(3), nebulaRecords[2].Data["id"])
	assert.Equal(t, "Charlie", nebulaRecords[2].Data["name"])

	// Clean up
	for _, rec := range nebulaRecords {
		rec.Release()
	}
}

func TestDataFileReader_StreamRecordsNotInitialized(t *testing.T) {
	logger, _ := zap.NewProduction()
	_ = NewDataFileReader(nil, 1000, logger)

	// Note: StreamRecords requires a real table with scan capabilities
	// Testing it directly would require integration tests with a real Iceberg table
	t.Skip("StreamRecords requires integration testing with real Iceberg table")
}

func TestDataFileReader_StreamBatchesNotInitialized(t *testing.T) {
	logger, _ := zap.NewProduction()
	_ = NewDataFileReader(nil, 1000, logger)

	// Note: StreamBatches requires a real table with scan capabilities
	// Testing it directly would require integration tests with a real Iceberg table
	t.Skip("StreamBatches requires integration testing with real Iceberg table")
}

// Mock table for testing (minimal implementation)
type mockTable struct {
	*table.Table
}

func TestDataFileReader_BatchSizeConfiguration(t *testing.T) {
	logger, _ := zap.NewProduction()

	tests := []struct {
		name      string
		batchSize int
		expected  int
	}{
		{
			name:      "default batch size",
			batchSize: 1000,
			expected:  1000,
		},
		{
			name:      "custom batch size",
			batchSize: 5000,
			expected:  5000,
		},
		{
			name:      "large batch size",
			batchSize: 100000,
			expected:  100000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewDataFileReader(nil, tt.batchSize, logger)
			assert.Equal(t, tt.expected, reader.batchSize)
		})
	}
}
