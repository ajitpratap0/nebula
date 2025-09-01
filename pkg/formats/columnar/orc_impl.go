// Package columnar provides ORC implementation
package columnar

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/nebulaerrors"
	"github.com/ajitpratap0/nebula/pkg/pool"
	"unsafe"
)

// ORC constants
const (
	orcMagic              = "ORC"
	orcVersion            = 1
	defaultStripeSize     = 64 * 1024 * 1024 // 64MB
	defaultRowIndexStride = 10000
	compressionBlockSize  = 256 * 1024 // 256KB
)

// orcWriter implements Writer for ORC format
type orcWriter struct {
	writer         io.Writer
	config         *WriterConfig
	schema         *core.Schema
	bytesWritten   int64
	recordsWritten int64

	// ORC specific
	stripes       []stripeInfo
	currentStripe *stripe
	footer        *orcFooter
	postscript    *orcPostscript

	// Buffering
	buffer     []byte
	bufferPool sync.Pool

	// Compression
	compressionKind CompressionKind
	compressor      func([]byte) ([]byte, error)

	mu sync.Mutex
}

// ORC data structures
type stripeInfo struct {
	offset       uint64
	indexLength  uint64
	dataLength   uint64
	footerLength uint64
	numberOfRows uint64
}

type stripe struct {
	rowData    [][]interface{}
	indexData  []byte
	rowCount   int
	memorySize int64
}

type orcFooter struct {
	headerLength   uint64
	contentLength  uint64
	stripes        []stripeInfo
	types          []orcType
	numberOfRows   uint64
	statistics     []columnStatistics
	rowIndexStride uint32
}

type orcPostscript struct {
	footerLength    uint64
	compressionKind CompressionKind
	compressionSize uint64
	version         []uint32
	metadataLength  uint64
	writerVersion   uint32
	magic           string
}

type orcType struct {
	kind          TypeKind
	subtypes      []uint32
	fieldNames    []string
	maximumLength uint32
	precision     uint32
	scale         uint32
}

type columnStatistics struct {
	numberOfValues uint64
	hasNull        bool
	bytesOnDisk    uint64

	// Type-specific stats
	intStats    *integerStatistics
	doubleStats *doubleStatistics
	stringStats *stringStatistics
	dateStats   *dateStatistics
	binaryStats *binaryStatistics
	boolStats   *booleanStatistics
}

type integerStatistics struct {
	minimum int64
	maximum int64
	sum     int64
}

type doubleStatistics struct {
	minimum float64
	maximum float64
	sum     float64
}

type stringStatistics struct {
	minimum string
	maximum string
	sum     int64 // total length
}

type dateStatistics struct {
	minimum int32
	maximum int32
}

type binaryStatistics struct {
	sum int64 // total length
}

type booleanStatistics struct {
	trueCount uint64
}

// ORC type kinds
type TypeKind uint32

const (
	BOOLEAN TypeKind = iota
	BYTE
	SHORT
	INT
	LONG
	FLOAT
	DOUBLE
	STRING
	DATE
	VARCHAR
	CHAR
	BINARY
	DECIMAL
	TIMESTAMP
	LIST
	MAP
	STRUCT
	UNION
)

// Compression kinds
type CompressionKind uint32

const (
	NONE CompressionKind = iota
	ZLIB
	SNAPPY
	LZO
	LZ4
	ZSTD
)

func newORCWriter(w io.Writer, config *WriterConfig) (*orcWriter, error) {
	if config.Schema == nil {
		return nil, fmt.Errorf("schema is required for ORC writer")
	}

	ow := &orcWriter{
		writer: w,
		config: config,
		schema: config.Schema,
		footer: &orcFooter{
			rowIndexStride: defaultRowIndexStride,
		},
		postscript: &orcPostscript{
			version:       []uint32{0, 12}, // ORC 0.12
			writerVersion: 1,
			magic:         orcMagic,
		},
		bufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, compressionBlockSize)
			},
		},
	}

	// Setup compression
	if err := ow.setupCompression(config.Compression); err != nil {
		return nil, err
	}

	// Initialize schema types
	if err := ow.initializeTypes(); err != nil {
		return nil, err
	}

	// Start first stripe
	ow.currentStripe = &stripe{
		rowData: make([][]interface{}, 0, config.BatchSize),
	}

	return ow, nil
}

func (ow *orcWriter) setupCompression(compression string) error {
	switch compression {
	case "", "none":
		ow.compressionKind = NONE
		ow.compressor = func(data []byte) ([]byte, error) { return data, nil }
	case "snappy":
		ow.compressionKind = SNAPPY
		ow.compressor = compressSnappy
	case "zlib":
		ow.compressionKind = ZLIB
		ow.compressor = compressZlib
	case "lz4":
		ow.compressionKind = LZ4
		ow.compressor = compressLZ4
	case "zstd":
		ow.compressionKind = ZSTD
		ow.compressor = compressZstd
	default:
		return fmt.Errorf("unsupported compression: %s", compression)
	}
	ow.postscript.compressionKind = ow.compressionKind
	return nil
}

func (ow *orcWriter) initializeTypes() error {
	// Create struct type for the schema
	structType := orcType{
		kind:       STRUCT,
		fieldNames: make([]string, 0, len(ow.schema.Fields)),
		subtypes:   make([]uint32, 0, len(ow.schema.Fields)),
	}

	// Add root struct type
	ow.footer.types = append(ow.footer.types, structType)

	// Add field types
	for i, field := range ow.schema.Fields {
		fieldType := orcType{
			kind: mapFieldType(field.Type),
		}

		ow.footer.types = append(ow.footer.types, fieldType)
		ow.footer.types[0].subtypes = append(ow.footer.types[0].subtypes, uint32(i+1))
		ow.footer.types[0].fieldNames = append(ow.footer.types[0].fieldNames, field.Name)

		// Initialize statistics
		ow.footer.statistics = append(ow.footer.statistics, columnStatistics{})
	}

	return nil
}

func (ow *orcWriter) WriteRecord(record *models.Record) error {
	ow.mu.Lock()
	defer ow.mu.Unlock()

	// Convert record to row data
	rowData := make([]interface{}, len(ow.schema.Fields))
	for i, field := range ow.schema.Fields {
		value, exists := record.Data[field.Name]
		if !exists || value == nil {
			rowData[i] = nil
		} else {
			rowData[i] = convertValue(value, field.Type)
		}
	}

	// Add to current stripe
	ow.currentStripe.rowData = append(ow.currentStripe.rowData, rowData)
	ow.currentStripe.rowCount++
	ow.currentStripe.memorySize += estimateRowSize(rowData)

	// Check if we need to flush stripe
	if ow.currentStripe.memorySize >= defaultStripeSize {
		if err := ow.flushStripe(); err != nil {
			return err
		}
	}

	ow.recordsWritten++
	return nil
}

func (ow *orcWriter) WriteRecords(records []*models.Record) error {
	for _, record := range records {
		if err := ow.WriteRecord(record); err != nil {
			return err
		}
	}
	return nil
}

func (ow *orcWriter) flushStripe() error {
	if ow.currentStripe.rowCount == 0 {
		return nil
	}

	stripeInfo := stripeInfo{
		offset:       uint64(ow.bytesWritten),
		numberOfRows: uint64(ow.currentStripe.rowCount),
	}

	// Write row index
	indexData, err := ow.writeRowIndex()
	if err != nil {
		return err
	}
	stripeInfo.indexLength = uint64(len(indexData))

	// Write row data
	dataLength, err := ow.writeRowData()
	if err != nil {
		return err
	}
	stripeInfo.dataLength = uint64(dataLength)

	// Write stripe footer
	footerLength, err := ow.writeStripeFooter()
	if err != nil {
		return err
	}
	stripeInfo.footerLength = uint64(footerLength)

	// Update footer
	ow.stripes = append(ow.stripes, stripeInfo)
	ow.footer.stripes = ow.stripes
	ow.footer.numberOfRows += stripeInfo.numberOfRows

	// Reset current stripe
	ow.currentStripe = &stripe{
		rowData: make([][]interface{}, 0, ow.config.BatchSize),
	}

	return nil
}

func (ow *orcWriter) writeRowIndex() ([]byte, error) {
	// Simplified row index - in production, this would include bloom filters and statistics
	indexData := pool.GetByteSlice()
	if cap(indexData) < 1024 {
		indexData = make([]byte, 0, 1024)
	}

	defer pool.PutByteSlice(indexData)

	// Write placeholder index
	for range ow.schema.Fields {
		// Position list for each column
		positions := make([]uint64, 0)
		for j := 0; j < ow.currentStripe.rowCount; j += int(ow.footer.rowIndexStride) {
			positions = append(positions, uint64(j))
		}

		// Encode positions (simplified)
		for _, pos := range positions {
			indexData = binary.AppendUvarint(indexData, pos)
		}
	}

	// Compress index data
	compressed, err := ow.compressor(indexData)
	if err != nil {
		return nil, err
	}

	// Write to output
	if _, err := ow.writer.Write(compressed); err != nil {
		return nil, err
	}
	ow.bytesWritten += int64(len(compressed))

	return compressed, nil
}

func (ow *orcWriter) writeRowData() (int, error) {
	totalBytes := 0

	// Write each column separately (columnar storage)
	for colIdx, field := range ow.schema.Fields {
		columnData := make([]interface{}, ow.currentStripe.rowCount)

		// Extract column data
		for rowIdx, row := range ow.currentStripe.rowData {
			columnData[rowIdx] = row[colIdx]
		}

		// Encode column based on type
		encoded, err := ow.encodeColumn(columnData, field.Type)
		if err != nil {
			return 0, err
		}

		// Compress column data
		compressed, err := ow.compressor(encoded)
		if err != nil {
			return 0, err
		}

		// Write to output
		if _, err := ow.writer.Write(compressed); err != nil {
			return 0, err
		}

		totalBytes += len(compressed)
		ow.bytesWritten += int64(len(compressed))

		// Update statistics
		ow.updateColumnStatistics(colIdx, columnData)
	}

	return totalBytes, nil
}

func (ow *orcWriter) encodeColumn(data []interface{}, fieldType core.FieldType) ([]byte, error) {
	buffer := pool.GlobalBufferPool.Get(len(data) * 16)
	defer pool.GlobalBufferPool.Put(buffer)

	encoded := buffer[:0]

	switch fieldType {
	case core.FieldTypeString:
		// Direct encoding for strings
		for _, v := range data {
			if v == nil {
				encoded = append(encoded, 0) // null indicator
			} else {
				str := v.(string)
				encoded = append(encoded, 1) // not null
				encoded = binary.AppendUvarint(encoded, uint64(len(str)))
				encoded = append(encoded, []byte(str)...)
			}
		}

	case core.FieldTypeInt:
		// RLE encoding for integers
		for _, v := range data {
			if v == nil {
				encoded = binary.AppendVarint(encoded, 0)
			} else {
				encoded = binary.AppendVarint(encoded, v.(int64))
			}
		}

	case core.FieldTypeFloat:
		// Direct encoding for floats
		for _, v := range data {
			if v == nil {
				encoded = binary.LittleEndian.AppendUint64(encoded, 0)
			} else {
				encoded = binary.LittleEndian.AppendUint64(encoded,
					uint64(float64ToUint64(v.(float64))))
			}
		}

	case core.FieldTypeBool:
		// Bit packing for booleans
		bytes := (len(data) + 7) / 8
		boolBytes := pool.GetByteSlice()

		if cap(boolBytes) < bytes {

			boolBytes = make([]byte, bytes)

		}

		defer pool.PutByteSlice(boolBytes)
		for i, v := range data {
			if v != nil && v.(bool) {
				boolBytes[i/8] |= 1 << (uint(i) % 8)
			}
		}
		encoded = append(encoded, boolBytes...)

	case core.FieldTypeTimestamp:
		// Seconds + nanoseconds encoding
		for _, v := range data {
			if v == nil {
				encoded = binary.AppendVarint(encoded, 0)
				encoded = binary.AppendVarint(encoded, 0)
			} else {
				t := v.(time.Time)
				encoded = binary.AppendVarint(encoded, t.Unix())
				encoded = binary.AppendVarint(encoded, int64(t.Nanosecond()))
			}
		}

	default:
		// Generic encoding
		for _, v := range data {
			if v == nil {
				encoded = append(encoded, 0)
			} else {
				encoded = append(encoded, 1)
				// Convert to string as fallback
				str := fmt.Sprintf("%v", v)
				encoded = binary.AppendUvarint(encoded, uint64(len(str)))
				encoded = append(encoded, []byte(str)...)
			}
		}
	}

	// Return a copy to avoid buffer reuse issues
	result := pool.GetByteSlice()
	if cap(result) < len(encoded) {
		result = make([]byte, len(encoded))
	}
	defer pool.PutByteSlice(result)
	copy(result, encoded)
	return result, nil
}

func (ow *orcWriter) writeStripeFooter() (int, error) {
	// Create stripe footer with stream information
	footer := pool.GetByteSlice()
	if cap(footer) < 1024 {
		footer = make([]byte, 0, 1024)

	}

	defer pool.PutByteSlice(footer)

	// Write stream count
	footer = binary.AppendUvarint(footer, uint64(len(ow.schema.Fields)))

	// Write stream information for each column
	for i := range ow.schema.Fields {
		// Stream kind (DATA)
		footer = binary.AppendUvarint(footer, 0)
		// Column ID
		footer = binary.AppendUvarint(footer, uint64(i))
		// Length (placeholder)
		footer = binary.AppendUvarint(footer, 0)
	}

	// Write to output
	if _, err := ow.writer.Write(footer); err != nil {
		return 0, err
	}
	ow.bytesWritten += int64(len(footer))

	return len(footer), nil
}

func (ow *orcWriter) updateColumnStatistics(colIdx int, data []interface{}) {
	stats := &ow.footer.statistics[colIdx]
	stats.numberOfValues = uint64(len(data))

	// Update type-specific statistics
	switch ow.schema.Fields[colIdx].Type {
	case core.FieldTypeInt:
		if stats.intStats == nil {
			stats.intStats = &integerStatistics{
				minimum: int64(^uint64(0) >> 1),    // MaxInt64
				maximum: -int64(^uint64(0)>>1) - 1, // MinInt64
			}
		}
		for _, v := range data {
			if v != nil {
				val := v.(int64)
				if val < stats.intStats.minimum {
					stats.intStats.minimum = val
				}
				if val > stats.intStats.maximum {
					stats.intStats.maximum = val
				}
				stats.intStats.sum += val
			} else {
				stats.hasNull = true
			}
		}

	case core.FieldTypeString:
		if stats.stringStats == nil {
			stats.stringStats = &stringStatistics{}
		}
		for _, v := range data {
			if v != nil {
				str := v.(string)
				if stats.stringStats.minimum == "" || str < stats.stringStats.minimum {
					stats.stringStats.minimum = str
				}
				if stats.stringStats.maximum == "" || str > stats.stringStats.maximum {
					stats.stringStats.maximum = str
				}
				stats.stringStats.sum += int64(len(str))
			} else {
				stats.hasNull = true
			}
		}
	}
}

func (ow *orcWriter) Flush() error {
	ow.mu.Lock()
	defer ow.mu.Unlock()

	// Flush current stripe
	return ow.flushStripe()
}

func (ow *orcWriter) Close() error {
	// Flush any remaining data
	if err := ow.Flush(); err != nil {
		return err
	}

	// Write file footer
	footerBytes, err := ow.encodeFooter()
	if err != nil {
		return err
	}

	if _, err := ow.writer.Write(footerBytes); err != nil {
		return err
	}
	ow.bytesWritten += int64(len(footerBytes))

	// Write postscript
	postscriptBytes, err := ow.encodePostscript(uint64(len(footerBytes)))
	if err != nil {
		return err
	}

	if _, err := ow.writer.Write(postscriptBytes); err != nil {
		return err
	}
	ow.bytesWritten += int64(len(postscriptBytes))

	// Write postscript length (1 byte)
	if _, err := ow.writer.Write([]byte{byte(len(postscriptBytes))}); err != nil {
		return err
	}
	ow.bytesWritten++

	// Write magic string
	if _, err := ow.writer.Write([]byte(orcMagic)); err != nil {
		return err
	}
	ow.bytesWritten += int64(len(orcMagic))

	return nil
}

func (ow *orcWriter) encodeFooter() ([]byte, error) {
	// In a real implementation, this would use Protocol Buffers
	// For now, using a simplified binary encoding
	footer := pool.GetByteSlice()
	if cap(footer) < 4096 {
		footer = make([]byte, 0, 4096)
	}
	defer pool.PutByteSlice(footer)

	// Write header length
	footer = binary.AppendUvarint(footer, ow.footer.headerLength)

	// Write content length
	footer = binary.AppendUvarint(footer, ow.footer.contentLength)

	// Write stripes
	footer = binary.AppendUvarint(footer, uint64(len(ow.footer.stripes)))
	for _, stripe := range ow.footer.stripes {
		footer = binary.AppendUvarint(footer, stripe.offset)
		footer = binary.AppendUvarint(footer, stripe.indexLength)
		footer = binary.AppendUvarint(footer, stripe.dataLength)
		footer = binary.AppendUvarint(footer, stripe.footerLength)
		footer = binary.AppendUvarint(footer, stripe.numberOfRows)
	}

	// Write types
	footer = binary.AppendUvarint(footer, uint64(len(ow.footer.types)))
	for _, t := range ow.footer.types {
		footer = binary.AppendUvarint(footer, uint64(t.kind))
		footer = binary.AppendUvarint(footer, uint64(len(t.subtypes)))
		for _, subtype := range t.subtypes {
			footer = binary.AppendUvarint(footer, uint64(subtype))
		}
		footer = binary.AppendUvarint(footer, uint64(len(t.fieldNames)))
		for _, name := range t.fieldNames {
			footer = binary.AppendUvarint(footer, uint64(len(name)))
			footer = append(footer, []byte(name)...)
		}
	}

	// Write statistics (simplified)
	footer = binary.AppendUvarint(footer, uint64(len(ow.footer.statistics)))
	for _, stat := range ow.footer.statistics {
		footer = binary.AppendUvarint(footer, stat.numberOfValues)
		if stat.hasNull {
			footer = append(footer, 1)
		} else {
			footer = append(footer, 0)
		}
	}

	// Write metadata
	footer = binary.AppendUvarint(footer, ow.footer.numberOfRows)
	footer = binary.AppendUvarint(footer, uint64(ow.footer.rowIndexStride))

	// Compress footer if needed
	if ow.compressionKind != NONE {
		compressed, err := ow.compressor(footer)
		if err != nil {
			return nil, err
		}
		return compressed, nil
	}

	return footer, nil
}

func (ow *orcWriter) encodePostscript(footerLength uint64) ([]byte, error) {
	ps := pool.GetByteSlice()
	if cap(ps) < 64 {
		ps = make([]byte, 0, 64)
	}
	defer pool.PutByteSlice(ps)

	// Footer length
	ps = binary.AppendUvarint(ps, footerLength)

	// Compression
	ps = append(ps, byte(ow.postscript.compressionKind))

	// Compression block size
	ps = binary.AppendUvarint(ps, compressionBlockSize)

	// Version
	ps = append(ps, byte(len(ow.postscript.version)))
	for _, v := range ow.postscript.version {
		ps = binary.AppendUvarint(ps, uint64(v))
	}

	// Metadata length
	ps = binary.AppendUvarint(ps, ow.postscript.metadataLength)

	// Writer version
	ps = append(ps, byte(ow.postscript.writerVersion))

	return ps, nil
}

func (ow *orcWriter) Format() Format {
	return ORC
}

func (ow *orcWriter) BytesWritten() int64 {
	return ow.bytesWritten
}

func (ow *orcWriter) RecordsWritten() int64 {
	return ow.recordsWritten
}

// orcReader implements Reader for ORC format
type orcReader struct {
	reader     io.Reader
	config     *ReaderConfig
	schema     *core.Schema
	footer     *orcFooter
	postscript *orcPostscript

	// Current reading state
	currentStripe int
	currentRow    int
	stripeData    [][]interface{}

	// Decompression
	decompressor func([]byte) ([]byte, error)
}

func newORCReader(r io.Reader, config *ReaderConfig) (*orcReader, error) {
	or := &orcReader{
		reader: r,
		config: config,
	}

	// Read file metadata
	if err := or.readFileMetadata(); err != nil {
		return nil, err
	}

	// Setup decompression
	if err := or.setupDecompression(); err != nil {
		return nil, err
	}

	// Build schema from footer
	if err := or.buildSchema(); err != nil {
		return nil, err
	}

	return or, nil
}

func (or *orcReader) readFileMetadata() error {
	// In a real implementation, we would:
	// 1. Seek to end of file
	// 2. Read magic string
	// 3. Read postscript length
	// 4. Read postscript
	// 5. Read footer

	// For now, return a placeholder
	or.footer = &orcFooter{
		numberOfRows: 0,
		types:        []orcType{},
		stripes:      []stripeInfo{},
		statistics:   []columnStatistics{},
	}

	or.postscript = &orcPostscript{
		compressionKind: NONE,
		version:         []uint32{0, 12},
		magic:           orcMagic,
	}

	return nil
}

func (or *orcReader) setupDecompression() error {
	switch or.postscript.compressionKind {
	case NONE:
		or.decompressor = func(data []byte) ([]byte, error) { return data, nil }
	case SNAPPY:
		or.decompressor = decompressSnappy
	case ZLIB:
		or.decompressor = decompressZlib
	case LZ4:
		or.decompressor = decompressLZ4
	case ZSTD:
		or.decompressor = decompressZstd
	default:
		return fmt.Errorf("unsupported compression: %v", or.postscript.compressionKind)
	}
	return nil
}

func (or *orcReader) buildSchema() error {
	if len(or.footer.types) == 0 {
		return nebulaerrors.New(nebulaerrors.ErrorTypeData, "no types found in ORC file")
	}

	// First type should be struct
	rootType := or.footer.types[0]
	if rootType.kind != STRUCT {
		return nebulaerrors.New(nebulaerrors.ErrorTypeData, "root type is not STRUCT")
	}

	fields := make([]core.Field, 0, len(rootType.fieldNames))
	for i, fieldName := range rootType.fieldNames {
		if i < len(rootType.subtypes) {
			fieldTypeIdx := rootType.subtypes[i]
			if int(fieldTypeIdx) < len(or.footer.types) {
				fieldType := or.footer.types[fieldTypeIdx]
				fields = append(fields, core.Field{
					Name: fieldName,
					Type: unmapFieldType(fieldType.kind),
				})
			}
		}
	}

	or.schema = &core.Schema{
		Name:   "orc_schema",
		Fields: fields,
	}

	return nil
}

func (or *orcReader) ReadRecords() ([]*models.Record, error) {
	records := make([]*models.Record, 0)

	for or.HasNext() {
		record, err := or.Next()
		if err != nil {
			return records, err
		}
		if record != nil {
			records = append(records, record)
		}
	}

	return records, nil
}

func (or *orcReader) Next() (*models.Record, error) {
	// Check if we need to load next stripe
	if or.stripeData == nil || or.currentRow >= len(or.stripeData) {
		if err := or.loadNextStripe(); err != nil {
			if err == io.EOF {
				return nil, nil
			}
			return nil, err
		}
	}

	if or.currentRow >= len(or.stripeData) {
		return nil, nil
	}

	// Create record from current row
	record := pool.GetRecord()
	rowData := or.stripeData[or.currentRow]

	for i, field := range or.schema.Fields {
		if i < len(rowData) && rowData[i] != nil {
			record.SetData(field.Name, rowData[i])
		}
	}

	or.currentRow++
	return record, nil
}

func (or *orcReader) loadNextStripe() error {
	if or.currentStripe >= len(or.footer.stripes) {
		return io.EOF
	}

	// In a real implementation, we would:
	// 1. Seek to stripe offset
	// 2. Read stripe data
	// 3. Decompress
	// 4. Decode columns

	// For now, return empty data
	or.stripeData = make([][]interface{}, 0)
	or.currentRow = 0
	or.currentStripe++

	return nil
}

func (or *orcReader) HasNext() bool {
	return or.currentStripe < len(or.footer.stripes) ||
		(or.stripeData != nil && or.currentRow < len(or.stripeData))
}

func (or *orcReader) Close() error {
	// Clean up resources
	or.stripeData = nil
	return nil
}

func (or *orcReader) Format() Format {
	return ORC
}

func (or *orcReader) Schema() (*core.Schema, error) {
	return or.schema, nil
}

// Helper functions

func mapFieldType(fieldType core.FieldType) TypeKind {
	switch fieldType {
	case core.FieldTypeString:
		return STRING
	case core.FieldTypeInt:
		return LONG
	case core.FieldTypeFloat:
		return DOUBLE
	case core.FieldTypeBool:
		return BOOLEAN
	case core.FieldTypeTimestamp:
		return TIMESTAMP
	case core.FieldTypeDate:
		return DATE
	case core.FieldTypeBinary:
		return BINARY
	case core.FieldTypeJSON:
		return STRING // Store JSON as string
	default:
		return STRING
	}
}

func unmapFieldType(kind TypeKind) core.FieldType {
	switch kind {
	case BOOLEAN:
		return core.FieldTypeBool
	case BYTE, SHORT, INT, LONG:
		return core.FieldTypeInt
	case FLOAT, DOUBLE:
		return core.FieldTypeFloat
	case STRING, VARCHAR, CHAR:
		return core.FieldTypeString
	case DATE:
		return core.FieldTypeDate
	case TIMESTAMP:
		return core.FieldTypeTimestamp
	case BINARY:
		return core.FieldTypeBinary
	default:
		return core.FieldTypeString
	}
}

func convertValue(value interface{}, fieldType core.FieldType) interface{} {
	if value == nil {
		return nil
	}

	switch fieldType {
	case core.FieldTypeInt:
		switch v := value.(type) {
		case int:
			return int64(v)
		case int32:
			return int64(v)
		case int64:
			return v
		case float64:
			return int64(v)
		default:
			return int64(0)
		}
	case core.FieldTypeFloat:
		switch v := value.(type) {
		case float32:
			return float64(v)
		case float64:
			return v
		case int:
			return float64(v)
		case int64:
			return float64(v)
		default:
			return float64(0)
		}
	case core.FieldTypeString:
		return fmt.Sprintf("%v", value)
	case core.FieldTypeBool:
		switch v := value.(type) {
		case bool:
			return v
		default:
			return false
		}
	case core.FieldTypeTimestamp:
		switch v := value.(type) {
		case time.Time:
			return v
		case int64:
			return time.Unix(v, 0)
		default:
			return time.Time{}
		}
	default:
		return value
	}
}

func estimateRowSize(row []interface{}) int64 {
	size := int64(0)
	for _, value := range row {
		if value == nil {
			size += 1
			continue
		}

		switch v := value.(type) {
		case string:
			size += int64(len(v)) + 8 // string + length
		case []byte:
			size += int64(len(v)) + 8 // bytes + length
		case int, int32, int64, uint, uint32, uint64:
			size += 8
		case float32, float64:
			size += 8
		case bool:
			size += 1
		case time.Time:
			size += 16 // timestamp
		default:
			size += 8 // default estimate
		}
	}
	return size
}

func float64ToUint64(f float64) uint64 {
	return *(*uint64)(unsafe.Pointer(&f))
}

// Compression functions (placeholders - would use actual libraries)

func compressSnappy(data []byte) ([]byte, error) {
	// Use klauspost/compress/snappy
	return data, nil
}

func decompressSnappy(data []byte) ([]byte, error) {
	// Use klauspost/compress/snappy
	return data, nil
}

func compressZlib(data []byte) ([]byte, error) {
	// Use compress/zlib
	return data, nil
}

func decompressZlib(data []byte) ([]byte, error) {
	// Use compress/zlib
	return data, nil
}

func compressLZ4(data []byte) ([]byte, error) {
	// Use pierrec/lz4
	return data, nil
}

func decompressLZ4(data []byte) ([]byte, error) {
	// Use pierrec/lz4
	return data, nil
}

func compressZstd(data []byte) ([]byte, error) {
	// Use klauspost/compress/zstd
	return data, nil
}

func decompressZstd(data []byte) ([]byte, error) {
	// Use klauspost/compress/zstd
	return data, nil
}
