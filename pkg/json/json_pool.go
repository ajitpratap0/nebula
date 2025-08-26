// Package json provides high-performance JSON serialization with object pooling
package json

import (
	"bytes"
	"io"
	"sync"

	"github.com/ajitpratap0/nebula/pkg/pool"
	gojson "github.com/goccy/go-json"
)

// JSONPool manages pooled JSON encoders and decoders
type JSONPool struct {
	encoderPool sync.Pool
	decoderPool sync.Pool
	bufferPool  sync.Pool
}

// Global JSON pool instance
var globalPool = &JSONPool{
	encoderPool: sync.Pool{
		New: func() interface{} {
			return &pooledEncoder{
				buffer: bytes.NewBuffer(make([]byte, 0, 4096)),
			}
		},
	},
	decoderPool: sync.Pool{
		New: func() interface{} {
			return &pooledDecoder{}
		},
	},
	bufferPool: sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4096))
		},
	},
}

// pooledEncoder wraps a JSON encoder with a reusable buffer
type pooledEncoder struct {
	encoder *gojson.Encoder
	buffer  *bytes.Buffer
}

// pooledDecoder wraps a JSON decoder
type pooledDecoder struct {
	decoder *gojson.Decoder
}

// GetEncoder gets a pooled JSON encoder
func GetEncoder(w io.Writer) *gojson.Encoder {
	pe := globalPool.encoderPool.Get().(*pooledEncoder)
	pe.buffer.Reset()

	// Always create a new encoder with the specified writer
	pe.encoder = gojson.NewEncoder(w)

	// Configure for performance
	pe.encoder.SetEscapeHTML(false)

	return pe.encoder
}

// PutEncoder returns an encoder to the pool
func PutEncoder(enc *gojson.Encoder) {
	// Find the pooled encoder wrapper
	pe := &pooledEncoder{
		encoder: enc,
		buffer:  bytes.NewBuffer(make([]byte, 0, 4096)),
	}
	globalPool.encoderPool.Put(pe)
}

// GetDecoder gets a pooled JSON decoder
func GetDecoder(r io.Reader) *gojson.Decoder {
	pd := globalPool.decoderPool.Get().(*pooledDecoder)

	// Always create a new decoder with the specified reader
	pd.decoder = gojson.NewDecoder(r)

	// Configure for performance
	pd.decoder.UseNumber()

	return pd.decoder
}

// PutDecoder returns a decoder to the pool
func PutDecoder(dec *gojson.Decoder) {
	pd := &pooledDecoder{
		decoder: dec,
	}
	globalPool.decoderPool.Put(pd)
}

// GetBuffer gets a pooled bytes.Buffer
func GetBuffer() *bytes.Buffer {
	buf := globalPool.bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

// PutBuffer returns a buffer to the pool
func PutBuffer(buf *bytes.Buffer) {
	if buf.Cap() > 1024*1024 { // Don't pool very large buffers
		return
	}
	globalPool.bufferPool.Put(buf)
}

// Marshal is a high-performance drop-in replacement for json.Marshal
func Marshal(v interface{}) ([]byte, error) {
	// Use goccy/go-json for better performance
	return gojson.Marshal(v)
}

// Unmarshal is a high-performance drop-in replacement for json.Unmarshal
func Unmarshal(data []byte, v interface{}) error {
	// Use goccy/go-json for better performance
	return gojson.Unmarshal(data, v)
}

// MarshalIndent is a high-performance replacement for json.MarshalIndent
func MarshalIndent(v interface{}, prefix, indent string) ([]byte, error) {
	return gojson.MarshalIndent(v, prefix, indent)
}

// MarshalToWriter marshals v directly to a writer using pooled encoder
func MarshalToWriter(w io.Writer, v interface{}) error {
	enc := GetEncoder(w)
	defer PutEncoder(enc)

	return enc.Encode(v)
}

// MarshalToBuffer marshals v to a pooled buffer
func MarshalToBuffer(v interface{}) (*bytes.Buffer, error) {
	buf := GetBuffer()

	enc := GetEncoder(buf)
	defer PutEncoder(enc)

	if err := enc.Encode(v); err != nil {
		PutBuffer(buf)
		return nil, err
	}

	return buf, nil
}

// MarshalMultiple efficiently marshals multiple objects to a single buffer
func MarshalMultiple(values []interface{}, separator []byte) ([]byte, error) {
	buf := GetBuffer()
	defer PutBuffer(buf)

	enc := GetEncoder(buf)
	defer PutEncoder(enc)

	for i, v := range values {
		if i > 0 && separator != nil {
			buf.Write(separator)
		}

		if err := enc.Encode(v); err != nil {
			return nil, err
		}

		// Remove trailing newline added by Encode
		if buf.Len() > 0 && buf.Bytes()[buf.Len()-1] == '\n' {
			buf.Truncate(buf.Len() - 1)
		}
	}

	// Create a copy since we're returning the buffer to the pool
	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())

	return result, nil
}

// MarshalArray efficiently marshals a slice as a JSON array
func MarshalArray(values []interface{}) ([]byte, error) {
	if len(values) == 0 {
		return []byte("[]"), nil
	}

	buf := GetBuffer()
	defer PutBuffer(buf)

	buf.WriteByte('[')

	enc := GetEncoder(buf)
	defer PutEncoder(enc)

	for i, v := range values {
		if i > 0 {
			buf.WriteByte(',')
		}

		data, err := gojson.Marshal(v)
		if err != nil {
			return nil, err
		}
		buf.Write(data)
	}

	buf.WriteByte(']')

	// Create a copy since we're returning the buffer to the pool
	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())

	return result, nil
}

// StreamingEncoder provides efficient streaming JSON encoding
type StreamingEncoder struct {
	writer      io.Writer
	encoder     *gojson.Encoder
	firstRecord bool
	isArray     bool
	pretty      bool
	indent      string
}

// NewStreamingEncoder creates a new streaming encoder
func NewStreamingEncoder(w io.Writer, isArray bool) *StreamingEncoder {
	enc := GetEncoder(w)

	se := &StreamingEncoder{
		writer:      w,
		encoder:     enc,
		firstRecord: true,
		isArray:     isArray,
	}

	if isArray {
		w.Write([]byte{'['})
	}

	return se
}

// SetPretty enables pretty printing
func (se *StreamingEncoder) SetPretty(pretty bool, indent string) {
	se.pretty = pretty
	se.indent = indent
	if pretty {
		se.encoder.SetIndent("", indent)
	}
}

// Encode encodes a single value
func (se *StreamingEncoder) Encode(v interface{}) error {
	if se.isArray {
		if !se.firstRecord {
			se.writer.Write([]byte{','})
			if se.pretty {
				se.writer.Write([]byte{'\n'})
			}
		}
		se.firstRecord = false
	}

	if err := se.encoder.Encode(v); err != nil {
		return err
	}

	// For line-delimited JSON, the encoder adds a newline automatically
	// For array format, we handle separators manually above

	return nil
}

// Close finalizes the encoding
func (se *StreamingEncoder) Close() error {
	if se.isArray {
		if se.pretty {
			se.writer.Write([]byte{'\n'})
		}
		se.writer.Write([]byte{']'})
	}

	PutEncoder(se.encoder)
	return nil
}

// OptimizedJSONWriter provides high-performance JSON writing with minimal allocations
type OptimizedJSONWriter struct {
	buffer     []byte
	bytePool   *pool.Pool[[]byte]
	escapeHTML bool
}

// NewOptimizedJSONWriter creates a new optimized JSON writer
func NewOptimizedJSONWriter(initialSize int) *OptimizedJSONWriter {
	return &OptimizedJSONWriter{
		buffer:     make([]byte, 0, initialSize),
		escapeHTML: false,
	}
}

// WriteField efficiently writes a JSON field
func (w *OptimizedJSONWriter) WriteField(key string, value interface{}) error {
	// This is a simplified version - in production you'd implement
	// full JSON serialization with proper escaping

	if len(w.buffer) > 0 {
		w.buffer = append(w.buffer, ',')
	}

	// Write key
	w.buffer = append(w.buffer, '"')
	w.buffer = append(w.buffer, key...)
	w.buffer = append(w.buffer, '"', ':')

	// Write value (simplified - would need full type handling)
	data, err := gojson.Marshal(value)
	if err != nil {
		return err
	}
	w.buffer = append(w.buffer, data...)

	return nil
}

// Bytes returns the accumulated JSON bytes
func (w *OptimizedJSONWriter) Bytes() []byte {
	return w.buffer
}

// Reset resets the writer for reuse
func (w *OptimizedJSONWriter) Reset() {
	w.buffer = w.buffer[:0]
}

// MarshalRecords efficiently marshals a slice of records
func MarshalRecords(records []*pool.Record, format string) ([]byte, error) {
	switch format {
	case "array":
		return MarshalRecordsArray(records)
	case "lines", "jsonl":
		return MarshalRecordsLines(records)
	default:
		return MarshalRecordsArray(records)
	}
}

// MarshalRecordsArray marshals records as a JSON array
func MarshalRecordsArray(records []*pool.Record) ([]byte, error) {
	if len(records) == 0 {
		return []byte("[]"), nil
	}

	// Pre-allocate buffer with estimated size
	estimatedSize := len(records) * 200 // Estimate 200 bytes per record
	buf := pool.GlobalBufferPool.Get(estimatedSize)
	defer pool.GlobalBufferPool.Put(buf)

	buf[0] = '['
	written := 1

	for i, record := range records {
		if i > 0 {
			buf[written] = ','
			written++
		}

		// Marshal record data
		data, err := gojson.Marshal(record.Data)
		if err != nil {
			return nil, err
		}

		// Ensure buffer has space
		if written+len(data) > len(buf) {
			newBuf := make([]byte, written, (written+len(data))*2)
			copy(newBuf, buf[:written])
			pool.GlobalBufferPool.Put(buf)
			buf = newBuf
		}

		copy(buf[written:], data)
		written += len(data)
	}

	buf[written] = ']'
	written++

	// Create result copy
	result := make([]byte, written)
	copy(result, buf[:written])

	return result, nil
}

// MarshalRecordsLines marshals records as line-delimited JSON
func MarshalRecordsLines(records []*pool.Record) ([]byte, error) {
	// Pre-allocate buffer with estimated size
	estimatedSize := len(records) * 200 // Estimate 200 bytes per record
	buf := pool.GlobalBufferPool.Get(estimatedSize)
	defer pool.GlobalBufferPool.Put(buf)

	written := 0

	for _, record := range records {
		// Marshal record data
		data, err := gojson.Marshal(record.Data)
		if err != nil {
			return nil, err
		}

		// Ensure buffer has space
		if written+len(data)+1 > len(buf) {
			newBuf := make([]byte, written, (written+len(data)+1)*2)
			copy(newBuf, buf[:written])
			pool.GlobalBufferPool.Put(buf)
			buf = newBuf
		}

		copy(buf[written:], data)
		written += len(data)

		buf[written] = '\n'
		written++
	}

	// Create result copy
	result := make([]byte, written)
	copy(result, buf[:written])

	return result, nil
}
