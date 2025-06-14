package compression

import (
	"bytes"
	"testing"
)

func TestLZ4Compressor(t *testing.T) {
	config := &Config{
		Algorithm:  LZ4,
		Level:      Default,
		BufferSize: 64 * 1024,
	}

	compressor, err := NewCompressor(config)
	if err != nil {
		t.Fatalf("Failed to create LZ4 compressor: %v", err)
	}

	// Test data
	original := []byte("This is a test string that will be compressed and decompressed using LZ4. " +
		"It contains some repetitive content content content to improve compression ratio.")

	// Test Compress/Decompress
	compressed, err := compressor.Compress(original)
	if err != nil {
		t.Fatalf("Failed to compress: %v", err)
	}

	decompressed, err := compressor.Decompress(compressed)
	if err != nil {
		t.Fatalf("Failed to decompress: %v", err)
	}

	if !bytes.Equal(original, decompressed) {
		t.Errorf("Decompressed data doesn't match original.\nOriginal: %s\nDecompressed: %s",
			string(original), string(decompressed))
	}

	// Verify compression actually occurred
	if len(compressed) >= len(original) {
		t.Logf("Warning: Compressed size (%d) is not smaller than original (%d)",
			len(compressed), len(original))
	}

	t.Logf("LZ4 Compression successful. Original: %d bytes, Compressed: %d bytes, Ratio: %.2f%%",
		len(original), len(compressed), float64(len(compressed))/float64(len(original))*100)

	// Test CompressStream/DecompressStream
	var compressedBuf bytes.Buffer
	err = compressor.CompressStream(&compressedBuf, bytes.NewReader(original))
	if err != nil {
		t.Fatalf("Failed to compress stream: %v", err)
	}

	var decompressedBuf bytes.Buffer
	err = compressor.DecompressStream(&decompressedBuf, &compressedBuf)
	if err != nil {
		t.Fatalf("Failed to decompress stream: %v", err)
	}

	if !bytes.Equal(original, decompressedBuf.Bytes()) {
		t.Errorf("Stream decompressed data doesn't match original")
	}
}

func TestLZ4CompressionLevels(t *testing.T) {
	levels := []Level{Fastest, Default, Better, Best}
	testData := bytes.Repeat([]byte("test data for compression "), 100)

	for _, level := range levels {
		t.Run(level.String(), func(t *testing.T) {
			config := &Config{
				Algorithm: LZ4,
				Level:     level,
			}

			compressor, err := NewCompressor(config)
			if err != nil {
				t.Fatalf("Failed to create compressor: %v", err)
			}

			compressed, err := compressor.Compress(testData)
			if err != nil {
				t.Fatalf("Failed to compress: %v", err)
			}

			decompressed, err := compressor.Decompress(compressed)
			if err != nil {
				t.Fatalf("Failed to decompress: %v", err)
			}

			if !bytes.Equal(testData, decompressed) {
				t.Errorf("Decompressed data doesn't match original for level %v", level)
			}

			t.Logf("Level %v: Original: %d bytes, Compressed: %d bytes, Ratio: %.2f%%",
				level, len(testData), len(compressed),
				float64(len(compressed))/float64(len(testData))*100)
		})
	}
}

// Helper method for Level
func (l Level) String() string {
	switch l {
	case Fastest:
		return "Fastest"
	case Default:
		return "Default"
	case Better:
		return "Better"
	case Best:
		return "Best"
	default:
		return "Unknown"
	}
}
