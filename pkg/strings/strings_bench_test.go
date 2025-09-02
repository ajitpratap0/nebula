// Package strings provides benchmarks for string building optimizations
package strings

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

// Generate test data
func generateTestStrings(count int) []string {
	strs := make([]string, count)
	for i := 0; i < count; i++ {
		strs[i] = fmt.Sprintf("test_string_%d", i)
	}
	return strs
}

func generateCSVData(rows, cols int) [][]string {
	data := make([][]string, rows)
	for i := 0; i < rows; i++ {
		row := make([]string, cols)
		for j := 0; j < cols; j++ {
			switch j % 4 {
			case 0:
				row[j] = fmt.Sprintf("text_%d_%d", i, j)
			case 1:
				row[j] = strconv.Itoa(i * j)
			case 2:
				row[j] = strconv.FormatBool(i%2 == 0)
			case 3:
				row[j] = fmt.Sprintf("%.2f", float64(i)*1.5)
			}
		}
		data[i] = row
	}
	return data
}

// Benchmark string concatenation
func BenchmarkStringConcatenation(b *testing.B) {
	testStrings := generateTestStrings(100)

	b.Run("StandardConcatenation", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			result := ""
			for _, s := range testStrings {
				result += s + ","
			}
			_ = result
		}
	})

	b.Run("PooledConcat", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			result := Concat(testStrings...)
			_ = result
		}
	})

	b.Run("PooledJoin", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			result := JoinPooled(testStrings, ",")
			_ = result
		}
	})

	b.Run("StandardJoin", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			result := strings.Join(testStrings, ",")
			_ = result
		}
	})
}

// Benchmark sprintf vs pooled sprintf
func BenchmarkSprintfComparison(b *testing.B) {
	values := []interface{}{"test", 42, true, 3.14}
	format := "string: %s, int: %d, bool: %t, float: %.2f"

	b.Run("StandardSprintf", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			result := fmt.Sprintf(format, values...)
			_ = result
		}
	})

	b.Run("PooledSprintf", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			result := Sprintf(format, values...)
			_ = result
		}
	})
}

// Benchmark CSV building
func BenchmarkCSVBuilding(b *testing.B) {
	csvData := generateCSVData(100, 10)
	headers := []string{"col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9", "col10"}

	b.Run("ManualCSVBuilding", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			result := strings.Join(headers, ",") + "\n"
			for _, row := range csvData {
				result += strings.Join(row, ",") + "\n"
			}
			_ = result
		}
	})

	b.Run("PooledCSVBuilder", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			builder := NewCSVBuilder(len(csvData), len(headers))
			defer builder.Close()

			builder.WriteHeader(headers)
			for _, row := range csvData {
				builder.WriteRow(row)
			}
			result := builder.String()
			_ = result
		}
	})
}

// Benchmark URL building
func BenchmarkURLBuilding(b *testing.B) {
	baseURL := "https://api.example.com/v1/data"

	b.Run("ManualURLBuilding", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			result := baseURL + "?limit=100&offset=" + strconv.Itoa(i*100) + "&format=json&sort=desc"
			_ = result
		}
	})

	b.Run("SprintfURLBuilding", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			result := fmt.Sprintf("%s?limit=100&offset=%d&format=json&sort=desc", baseURL, i*100)
			_ = result
		}
	})

	b.Run("PooledURLBuilder", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			builder := NewURLBuilder(baseURL)
			defer builder.Close()

			builder.AddParamInt("limit", 100).
				AddParamInt("offset", i*100).
				AddParam("format", "json").
				AddParam("sort", "desc")

			result := builder.String()
			_ = result
		}
	})
}

// Benchmark SQL building
func BenchmarkSQLBuilding(b *testing.B) {
	tableName := "users"
	values := []string{"John Doe", "john@example.com", "Manager"}

	b.Run("ManualSQLBuilding", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			result := "INSERT INTO " + tableName + " (name, email, role) VALUES ('" + values[0] + "', '" + values[1] + "', '" + values[2] + "')"
			_ = result
		}
	})

	b.Run("SprintfSQLBuilding", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			result := fmt.Sprintf("INSERT INTO %s (name, email, role) VALUES ('%s', '%s', '%s')",
				tableName, values[0], values[1], values[2])
			_ = result
		}
	})

	b.Run("PooledSQLBuilder", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			builder := NewSQLBuilder(200)
			defer builder.Close()

			builder.WriteQuery("INSERT INTO").WriteSpace().
				WriteIdentifier(tableName).WriteSpace().
				WriteQuery("(name, email, role) VALUES (").
				WriteStringLiteral(values[0]).WriteQuery(", ").
				WriteStringLiteral(values[1]).WriteQuery(", ").
				WriteStringLiteral(values[2]).WriteQuery(")")

			result := builder.String()
			_ = result
		}
	})
}

// Benchmark builder pool efficiency
func BenchmarkBuilderPoolEfficiency(b *testing.B) {
	testStrings := generateTestStrings(50)

	b.Run("PooledBuilders", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				builder := GetBuilder(Small)
				for _, s := range testStrings {
					builder.WriteString(s)
					builder.WriteByte(',')
				}
				result := builder.String()
				PutBuilder(builder, Small)
				_ = result
			}
		})
	})

	b.Run("NewBuilders", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				builder := NewBuilder(1024)
				for _, s := range testStrings {
					builder.WriteString(s)
					builder.WriteByte(',')
				}
				result := builder.String()
				_ = result
			}
		})
	})
}

// Benchmark scaling with different data sizes
func BenchmarkStringBuildingScaling(b *testing.B) {
	sizes := []int{10, 100, 1000, 10000}

	for _, size := range sizes {
		testStrings := generateTestStrings(size)

		b.Run(fmt.Sprintf("StandardConcatenation_%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				result := ""
				for _, s := range testStrings {
					result += s
				}
				_ = result
			}
		})

		b.Run(fmt.Sprintf("PooledConcat_%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				result := Concat(testStrings...)
				_ = result
			}
		})

		b.Run(fmt.Sprintf("BuildString_%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				result := BuildString(func(builder *Builder) {
					for _, s := range testStrings {
						builder.WriteString(s)
					}
				})
				_ = result
			}
		})
	}
}

// Test correctness
func TestStringBuildingCorrectness(t *testing.T) {
	testStrings := []string{"hello", "world", "test"}

	// Test concatenation
	expected := "helloworldtest"
	result := Concat(testStrings...)
	if result != expected {
		t.Errorf("Concat failed: expected %s, got %s", expected, result)
	}

	// Test join
	expected = "hello,world,test"
	result = JoinPooled(testStrings, ",")
	if result != expected {
		t.Errorf("JoinPooled failed: expected %s, got %s", expected, result)
	}

	// Test sprintf
	expected = "value: 42"
	result = Sprintf("value: %d", 42)
	if result != expected {
		t.Errorf("Sprintf failed: expected %s, got %s", expected, result)
	}

	// Test CSV builder
	builder := NewCSVBuilder(2, 3)
	defer builder.Close()

	builder.WriteHeader([]string{"name", "age", "city"})
	builder.WriteRow([]string{"John", "30", "NYC"})
	builder.WriteRow([]string{"Jane", "25", "LA"})

	csvResult := builder.String()
	expectedCSV := "name,age,city\nJohn,30,NYC\nJane,25,LA\n"
	if csvResult != expectedCSV {
		t.Errorf("CSV builder failed:\nexpected: %q\ngot: %q", expectedCSV, csvResult)
	}

	// Test URL builder
	urlBuilder := NewURLBuilder("https://api.example.com/data")
	defer urlBuilder.Close()

	urlBuilder.AddParam("limit", "100").AddParamInt("offset", 50).AddParamBool("active", true)
	urlResult := urlBuilder.String()
	expectedURL := "https://api.example.com/data?limit=100&offset=50&active=true"
	if urlResult != expectedURL {
		t.Errorf("URL builder failed: expected %s, got %s", expectedURL, urlResult)
	}
}
