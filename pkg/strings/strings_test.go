package strings

import (
	"strings"
	"testing"
	"unsafe"
)

func TestBytesToString(t *testing.T) {
	b := []byte("hello world")
	s := BytesToString(b)

	if s != "hello world" {
		t.Errorf("expected 'hello world', got '%s'", s)
	}

	// Test empty slice
	empty := BytesToString([]byte{})
	if empty != "" {
		t.Errorf("expected empty string, got '%s'", empty)
	}
}

func TestStringToBytes(t *testing.T) {
	s := "hello world"
	b := StringToBytes(s)

	if string(b) != "hello world" {
		t.Errorf("expected 'hello world', got '%s'", string(b))
	}

	// Test empty string
	empty := StringToBytes("")
	if empty != nil {
		t.Errorf("expected nil slice, got %v", empty)
	}
}

func TestBuilder(t *testing.T) {
	builder := NewBuilder(32)

	builder.WriteString("hello")
	builder.WriteByte(' ')
	builder.WriteString("world")

	result := builder.String()
	if result != "hello world" {
		t.Errorf("expected 'hello world', got '%s'", result)
	}

	if builder.Len() != 11 {
		t.Errorf("expected length 11, got %d", builder.Len())
	}
}

func TestBuilderGrow(t *testing.T) {
	builder := NewBuilder(2)
	initialCap := builder.Cap()

	builder.Grow(10)
	if builder.Cap() <= initialCap {
		t.Errorf("expected capacity to grow, initial: %d, after: %d", initialCap, builder.Cap())
	}
}

func TestBuilderReset(t *testing.T) {
	builder := NewBuilder(32)
	builder.WriteString("test")

	if builder.Len() != 4 {
		t.Errorf("expected length 4, got %d", builder.Len())
	}

	builder.Reset()
	if builder.Len() != 0 {
		t.Errorf("expected length 0 after reset, got %d", builder.Len())
	}
}

func TestPool(t *testing.T) {
	pool := NewPool(2, 32)

	// Get builder from pool
	builder1 := pool.Get()
	if builder1 == nil {
		t.Error("expected non-nil builder from pool")
	}

	// Use builder
	builder1.WriteString("test")
	if builder1.String() != "test" {
		t.Errorf("expected 'test', got '%s'", builder1.String())
	}

	// Return to pool
	pool.Put(builder1)

	// Get again - should be reset
	builder2 := pool.Get()
	if builder2.Len() != 0 {
		t.Errorf("expected reset builder, got length %d", builder2.Len())
	}
}

func TestContains(t *testing.T) {
	tests := []struct {
		s, substr string
		expected  bool
	}{
		{"hello world", "world", true},
		{"hello world", "foo", false},
		{"hello world", "", true},
		{"", "foo", false},
		{"hello", "hello world", false},
	}

	for _, test := range tests {
		result := Contains(test.s, test.substr)
		if result != test.expected {
			t.Errorf("Contains(%q, %q) = %v, expected %v", test.s, test.substr, result, test.expected)
		}
	}
}

func TestIndex(t *testing.T) {
	tests := []struct {
		s, substr string
		expected  int
	}{
		{"hello world", "world", 6},
		{"hello world", "foo", -1},
		{"hello world", "", 0},
		{"", "foo", -1},
		{"hello", "hello world", -1},
		{"abcabc", "abc", 0},
	}

	for _, test := range tests {
		result := Index(test.s, test.substr)
		if result != test.expected {
			t.Errorf("Index(%q, %q) = %d, expected %d", test.s, test.substr, result, test.expected)
		}
	}
}

func TestHasPrefix(t *testing.T) {
	tests := []struct {
		s, prefix string
		expected  bool
	}{
		{"hello world", "hello", true},
		{"hello world", "world", false},
		{"hello world", "", true},
		{"", "hello", false},
		{"hi", "hello", false},
	}

	for _, test := range tests {
		result := HasPrefix(test.s, test.prefix)
		if result != test.expected {
			t.Errorf("HasPrefix(%q, %q) = %v, expected %v", test.s, test.prefix, result, test.expected)
		}
	}
}

func TestHasSuffix(t *testing.T) {
	tests := []struct {
		s, suffix string
		expected  bool
	}{
		{"hello world", "world", true},
		{"hello world", "hello", false},
		{"hello world", "", true},
		{"", "world", false},
		{"hi", "world", false},
	}

	for _, test := range tests {
		result := HasSuffix(test.s, test.suffix)
		if result != test.expected {
			t.Errorf("HasSuffix(%q, %q) = %v, expected %v", test.s, test.suffix, result, test.expected)
		}
	}
}

func TestTrimSpace(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"  hello world  ", "hello world"},
		{"hello world", "hello world"},
		{"  ", ""},
		{"", ""},
		{"\t\nhello\r\n", "hello"},
	}

	for _, test := range tests {
		result := TrimSpace(test.input)
		if result != test.expected {
			t.Errorf("TrimSpace(%q) = %q, expected %q", test.input, result, test.expected)
		}
	}
}

func TestSplit(t *testing.T) {
	tests := []struct {
		s, delimiter string
		expected     []string
	}{
		{"a,b,c", ",", []string{"a", "b", "c"}},
		{"hello", ",", []string{"hello"}},
		{"a,,b", ",", []string{"a", "", "b"}},
		{"", ",", []string{""}},
		{"a,b,c", "", []string{"a,b,c"}},
	}

	for _, test := range tests {
		result := Split(test.s, test.delimiter)
		if len(result) != len(test.expected) {
			t.Errorf("Split(%q, %q) length = %d, expected %d", test.s, test.delimiter, len(result), len(test.expected))
			continue
		}

		for i, part := range result {
			if part != test.expected[i] {
				t.Errorf("Split(%q, %q)[%d] = %q, expected %q", test.s, test.delimiter, i, part, test.expected[i])
			}
		}
	}
}

func TestJoin(t *testing.T) {
	tests := []struct {
		strings   []string
		delimiter string
		expected  string
	}{
		{[]string{"a", "b", "c"}, ",", "a,b,c"},
		{[]string{"hello"}, ",", "hello"},
		{[]string{}, ",", ""},
		{[]string{"a", "", "b"}, ",", "a,,b"},
	}

	for _, test := range tests {
		result := Join(test.strings, test.delimiter)
		if result != test.expected {
			t.Errorf("Join(%v, %q) = %q, expected %q", test.strings, test.delimiter, result, test.expected)
		}
	}
}

func TestIntern(t *testing.T) {
	intern := NewIntern()

	s1 := intern.Get("hello")
	s2 := intern.Get("hello")

	// Should return the same string instance
	if s1 != s2 {
		t.Error("interned strings should be equal")
	}

	// Check that they are actually the same underlying string
	if unsafe.StringData(s1) != unsafe.StringData(s2) {
		t.Error("interned strings should share memory")
	}

	if intern.Size() != 1 {
		t.Errorf("expected size 1, got %d", intern.Size())
	}

	intern.Clear()
	if intern.Size() != 0 {
		t.Errorf("expected size 0 after clear, got %d", intern.Size())
	}
}

// Benchmarks to compare with standard library

func BenchmarkBytesToString(b *testing.B) {
	data := []byte("hello world this is a test string")

	b.Run("ZeroCopy", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = BytesToString(data)
		}
	})

	b.Run("Standard", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = string(data)
		}
	})
}

func BenchmarkStringToBytes(b *testing.B) {
	s := "hello world this is a test string"

	b.Run("ZeroCopy", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = StringToBytes(s)
		}
	})

	b.Run("Standard", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = []byte(s)
		}
	})
}

func BenchmarkStringBuilder(b *testing.B) {
	parts := []string{"hello", " ", "world", " ", "this", " ", "is", " ", "a", " ", "test"}

	b.Run("ZeroCopyBuilder", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			builder := NewBuilder(64)
			for _, part := range parts {
				builder.WriteString(part)
			}
			_ = builder.String()
		}
	})

	b.Run("StandardBuilder", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var builder strings.Builder
			for _, part := range parts {
				builder.WriteString(part)
			}
			_ = builder.String()
		}
	})
}

func BenchmarkStringJoin(b *testing.B) {
	parts := []string{"hello", "world", "this", "is", "a", "test", "string"}

	b.Run("ZeroCopyJoin", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = Join(parts, " ")
		}
	})

	b.Run("StandardJoin", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = strings.Join(parts, " ")
		}
	})
}

func TestURLBuilder(t *testing.T) {
	tests := []struct {
		name     string
		build    func() string
		expected string
	}{
		{
			name: "basic URL with params",
			build: func() string {
				ub := NewURLBuilder("https://api.example.com")
				defer ub.Close()
				return ub.AddParam("key", "value").AddParam("foo", "bar").String()
			},
			expected: "https://api.example.com?key=value&foo=bar",
		},
		{
			name: "URL with path segments",
			build: func() string {
				ub := NewURLBuilder("https://api.example.com")
				defer ub.Close()
				return ub.AddPath("v1", "users", "123").String()
			},
			expected: "https://api.example.com/v1/users/123",
		},
		{
			name: "URL with path and params",
			build: func() string {
				ub := NewURLBuilder("https://api.example.com")
				defer ub.Close()
				return ub.AddPath("v1", "users").AddParam("limit", "10").String()
			},
			expected: "https://api.example.com/v1/users?limit=10",
		},
		{
			name: "URL with encoding",
			build: func() string {
				ub := NewURLBuilder("https://api.example.com")
				defer ub.Close()
				return ub.AddParam("query", "hello world").AddParam("special", "a+b=c").String()
			},
			expected: "https://api.example.com?query=hello+world&special=a%2Bb%3Dc",
		},
		{
			name: "form builder",
			build: func() string {
				fb := NewFormBuilder()
				defer fb.Close()
				return fb.AddParam("grant_type", "authorization_code").
					AddParam("code", "abc123").
					Query()
			},
			expected: "grant_type=authorization_code&code=abc123",
		},
		{
			name: "URL with integer param",
			build: func() string {
				ub := NewURLBuilder("https://api.example.com")
				defer ub.Close()
				return ub.AddParamInt("page", 5).AddParamInt("size", 100).String()
			},
			expected: "https://api.example.com?page=5&size=100",
		},
		{
			name: "URL with boolean param",
			build: func() string {
				ub := NewURLBuilder("https://api.example.com")
				defer ub.Close()
				return ub.AddParamBool("active", true).AddParamBool("deleted", false).String()
			},
			expected: "https://api.example.com?active=true&deleted=false",
		},
		{
			name: "empty path segments",
			build: func() string {
				ub := NewURLBuilder("https://api.example.com")
				defer ub.Close()
				return ub.AddPath("v1", "", "users").String()
			},
			expected: "https://api.example.com/v1/users",
		},
		{
			name: "path with special characters",
			build: func() string {
				ub := NewURLBuilder("https://api.example.com")
				defer ub.Close()
				return ub.AddPath("v1", "my file.txt").String()
			},
			expected: "https://api.example.com/v1/my%20file.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.build()
			if result != tt.expected {
				t.Errorf("URLBuilder test failed\nExpected: %s\nGot:      %s", tt.expected, result)
			}
		})
	}
}

func BenchmarkURLBuilder(b *testing.B) {
	b.Run("URLBuilder", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			ub := NewURLBuilder("https://api.example.com")
			ub.AddPath("v1", "users", "123").
				AddParam("access_token", "abcdef123456").
				AddParam("fields", "id,name,email").
				AddParamInt("limit", 100).
				AddParamBool("active", true)
			_ = ub.String()
			ub.Close()
		}
	})
}
