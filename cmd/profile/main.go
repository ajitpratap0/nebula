package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"
)

func main() {
	// Command-line flags
	var (
		duration     = flag.Duration("duration", 30*time.Second, "Profiling duration")
		outputDir    = flag.String("output", "./profiles", "Output directory for profiles")
		profileTypes = flag.String("types", "cpu,memory", "Profile types (cpu,memory,block,mutex,goroutine,all)")
		cpuFile      = flag.String("cpuprofile", "", "Write CPU profile to file")
		memFile      = flag.String("memprofile", "", "Write memory profile to file")
	)

	// Help flag
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nOptions:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s -types cpu -duration 30s\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cpuprofile cpu.prof -memprofile mem.prof\n", os.Args[0])
	}

	flag.Parse()

	// Parse profile types
	types := parseProfileTypes(*profileTypes)

	fmt.Printf("Starting performance profiling...\n")
	fmt.Printf("Duration: %v\n", *duration)
	fmt.Printf("Profile types: %s\n", *profileTypes)
	fmt.Printf("Output directory: %s\n", *outputDir)

	// Create output directory
	if err := os.MkdirAll(*outputDir, 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	// Start CPU profiling if requested
	if *cpuFile != "" || contains(types, "cpu") {
		cpuProfileFile := *cpuFile
		if cpuProfileFile == "" {
			cpuProfileFile = fmt.Sprintf("%s/cpu.prof", *outputDir)
		}

		f, err := os.Create(cpuProfileFile)
		if err != nil {
			log.Fatalf("Failed to create CPU profile: %v", err)
		}
		defer f.Close()

		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatalf("Failed to start CPU profile: %v", err)
		}
		defer pprof.StopCPUProfile()

		fmt.Printf("CPU profiling enabled, writing to: %s\n", cpuProfileFile)
	}

	// Run some work
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	// Simulate some CPU work
	runSimulatedWork(ctx)

	// Write memory profile if requested
	if *memFile != "" || contains(types, "memory") {
		memProfileFile := *memFile
		if memProfileFile == "" {
			memProfileFile = fmt.Sprintf("%s/mem.prof", *outputDir)
		}

		f, err := os.Create(memProfileFile)
		if err != nil {
			log.Fatalf("Failed to create memory profile: %v", err)
		}
		defer f.Close()

		runtime.GC() // Get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatalf("Failed to write memory profile: %v", err)
		}

		fmt.Printf("Memory profile written to: %s\n", memProfileFile)
	}

	// Write other profiles
	for _, profileType := range types {
		switch profileType {
		case "block":
			writeProfile("block", fmt.Sprintf("%s/block.prof", *outputDir))
		case "mutex":
			writeProfile("mutex", fmt.Sprintf("%s/mutex.prof", *outputDir))
		case "goroutine":
			writeProfile("goroutine", fmt.Sprintf("%s/goroutine.prof", *outputDir))
		}
	}

	fmt.Printf("Profiling completed successfully\n")
}

// runSimulatedWork simulates some CPU and memory work
func runSimulatedWork(ctx context.Context) {
	fmt.Printf("Running simulated workload...\n")

	// Allocate some memory and do some CPU work
	data := make([][]byte, 1000)

	for i := 0; i < len(data); i++ {
		select {
		case <-ctx.Done():
			return
		default:
			// Allocate memory
			data[i] = make([]byte, 1024*1024) // 1MB

			// Do some CPU work
			for j := 0; j < len(data[i]); j++ {
				data[i][j] = byte(i + j)
			}

			// Sleep briefly to make profiling observable
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Keep data alive
	_ = data

	fmt.Printf("Simulated workload completed\n")
}

// writeProfile writes a specific profile type to file
func writeProfile(profileName, filename string) {
	profile := pprof.Lookup(profileName)
	if profile == nil {
		fmt.Printf("Profile %s not found\n", profileName)
		return
	}

	f, err := os.Create(filename)
	if err != nil {
		log.Printf("Failed to create %s profile: %v", profileName, err)
		return
	}
	defer f.Close()

	if err := profile.WriteTo(f, 0); err != nil {
		log.Printf("Failed to write %s profile: %v", profileName, err)
		return
	}

	fmt.Printf("%s profile written to: %s\n", strings.Title(profileName), filename)
}

// parseProfileTypes parses the profile types string
func parseProfileTypes(typesStr string) []string {
	if typesStr == "all" {
		return []string{"cpu", "memory", "block", "mutex", "goroutine"}
	}

	parts := strings.Split(typesStr, ",")
	types := make([]string, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		switch part {
		case "cpu", "memory", "mem", "block", "mutex", "goroutine":
			if part == "mem" {
				part = "memory"
			}
			types = append(types, part)
		}
	}

	return types
}

// contains checks if a slice contains a string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
