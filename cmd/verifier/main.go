package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"parser/internal/botdetector"
	"parser/internal/csvin"
	"parser/internal/iox"
	"parser/internal/verifier"
)

var (
	inPath   = flag.String("in", "", "Input CSV file path")
	outPath  = flag.String("out", "", "Output CSV file path")
	botsPath = flag.String("bots", "", "Optional path to bots config (JSON or YAML)")
	stage    = flag.String("stage", "", "Stage to run: normalize | enrich | verify")
	//plan     = flag.Bool("plan", false, "If true, show column plan and exit")
	workers = flag.Int("workers", 15, "Number of parallel DNS workers for verify stage")
)

func main() {
	flag.Parse()
	ctx := context.Background()

	if *inPath == "" || *outPath == "" {
		log.Fatalf("--in and --out are required")
	}

	switch *stage {
	case "normalize":
		runNormalize(ctx, *inPath, *outPath)
	case "enrich":
		runEnrich(ctx, *inPath, *outPath)
	case "verify":
		if err := runVerify(ctx, *inPath, *outPath, *botsPath, *workers); err != nil {
			log.Fatalf("verify error: %v", err)
		}
	default:
		log.Fatalf("unknown stage: %s", *stage)
	}
}

func runNormalize(ctx context.Context, inPath, outPath string) {
	fmt.Println("normalize stage placeholder – implemented earlier")
}

func runEnrich(ctx context.Context, inPath, outPath string) {
	fmt.Println("enrich stage placeholder – implemented earlier")
}

func runVerify(ctx context.Context, inPath, outPath, botsPath string, workers int) error {
	log.Printf("verify stage: reading unique IPs from %s", inPath)

	// 1️⃣ Učitavanje bot pravila
	if err := botdetector.InitFromFile(botsPath); err != nil {
		log.Printf("warning: bot rules load failed: %v (using defaults)", err)
	}

	// 2️⃣ Otvaranje normalized.csv i prikupljanje IP adresa
	in, err := iox.OpenAuto(inPath)
	if err != nil {
		return fmt.Errorf("open input: %w", err)
	}
	defer in.Close()

	reader := csvin.New(in, csvin.Options{Comma: ',', TrimSpace: true})
	header, _, err := reader.Header()
	if err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	colIdx := -1
	for i, name := range header {
		if name == "host_ip" {
			colIdx = i
			break
		}
	}
	if colIdx == -1 {
		return fmt.Errorf("column 'host_ip' not found")
	}

	unique := make(map[string]struct{})
	for {
		row, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		ip := strings.TrimSpace(row["host_ip"])
		if ip != "" {
			unique[ip] = struct{}{}
		}
	}
	log.Printf("collected %d unique IPs", len(unique))

	// 3️⃣ Pretvaranje u slice
	ips := make([]string, 0, len(unique))
	for ip := range unique {
		ips = append(ips, ip)
	}

	// 4️⃣ Reverse DNS verifikacija
	total := len(ips)
	log.Printf("starting reverse DNS verification with %d workers...", workers)

	progressChan := make(chan int, 100)
	go func() {
		count := 0
		for n := range progressChan {
			count += n
			if count%5000 == 0 || count == total {
				log.Printf("progress: processed %d / %d (%.1f%%)",
					count, total, float64(count)*100/float64(total))
			}
		}
	}()

	results, err := verifier.VerifyIPs(ctx, ips, workers, 3*time.Second, progressChan)
	close(progressChan)
	if err != nil {
		return fmt.Errorf("verify: %w", err)
	}

	// 5️⃣ Upis u CSV
	if err := verifier.WriteResultsCSV(outPath, results); err != nil {
		return fmt.Errorf("write csv: %w", err)
	}
	log.Printf("verify stage complete → wrote %s", outPath)
	return nil
}
