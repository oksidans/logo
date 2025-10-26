package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"parser/internal/botdetector"
	"parser/internal/csvin"
	"parser/internal/csvout"
	"parser/internal/enrich"
	"parser/internal/iox"
	"parser/internal/mapper"
	"parser/internal/schema"
	"parser/internal/verifier"
)

var (
	inPath     = flag.String("in", "", "Input CSV path (raw for normalize; normalized for enrich/verify; final for merge). Supports .gz")
	outPath    = flag.String("out", "", "Output CSV path. Supports .gz")
	stage      = flag.String("stage", "normalize", "Stage to run: normalize | enrich | verify | merge")
	botsPath   = flag.String("bots", "", "Optional path to bots config (JSON or YAML)")
	showPlan   = flag.Bool("plan", true, "Print plan and exit")
	workers    = flag.Int("workers", 15, "Number of parallel DNS workers for verify stage")
	verifiedIn = flag.String("verified", "", "Path to verified.csv (host_ip,botName,verified) for merge stage")
)

func main() {
	flag.Parse()
	if *inPath == "" || *outPath == "" {
		log.Fatal("usage: --in <in.csv[.gz]> --out <out.csv[.gz]> --stage normalize|enrich|verify|merge [--verified verified.csv] [--bots bots.json|yaml] [--workers N] [--plan=false]")
	}

	// global bot rules (used by verify; safe to init always)
	if err := botdetector.InitFromFile(*botsPath); err != nil && *botsPath != "" {
		log.Printf("bots: %v", err)
	} else if *botsPath != "" {
		log.Printf("bots: loaded from %s", *botsPath)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		<-ch
		fmt.Fprintln(os.Stderr, "\n⏹️  Signal received, shutting down...")
		cancel()
	}()

	if *showPlan {
		fmt.Println("Plan:")
		fmt.Printf("  Stage : %s\n", *stage)
		fmt.Printf("  Input : %s\n", *inPath)
		fmt.Printf("  Output: %s\n", *outPath)
		switch *stage {
		case "normalize":
			fmt.Printf("  Header (out): %v\n", schema.BaseHeader())
		case "enrich":
			fmt.Printf("  Header (out): %v\n", schema.BaseHeader())
			fmt.Println("  Note  : 'target' → resource type; empty 'referrer' → 'Direct Hit'.")
		case "verify":
			fmt.Println("  Note  : extracts unique host_ip from normalized CSV and writes verified.csv (host_ip,botName,verified).")
			fmt.Printf("  Workers: %d\n", *workers)
		case "merge":
			if *verifiedIn == "" {
				fmt.Println("  Note  : requires --verified <verified.csv>. Will merge botName+verified by host_ip into final CSV.")
			} else {
				fmt.Printf("  Verified: %s\n", *verifiedIn)
			}
			fmt.Printf("  Header (out): %v\n", schema.BaseHeader())
		}
		return
	}

	switch *stage {
	case "normalize":
		if err := runNormalize(ctx, *inPath, *outPath); err != nil {
			log.Fatalf("normalize error: %v", err)
		}
	case "enrich":
		if err := runEnrich(ctx, *inPath, *outPath); err != nil {
			log.Fatalf("enrich error: %v", err)
		}
	case "verify":
		if err := runVerify(ctx, *inPath, *outPath, *botsPath, *workers); err != nil {
			log.Fatalf("verify error: %v", err)
		}
	case "merge":
		if *verifiedIn == "" {
			log.Fatal("merge stage requires --verified <verified.csv>")
		}
		if err := runMerge(ctx, *inPath, *verifiedIn, *outPath); err != nil {
			log.Fatalf("merge error: %v", err)
		}
	default:
		log.Fatalf("unknown stage: %s", *stage)
	}
}

// ---------- normalize ----------

func runNormalize(ctx context.Context, inPath, outPath string) error {
	in, err := iox.OpenAuto(inPath)
	if err != nil {
		return fmt.Errorf("open input: %w", err)
	}
	defer in.Close()

	out, err := iox.CreateAuto(outPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer out.Close()

	reader := csvin.New(in, csvin.Options{Comma: ',', TrimSpace: true})
	if _, _, err := reader.Header(); err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	writer := csvout.New(out)
	if err := writer.WriteHeader(schema.BaseHeader()); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	var rowsIn, rowsOut, bad int64
	start := time.Now()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			log.Printf("progress: in=%d out=%d bad=%d", rowsIn, rowsOut, bad)
		default:
			row, err := reader.Next()
			if err != nil {
				if err == io.EOF {
					goto DONE
				}
				bad++
				continue
			}
			rowsIn++
			mapped := mapper.MapToCSV(row)
			if err := writer.WriteRow(mapped); err != nil {
				return fmt.Errorf("write row: %w", err)
			}
			rowsOut++
		}
	}

DONE:
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	log.Printf("normalize done. in=%d out=%d bad=%d time=%s", rowsIn, rowsOut, bad, time.Since(start))
	return nil
}

// ---------- enrich ----------

func runEnrich(ctx context.Context, inPath, outPath string) error {
	in, err := iox.OpenAuto(inPath)
	if err != nil {
		return fmt.Errorf("open input: %w", err)
	}
	defer in.Close()

	out, err := iox.CreateAuto(outPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer out.Close()

	reader := csvin.New(in, csvin.Options{Comma: ',', TrimSpace: true})
	header, _, err := reader.Header()
	if err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	writer := csvout.New(out)
	if err := writer.WriteHeader(schema.BaseHeader()); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	has := func(name string) bool {
		for _, h := range header {
			if h == name {
				return true
			}
		}
		return false
	}
	if !has("target") {
		log.Printf("warning: 'target' column not found")
	}

	var rowsIn, rowsOut, bad int64
	start := time.Now()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			log.Printf("progress: in=%d out=%d bad=%d", rowsIn, rowsOut, bad)
		default:
			row, err := reader.Next()
			if err != nil {
				if err == io.EOF {
					goto DONE
				}
				bad++
				continue
			}
			rowsIn++

			// 1) target -> resource type (use target, fallback to referring_page)
			url := row["target"]
			if url == "" {
				url = row["referring_page"]
			}
			class := enrich.ResourceTypeFromURL(url)
			row["target"] = class

			// 2) referrer => "Direct Hit" if empty but referring_page exists
			ref := strings.TrimSpace(row["referrer"])
			refpg := strings.TrimSpace(row["referring_page"])
			if (ref == "" || ref == "-") && refpg != "" && refpg != "-" {
				row["referrer"] = "Direct Hit"
			}

			outRow := make([]string, len(schema.BaseColumns))
			for i, c := range schema.BaseColumns {
				outRow[i] = row[c.Name]
			}
			if err := writer.WriteRow(outRow); err != nil {
				return fmt.Errorf("write row: %w", err)
			}
			rowsOut++
		}
	}

DONE:
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	log.Printf("enrich done. in=%d out=%d bad=%d time=%s", rowsIn, rowsOut, bad, time.Since(start))
	return nil
}

// ---------- verify (auto extract IPs + reverse DNS) ----------

func runVerify(ctx context.Context, inPath, outPath, botsPath string, workers int) error {
	log.Printf("verify stage: reading unique IPs from %s", inPath)

	// ensure bot rules loaded (already attempted in main)
	if err := botdetector.InitFromFile(botsPath); err != nil && botsPath != "" {
		log.Printf("warning: bot rules load failed: %v (using defaults)", err)
	}

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

	// ensure host_ip exists
	hasHost := false
	for _, h := range header {
		if h == "host_ip" {
			hasHost = true
			break
		}
	}
	if !hasHost {
		return fmt.Errorf("column 'host_ip' not found in input")
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
		// IPv6 zagrade -> ukloni; validacija IP-a
		if strings.HasPrefix(ip, "[") && strings.HasSuffix(ip, "]") {
			ip = strings.Trim(ip, "[]")
		}
		if net.ParseIP(ip) == nil {
			continue
		}
		unique[ip] = struct{}{}
	}
	log.Printf("collected %d unique IPs", len(unique))

	ips := make([]string, 0, len(unique))
	for ip := range unique {
		ips = append(ips, ip)
	}

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
	if err := verifier.WriteResultsCSV(outPath, results); err != nil {
		return fmt.Errorf("write csv: %w", err)
	}
	log.Printf("verify stage complete → wrote %s", outPath)
	return nil
}

// ---------- merge (final.csv + verified.csv -> merged.csv) ----------

func runMerge(ctx context.Context, finalPath, verifiedPath, outPath string) error {
	// 1) Učitaj verified.csv u mapu: ip -> (botName, verified)
	type verPair struct {
		name string
		flag string // normalized to "1" or "0"
	}

	verMap := make(map[string]verPair, 1<<16)

	vIn, err := iox.OpenAuto(verifiedPath)
	if err != nil {
		return fmt.Errorf("open verified: %w", err)
	}
	defer vIn.Close()

	vReader := csvin.New(vIn, csvin.Options{Comma: ',', TrimSpace: true})
	vHeader, _, err := vReader.Header()
	if err != nil {
		return fmt.Errorf("read verified header: %w", err)
	}
	hasHost, hasBN, hasVF := false, false, false
	for _, h := range vHeader {
		switch h {
		case "host_ip":
			hasHost = true
		case "botName":
			hasBN = true
		case "verified":
			hasVF = true
		}
	}
	if !hasHost || !hasBN || !hasVF {
		return fmt.Errorf("verified.csv must contain 'host_ip','botName','verified' columns")
	}

	for {
		row, err := vReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		ip := strings.TrimSpace(row["host_ip"])
		if ip == "" {
			continue
		}
		flag := normalizeBool(row["verified"])
		verMap[ip] = verPair{name: row["botName"], flag: flag}
	}

	// 2) Otvori final.csv i piši merged sa osveženim poljima
	in, err := iox.OpenAuto(finalPath)
	if err != nil {
		return fmt.Errorf("open final: %w", err)
	}
	defer in.Close()

	out, err := iox.CreateAuto(outPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer out.Close()

	reader := csvin.New(in, csvin.Options{Comma: ',', TrimSpace: true})
	header, _, err := reader.Header()
	if err != nil {
		return fmt.Errorf("read final header: %w", err)
	}
	// sanity for host_ip presence
	foundHost := false
	for _, h := range header {
		if h == "host_ip" {
			foundHost = true
			break
		}
	}
	if !foundHost {
		return fmt.Errorf("final CSV must contain 'host_ip' column")
	}

	writer := csvout.New(out)
	if err := writer.WriteHeader(schema.BaseHeader()); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	var rowsIn, rowsOut, patched int64
	start := time.Now()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			log.Printf("merge progress: in=%d out=%d patched=%d", rowsIn, rowsOut, patched)
		default:
			row, err := reader.Next()
			if err != nil {
				if err == io.EOF {
					goto DONE
				}
				continue
			}
			rowsIn++

			ip := strings.TrimSpace(row["host_ip"])
			if p, ok := verMap[ip]; ok {
				// upiši/override botName i verified
				row["botName"] = p.name
				row["verified"] = p.flag
				patched++
			}

			outRow := make([]string, len(schema.BaseColumns))
			for i, c := range schema.BaseColumns {
				outRow[i] = row[c.Name]
			}
			if err := writer.WriteRow(outRow); err != nil {
				return fmt.Errorf("write row: %w", err)
			}
			rowsOut++
		}
	}

DONE:
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	log.Printf("merge done. in=%d out=%d patched=%d time=%s", rowsIn, rowsOut, patched, time.Since(start))
	return nil
}

func normalizeBool(s string) string {
	v := strings.ToLower(strings.TrimSpace(s))
	switch v {
	case "1", "true", "yes", "y":
		return "1"
	default:
		return "0"
	}
}
