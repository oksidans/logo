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
	"parser/internal/csvout"
	"parser/internal/iox"
	"parser/internal/schema"
	"parser/internal/verifier"
)

func main() {
	inPath := flag.String("in", "", "Input CSV file path")
	outPath := flag.String("out", "", "Output CSV file path")
	stage := flag.String("stage", "normalize", "Stage: normalize | verify | merge")
	botsPath := flag.String("bots", "", "Bot rules file (.json or .yaml)")
	workers := flag.Int("workers", 15, "Number of parallel DNS lookup workers (verify stage only)")
	showPlan := flag.Bool("plan", false, "Show plan and exit (for debugging)")

	flag.Parse()

	if *showPlan {
		fmt.Println("=== PLAN ===")
		fmt.Printf("Stage  : %s\n", *stage)
		fmt.Printf("Input  : %s\n", *inPath)
		fmt.Printf("Output : %s\n", *outPath)
		fmt.Printf("Workers: %d\n", *workers)
		fmt.Printf("Bots   : %s\n", *botsPath)
		return
	}

	ctx := context.Background()

	switch *stage {
	case "normalize":
		if err := runNormalize(ctx, *inPath, *outPath); err != nil {
			log.Fatal(err)
		}
	case "verify":
		if err := botdetector.InitFromFile(*botsPath); err != nil {
			log.Printf("warning: bot rules load failed: %v (using defaults)", err)
		}
		if err := runVerify(ctx, *inPath, *outPath, *workers); err != nil {
			log.Fatal(err)
		}
	case "merge":
		if err := runMerge(ctx, "final.csv", "verified.csv", *outPath); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("unknown stage: %s", *stage)
	}
}

// ---------- STAGE 1: normalize ----------
func runNormalize(ctx context.Context, inPath, outPath string) error {
	in, err := iox.OpenAuto(inPath)
	if err != nil {
		return err
	}
	defer in.Close()

	reader := csvin.New(in, csvin.Options{Comma: ',', TrimSpace: true})
	header, _, err := reader.Header()
	if err != nil {
		return err
	}
	if len(header) == 0 {
		return fmt.Errorf("no header found in input CSV")
	}

	out, err := iox.CreateAuto(outPath)
	if err != nil {
		return err
	}
	defer out.Close()

	writer := csvout.New(out)
	if err := writer.WriteHeader(schema.BaseHeader()); err != nil {
		return err
	}

	rowsIn := 0
	rowsOut := 0
	start := time.Now()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			row, err := reader.Next()
			if err != nil {
				if err == io.EOF {
					goto DONE
				}
				log.Printf("read err: %v", err)
				continue
			}
			rowsIn++

			outRow := make([]string, len(schema.BaseColumns))
			for i, c := range schema.BaseColumns {
				outRow[i] = row[c.Name]
			}
			if err := writer.WriteRow(outRow); err != nil {
				return err
			}
			rowsOut++
		}
	}

DONE:
	if err := writer.Flush(); err != nil {
		return err
	}
	log.Printf("normalize done. in=%d out=%d time=%s", rowsIn, rowsOut, time.Since(start))
	return nil
}

// ---------- STAGE 2: verify ----------
func runVerify(ctx context.Context, inPath, outPath string, workers int) error {
	log.Printf("verify stage: reading unique IPs from %s", inPath)
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
	foundHost := false
	for _, h := range header {
		if h == "host_ip" {
			foundHost = true
			break
		}
	}
	if !foundHost {
		return fmt.Errorf("CSV missing 'host_ip' column")
	}

	unique := make(map[string]struct{})
	for {
		row, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			continue
		}
		ip := normalizeIPKey(row["host_ip"])
		if ip == "" {
			continue
		}
		unique[ip] = struct{}{}
	}
	ips := make([]string, 0, len(unique))
	for ip := range unique {
		ips = append(ips, ip)
	}
	log.Printf("collected %d unique IPs", len(ips))

	progress := make(chan int, 100)
	go func() {
		tick := time.NewTicker(5 * time.Second)
		defer tick.Stop()
		total := len(ips)
		done := 0
		for {
			select {
			case <-tick.C:
				log.Printf("progress: %d/%d", done, total)
			case n, ok := <-progress:
				if !ok {
					return
				}
				done += n
			}
		}
	}()

	results, err := verifier.VerifyIPs(ctx, ips, workers, 8*time.Second, progress)
	close(progress)
	if err != nil {
		return fmt.Errorf("verify: %w", err)
	}

	if err := verifier.WriteResultsCSV(outPath, results); err != nil {
		return fmt.Errorf("write verified: %w", err)
	}
	log.Printf("verify done: wrote %d rows to %s", len(results), outPath)
	return nil
}

//
// ---------- STAGE 3: merge (final.csv + verified.csv) ----------

func normalizeIPKey(ip string) string {
	ip = strings.TrimSpace(ip)
	if strings.HasPrefix(ip, "[") && strings.HasSuffix(ip, "]") {
		ip = strings.Trim(ip, "[]")
	}
	return ip
}

func uniqJoinPipe(a, b string) string {
	if a == "" {
		return b
	}
	if b == "" {
		return a
	}
	seen := make(map[string]struct{})
	out := make([]string, 0, 8)
	push := func(tok string) {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			return
		}
		if _, ok := seen[tok]; ok {
			return
		}
		seen[tok] = struct{}{}
		out = append(out, tok)
	}
	for _, tok := range strings.Split(a, "|") {
		push(tok)
	}
	for _, tok := range strings.Split(b, "|") {
		push(tok)
	}
	return strings.Join(out, "|")
}

func runMerge(ctx context.Context, finalPath, verifiedPath, outPath string) error {
	type verPair struct {
		name string
		flag string // "1" or "0"
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
		return fmt.Errorf("verified.csv must contain 'host_ip','botName','verified'")
	}
	for {
		row, err := vReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		ip := normalizeIPKey(row["host_ip"])
		if ip == "" {
			continue
		}

		var vflag string
		switch strings.TrimSpace(row["verified"]) {
		case "1", "true", "TRUE", "True", "yes", "y":
			vflag = "1"
		default:
			vflag = "0"
		}

		verMap[ip] = verPair{
			name: strings.TrimSpace(row["botName"]),
			flag: vflag,
		}
	}

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

			ipKey := normalizeIPKey(row["host_ip"])
			if p, ok := verMap[ipKey]; ok {
				row["botName"] = uniqJoinPipe(row["botName"], p.name)
				row["verified"] = p.flag
				patched++
			}

			outRow := make([]string, len(schema.BaseColumns))
			for i, c := range schema.BaseColumns {
				outRow[i] = row[c.Name] // protocol ostaje kakav je bio
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
