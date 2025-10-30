package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"regexp"
	"strings"
	"time"

	"parser/internal/aibots"
	"parser/internal/botdetector"
	"parser/internal/csvin"
	"parser/internal/csvout"
	"parser/internal/enrich"
	"parser/internal/iox"
	"parser/internal/jsonl"
	"parser/internal/mapper"
	"parser/internal/schema"
	"parser/internal/verifier"
)

var version = "v1.4"

func main() {
	// Common I/O + stage
	inPath := flag.String("in", "", "Input file path")
	outPath := flag.String("out", "", "Output file path")
	stage := flag.String("stage", "normalize", "Stage: jsonl | normalize | enrich | verify | merge | aibots")

	// JSONL acceleration flags
	jsonlWorkers := flag.Int("jsonl-workers", 8, "Number of workers for jsonl stage")
	jsonlTempDir := flag.String("jsonl-temp", "", "Temp dir for jsonl stage (default: system temp)")
	jsonlBuf := flag.Int("jsonl-buf", 8192, "Buffered jobs (lines) for jsonl stage")

	// Verify / Merge flags
	botsPath := flag.String("bots", "", "Bot rules file (.json or .yaml)")
	workers := flag.Int("workers", 15, "Number of parallel DNS lookup workers (verify stage)")
	uaPtrVerify := flag.Bool("ua-ptr-verify", false, "Mark verified=1 when UA and PTR share same base domain (heuristic)")

	// Mapper fallback for http/https when ClientRequestScheme is missing
	defaultScheme := flag.String("default-scheme", "https", "Fallback scheme when ClientRequestScheme is missing (http or https)")

	showPlan := flag.Bool("plan", false, "Show plan and exit")
	flag.Parse()

	// Plumb default scheme to mapper (used in protocol + absolute URL construction)
	mapper.SetDefaultScheme(strings.ToLower(strings.TrimSpace(*defaultScheme)))

	if *showPlan {
		fmt.Printf("==== Parser %s Execution Plan ====\n", version)
		fmt.Printf("Stage              : %s\n", *stage)
		fmt.Printf("Input              : %s\n", *inPath)
		fmt.Printf("Output             : %s\n", *outPath)
		fmt.Printf("Bots rules         : %s\n", *botsPath)
		fmt.Printf("Workers (DNS)      : %d\n", *workers)
		fmt.Printf("UA↔PTR verify      : %v\n", *uaPtrVerify)
		fmt.Printf("JSONL workers      : %d\n", *jsonlWorkers)
		fmt.Printf("JSONL tempdir      : %s\n", *jsonlTempDir)
		fmt.Printf("JSONL bufsize      : %d\n", *jsonlBuf)
		fmt.Printf("Default scheme     : %s\n", *defaultScheme)
		return
	}

	start := time.Now()
	defer func() {
		log.Printf("⏱️ completed in %v", time.Since(start))
	}()

	ctx := context.Background()

	switch *stage {
	case "jsonl":
		err := jsonl.ConvertJSONLToCSVConcurrent(
			*inPath,
			*outPath,
			jsonl.Options{
				Workers:  *jsonlWorkers,
				TempDir:  *jsonlTempDir,
				BufLines: *jsonlBuf,
			},
		)
		if err != nil {
			log.Fatalf("jsonl stage: %v", err)
		}
		log.Println("✅ JSONL → CSV conversion complete")

	case "normalize":
		if err := runNormalize(ctx, *inPath, *outPath); err != nil {
			log.Fatal(err)
		}
		log.Println("✅ Normalization complete")

	case "enrich":
		if err := runEnrich(ctx, *inPath, *outPath); err != nil {
			log.Fatal(err)
		}
		log.Println("✅ Enrichment complete")

	case "verify":
		if err := botdetector.InitFromFile(*botsPath); err != nil && *botsPath != "" {
			log.Printf("warning: bot rules load failed: %v (using defaults)", err)
		}
		if err := runVerify(ctx, *inPath, *outPath, *workers); err != nil {
			log.Fatal(err)
		}
		log.Println("✅ DNS verification complete")

	case "merge":
		if err := runMerge(ctx, "final.csv", "verified.csv", *outPath, *uaPtrVerify); err != nil {
			log.Fatal(err)
		}
		log.Println("✅ Merge complete")

	case "aibots":
		if err := runAIBots(ctx, *inPath, *outPath); err != nil {
			log.Fatal(err)
		}
		log.Println("✅ AI-bots tagging complete")

	default:
		log.Fatalf("unknown stage: %s", *stage)
	}
}

// ---------- STAGE 1: normalize ----------
func runNormalize(ctx context.Context, inPath, outPath string) error {
	if inPath == "" || outPath == "" {
		return fmt.Errorf("normalize: --in and --out are required")
	}
	if inPath == outPath {
		return fmt.Errorf("normalize: input and output paths must differ (got %q)", inPath)
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
	if len(header) == 0 {
		return fmt.Errorf("normalize: input has no header")
	}
	log.Printf("normalize: input header has %d columns; first 10: %v", len(header), func() []string {
		if len(header) > 10 {
			return header[:10]
		}
		return header
	}())

	out, err := iox.CreateAuto(outPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer out.Close()

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
			log.Printf("normalize progress: in=%d out=%d bad=%d", rowsIn, rowsOut, bad)
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

			outRow := mapper.MapToCSV(row)
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
	log.Printf("normalize done. in=%d out=%d bad=%d time=%s", rowsIn, rowsOut, bad, time.Since(start))
	if rowsOut == 0 {
		log.Printf("normalize: WARNING: produced 0 rows — check input columns (e.g. ClientIP)")
	}
	return nil
}

// ---------- STAGE 2: enrich ----------
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
	_, _, err = reader.Header() // skip input header
	if err != nil {
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
			log.Printf("enrich progress: in=%d out=%d bad=%d", rowsIn, rowsOut, bad)
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

			// 1) target classification
			url := row["target"]
			if url == "" {
				url = row["referring_page"]
			}
			row["target"] = enrich.ResourceTypeFromURL(url)

			// 2) referrer => "Direct Hit" if empty but referring_page is set
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

// ---------- STAGE 3: verify ----------
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

	findCol := func(cands ...string) (string, bool) {
		for _, h := range header {
			hn := strings.TrimSpace(h)
			for _, c := range cands {
				if hn == c {
					return hn, true
				}
			}
		}
		return "", false
	}

	ipCol, ok := findCol("host_ip", "ClientIP", "client_ip", "ip", "remote_addr")
	if !ok {
		return fmt.Errorf("could not find an IP column (tried: host_ip, ClientIP, client_ip, ip, remote_addr)")
	}
	log.Printf("verify: using IP column %q", ipCol)

	normalizeIPKey := func(ip string) string {
		ip = strings.TrimSpace(ip)
		if strings.HasPrefix(ip, "[") && strings.HasSuffix(ip, "]") {
			ip = strings.Trim(ip, "[]")
		}
		return ip
	}

	unique := make(map[string]struct{})
	var totalRows, emptyIPs int

	for {
		row, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			continue
		}
		totalRows++
		ip := normalizeIPKey(row[ipCol])
		if ip == "" || ip == "-" {
			emptyIPs++
			continue
		}
		unique[ip] = struct{}{}
	}

	ips := make([]string, 0, len(unique))
	for ip := range unique {
		ips = append(ips, ip)
	}

	log.Printf("scanned rows=%d, empty_ip=%d, unique_ips=%d", totalRows, emptyIPs, len(ips))
	if len(ips) == 0 {
		return verifier.WriteResultsCSV(outPath, nil)
	}

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

// ---------- Helpers for merge ----------
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

// UA↔PTR base-domain match (heuristic)
func uaPtrSameBaseDomain(ua, ptrBlob string) bool {
	if ua == "" || ptrBlob == "" {
		return false
	}
	ptrDomains := make(map[string]struct{})
	for _, tok := range strings.Split(ptrBlob, "|") {
		d := baseDomain(hostFromAny(tok))
		if d != "" {
			ptrDomains[d] = struct{}{}
		}
	}
	for _, d := range extractDomainsFromUA(ua) {
		bd := baseDomain(d)
		if bd == "" {
			continue
		}
		if _, ok := ptrDomains[bd]; ok {
			return true
		}
	}
	return false
}

func extractDomainsFromUA(ua string) []string {
	ua = strings.ToLower(ua)
	re := regexp.MustCompile(`([a-z0-9][a-z0-9\-]*\.)+[a-z]{2,}`)
	m := re.FindAllString(ua, -1)
	uniq := make(map[string]struct{}, len(m))
	out := make([]string, 0, len(m))
	for _, s := range m {
		s = strings.Trim(s, ".")
		if _, ok := uniq[s]; !ok {
			uniq[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}

func hostFromAny(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimSuffix(s, ".")
	return s
}

func baseDomain(host string) string {
	if host == "" {
		return ""
	}
	parts := strings.Split(host, ".")
	if len(parts) < 2 {
		return ""
	}
	return parts[len(parts)-2] + "." + parts[len(parts)-1]
}

// ---------- STAGE 4: merge ----------
func runMerge(ctx context.Context, finalPath, verifiedPath, outPath string, uaPtrVerify bool) error {
	type verPair struct {
		name string
		flag string // "1" or "0"
	}
	verMap := make(map[string]verPair, 1<<16)

	// Load verified.csv -> map[IP]verPair
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

	// Open final.csv and prepare output
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
				// Merge bot names (union, pipe-delimited)
				row["botName"] = uniqJoinPipe(row["botName"], p.name)
				// Verified from verified.csv
				row["verified"] = p.flag

				// Optional heuristic: UA↔PTR base-domain match => verified=1
				if uaPtrVerify && row["verified"] != "1" {
					ua := strings.ToLower(row["user_agent"])
					ptrBlob := strings.ToLower(row["botName"])
					if uaPtrSameBaseDomain(ua, ptrBlob) {
						row["verified"] = "1"
					}
				}
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

// ---------- STAGE 5: aibots ----------
// Ulaz: merged.csv; Izlaz: merged_ai.csv
// Dodaje novu kolonu "AiBots". Ako UA sadrži neku AI-liniju, upisuje PRVU prepoznatu; inače '-'.
func runAIBots(ctx context.Context, inPath, outPath string) error {
	if inPath == "" || outPath == "" {
		return fmt.Errorf("aibots: --in and --out are required")
	}
	if inPath == outPath {
		return fmt.Errorf("aibots: input and output paths must differ (got %q)", inPath)
	}

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
	_, _, err = reader.Header()
	if err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	// Header = BaseHeader + AiBots
	base := schema.BaseHeader()
	outHeader := append(append([]string(nil), base...), "AiBots")

	writer := csvout.New(out)
	if err := writer.WriteHeader(outHeader); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	var rowsIn, rowsOut, tagged int64
	start := time.Now()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			log.Printf("aibots progress: in=%d out=%d tagged=%d", rowsIn, rowsOut, tagged)
		default:
			row, err := reader.Next()
			if err != nil {
				if err == io.EOF {
					goto DONE
				}
				continue
			}
			rowsIn++

			ai := "-"
			if found := aibots.Detect(row["user_agent"]); len(found) > 0 {
				ai = found[0] // prva detekcija radi preglednosti
				tagged++
			}

			// out row = BaseColumns order + AiBots kao poslednja kolona
			outRow := make([]string, 0, len(base)+1)
			for _, c := range schema.BaseColumns {
				outRow = append(outRow, row[c.Name])
			}
			outRow = append(outRow, ai)

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
	log.Printf("aibots done. in=%d out=%d tagged=%d time=%s", rowsIn, rowsOut, tagged, time.Since(start))
	return nil
}
