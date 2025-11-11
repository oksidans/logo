package verifier

import (
	"context"
	"encoding/csv"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"parser/internal/botdetector"
)

type Result struct {
	IP       string
	BotName  string // bazni PTR domen (ako PTR postoji), inače labela; bez '|'
	Verified bool   // true ako je botdetector prepoznao poznatog bota (po PTR-u)
}

// VerifyIPs – parallel reverse DNS lookup with progress channel.
func VerifyIPs(ctx context.Context, ips []string, workers int, timeout time.Duration, progress chan<- int) ([]Result, error) {
	jobs := make(chan string, workers*2)
	resultsChan := make(chan Result, workers*2)
	var wg sync.WaitGroup

	workerFn := func() {
		defer wg.Done()
		for ip := range jobs {
			select {
			case <-ctx.Done():
				return
			default:
				rCtx, cancel := context.WithTimeout(ctx, timeout)
				ptrs, _ := net.DefaultResolver.LookupAddr(rCtx, ip)
				cancel()

				botNameCanonical, verified := canonicalFromPTRs(ptrs)
				if botNameCanonical == "" {
					botNameCanonical = "unable to verify bot"
				}
				resultsChan <- Result{
					IP:       ip,
					BotName:  botNameCanonical,
					Verified: verified,
				}
				if progress != nil {
					progress <- 1
				}
			}
		}
	}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go workerFn()
	}

	go func() {
		for _, ip := range ips {
			jobs <- ip
		}
		close(jobs)
		wg.Wait()
		close(resultsChan)
	}()

	results := make([]Result, 0, len(ips))
	for res := range resultsChan {
		results = append(results, res)
	}
	return results, nil
}

// ---- helpers ----

// stripNumericPrefix: odbaci vodeće labele koje sadrže cifru.
// "66-249-66-1.googlebot.com"         -> "googlebot.com"
// "crawl-66-249-75-166.googlebot.com" -> "googlebot.com"
// "ec2-47-...ap-southeast-1.compute.amazonaws.com" -> "ap-southeast-1.compute.amazonaws.com"
func stripNumericPrefix(host string) string {
	h := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(host, ".")))
	if h == "" {
		return h
	}
	labels := strings.Split(h, ".")
	i := 0
	for i < len(labels) {
		if strings.IndexFunc(labels[i], func(r rune) bool { return r >= '0' && r <= '9' }) >= 0 {
			i++
			continue
		}
		break
	}
	if i >= len(labels) {
		return h
	}
	return strings.Join(labels[i:], ".")
}

// baseDomain: vrati eTLD+1; podrška za tipične dvoslojne sufikse (.co.uk …).
func baseDomain(host string) string {
	h := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(host, ".")))
	if h == "" {
		return h
	}
	parts := strings.Split(h, ".")
	if len(parts) < 2 {
		return h
	}
	last := parts[len(parts)-1]
	second := parts[len(parts)-2]
	if last == "uk" && (second == "co" || second == "ac" || second == "gov" || second == "ltd" || second == "plc" || second == "org") && len(parts) >= 3 {
		return parts[len(parts)-3] + "." + second + "." + last
	}
	return second + "." + last
}

// canonicalFromPTRs:
// - Ako postoji bar jedan PTR: uzmi prvi PTR, skini numeričke prefikse, pa svedi na eTLD+1 (npr. "googlebot.com").
// - Verified je true ako je bilo koji PTR match-ovan u botdetectoru (labelu NE vraćamo u botName).
// - Ako nema PTR-ova: "", false (caller će upisati "unable to verify bot").
func canonicalFromPTRs(ptrs []string) (string, bool) {
	clean := make([]string, 0, len(ptrs))
	seen := make(map[string]struct{}, len(ptrs))
	for _, p := range ptrs {
		p = strings.TrimSpace(strings.TrimSuffix(p, "."))
		if p == "" {
			continue
		}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		clean = append(clean, p)
	}
	if len(clean) == 0 {
		return "", false
	}

	// verified = true ako je neki PTR poznat
	verified := false
	for _, p := range clean {
		if _, ok := botdetector.Match(p); ok {
			verified = true
			break
		}
	}

	trimmed := stripNumericPrefix(clean[0])
	return baseDomain(trimmed), verified
}

// WriteResultsCSV writes verification results to CSV (verified as "1" or "0").
func WriteResultsCSV(outPath string, results []Result) error {
	f, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	w := csv.NewWriter(f)
	defer w.Flush()

	if err := w.Write([]string{"host_ip", "botName", "verified"}); err != nil {
		return err
	}
	for _, r := range results {
		flag := "0"
		if r.Verified {
			flag = "1"
		}
		if err := w.Write([]string{r.IP, r.BotName, flag}); err != nil {
			return err
		}
	}
	return w.Error()
}
