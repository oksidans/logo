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
	BotName  string // "DetectedBot|" ili "basedomain.tld" ili "unable to verify bot"
	Verified bool   // true samo ako je poznati bot detektovan (preko botdetector)
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

				botNameCanonical, verified := combineBotNameAndPtrs(ptrs)
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

// baseDomain: vrati eTLD+1 (heuristika, dovoljno za naše potrebe)
func baseDomain(host string) string {
	h := strings.ToLower(strings.TrimSpace(host))
	h = strings.TrimSuffix(h, ".")
	if h == "" {
		return h
	}
	parts := strings.Split(h, ".")
	if len(parts) < 2 {
		return h
	}
	last := parts[len(parts)-1]
	second := parts[len(parts)-2]
	// podrška za *.co.uk, *.ac.uk, ...
	if last == "uk" && (second == "co" || second == "ac" || second == "gov" || second == "ltd" || second == "plc" || second == "org") && len(parts) >= 3 {
		return parts[len(parts)-3] + "." + second + "." + last
	}
	return second + "." + last
}

// stripNumericPrefix: odbaci vodeće labele koje sadrže bar jednu cifru
// npr. "66-249-66-1.googlebot.com" -> "googlebot.com"
//
//	"crawl-66-249-75-166.googlebot.com" -> "googlebot.com"
//	"ec2-47-128-...ap-southeast-1.compute.amazonaws.com" -> "ap-southeast-1.compute.amazonaws.com"
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
		// dozvoli i prefikse koji su “alfa+brojevi”, ali ako imaju broj – tretiramo ih kao numeričke
		// (gore već pokriveno: “ima cifru”)
		break
	}
	if i >= len(labels) {
		// sve su numeričke — vrati original (bolje nego prazan)
		return h
	}
	return strings.Join(labels[i:], ".")
}

// combineBotNameAndPtrs sa novom logikom:
// - ako je poznat bot: "DetectedBot|"
// - inače: uzmi prvi PTR, ukloni numeričke prefikse, pa eTLD+1 (npr. googlebot.com)
// - ako nema PTR: ""
func combineBotNameAndPtrs(ptrs []string) (combined string, verified bool) {
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

	// pokušaj detekcije poznatog bota
	detected := ""
	for _, p := range clean {
		if name, ok := botdetector.Match(p); ok {
			detected = name
			break
		}
	}

	if detected != "" {
		return detected + "|", true
	}

	if len(clean) > 0 {
		trimmed := stripNumericPrefix(clean[0])
		return baseDomain(trimmed), false
	}

	return "", false
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
