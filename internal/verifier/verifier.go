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
	BotName  string // DetektovaniBot|PTR1|PTR2|... ili samo PTR-ovi, ili "unable to verify bot"
	Verified bool   // true samo ako je poznati bot detektovan (preko botdetector)
}

// VerifyIPs – parallel reverse DNS lookup with progress channel.
// progress prima +1 po obradi svake IP adrese (može biti nil).
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

				botNameCombined, verified := combineBotNameAndPtrs(ptrs)
				if botNameCombined == "" {
					botNameCombined = "unable to verify bot"
				}
				resultsChan <- Result{
					IP:       ip,
					BotName:  botNameCombined,
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

// combineBotNameAndPtrs vrati:
// - "<DetectedBot>|<PTR1>|<PTR2>|..." ako je poznati bot nađen,
// - "<PTR1>|<PTR2>|..." ako nije,
// - "" ako nema PTR-ova uopšte.
func combineBotNameAndPtrs(ptrs []string) (combined string, verified bool) {
	clean := make([]string, 0, len(ptrs))
	seen := make(map[string]struct{}, len(ptrs))
	for _, p := range ptrs {
		p = strings.TrimSpace(p)
		p = strings.TrimSuffix(p, ".")
		if p == "" {
			continue
		}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		clean = append(clean, p)
	}

	detected := ""
	for _, p := range clean {
		if name, ok := botdetector.Match(p); ok {
			detected = name
			break
		}
	}

	switch {
	case detected != "" && len(clean) > 0:
		return detected + "|" + strings.Join(clean, "|"), true
	case detected != "" && len(clean) == 0:
		return detected, true
	case detected == "" && len(clean) > 0:
		return strings.Join(clean, "|"), false
	default:
		return "", false
	}
}

// WriteResultsCSV writes verification results to CSV (verified as "1" or "0").
func WriteResultsCSV(outPath string, results []Result) error {
	f, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer f.Close()

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
