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
	BotName  string // sada sadrži: DetektovaniBot (ako postoji) + SVI PTR-ovi (spojeni sa '|'), ili "unable to verify bot"
	Verified bool   // true samo ako je detektovan poznati bot preko botdetector pravila
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
				// Ako baš ništa nemamo, zabeleži poruku
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
//   - combined string: "<DetectedBot>|<PTR1>|<PTR2>|..." (ako je poznati bot detektovan)
//     ili samo "<PTR1>|<PTR2>|..." (ako nema detekcije poznatog bota)
//     ili "" ako nema PTR-ova
//   - verified: true samo ako je poznati bot detektovan preko botdetector.Match
func combineBotNameAndPtrs(ptrs []string) (combined string, verified bool) {
	// očisti PTR-ove: trim, bez završne tačke, bez duplikata, sa očuvanjem redosleda
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

	// pokušaj detekcije poznatog bota nad PTR-ovima (prvi pogodak je dovoljan)
	detected := ""
	for _, p := range clean {
		if name, ok := botdetector.Match(p); ok {
			detected = name
			break
		}
	}

	// sastavi combined string
	switch {
	case detected != "" && len(clean) > 0:
		// DetektovaniBot + svi PTR-ovi
		combined = detected + "|" + strings.Join(clean, "|")
		verified = true
	case detected != "" && len(clean) == 0:
		combined = detected
		verified = true
	case detected == "" && len(clean) > 0:
		combined = strings.Join(clean, "|")
		verified = false
	default:
		combined = ""
		verified = false
	}
	return
}

// WriteResultsCSV writes verification results to CSV.
func WriteResultsCSV(outPath string, results []Result) error {
	f, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	// host_ip, botName, verified
	if err := w.Write([]string{"host_ip", "botName", "verified"}); err != nil {
		return err
	}
	for _, r := range results {
		flag := "false"
		if r.Verified {
			flag = "true"
		}
		if err := w.Write([]string{r.IP, r.BotName, flag}); err != nil {
			return err
		}
	}
	return w.Error()
}
