package verifier

import (
	"context"
	"encoding/csv"
	"net"
	"os"
	"sync"
	"time"

	"parser/internal/botdetector"
)

type Result struct {
	IP       string
	BotName  string
	Verified bool
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
				name := resolveBotName(ptrs)
				resultsChan <- Result{IP: ip, BotName: name, Verified: name != ""}
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

// resolveBotName extracts a bot name based on PTR record content.
func resolveBotName(ptrs []string) string {
	for _, ptr := range ptrs {
		if name, ok := botdetector.Match(ptr); ok {
			return name
		}
	}
	return ""
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

	w.Write([]string{"host_ip", "botName", "verified"})
	for _, r := range results {
		w.Write([]string{r.IP, r.BotName, boolToStr(r.Verified)})
	}
	return w.Error()
}

func boolToStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
