package csvx

import (
	"bufio"
	"encoding/csv"
	"io"
	"os"
	"strings"
	"time"

	"parser/internal/ingest/aggregators"
)

type DebugInfo struct {
	Enabled        bool
	PrintFirstN    int
	TotalRead      int64
	SkipWrongMonth int64
	SkipParseErr   int64
	SkipNoMethod   int64
	SkipNoStatus   int64
	LastHeader     []string
}

var simpleDateLayout = "2006-01-02 15:04:05"

func normalizeName(s string) string { return strings.TrimSpace(strings.ToLower(s)) }

func findCol(header []string, candidates ...string) int {
	lh := make([]string, len(header))
	for i, h := range header {
		lh[i] = normalizeName(h)
	}
	for i := range candidates {
		candidates[i] = normalizeName(candidates[i])
	}
	for i, h := range lh {
		for _, c := range candidates {
			if h == c {
				return i
			}
		}
	}
	return -1
}

// StreamAndAggregate: čita CSV, filtrira po mesecu/godini i puni agg.
// Takođe postavlja agg.MinTS/MaxTS ISKLJUČIVO iz filtriranih redova (target mesec/godina).
func StreamAndAggregate(csvPath string, month, year int, agg *aggregators.AggregateBucket, dbg *DebugInfo) error {
	f, err := os.Open(csvPath)
	if err != nil {
		return err
	}
	defer f.Close()

	r := csv.NewReader(bufio.NewReaderSize(f, 1<<20)) // 1MB buffer
	r.ReuseRecord = true
	r.FieldsPerRecord = -1

	header, err := r.Read()
	if err != nil {
		return err
	}
	if dbg != nil {
		dbg.LastHeader = append([]string(nil), header...)
	}

	iDate := findCol(header, "datetime", "date", "time", "timestamp", "ts")
	iMethod := findCol(header, "method", "http_method")
	iStatus := findCol(header, "status", "status_code", "st_code", "code")
	iReq := findCol(header, "request", "path", "url", "target")

	firstFilteredSeen := false

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			if dbg != nil {
				dbg.SkipParseErr++
			}
			continue
		}
		if dbg != nil {
			dbg.TotalRead++
		}

		// datetime
		if iDate < 0 || iDate >= len(record) {
			if dbg != nil {
				dbg.SkipParseErr++
			}
			continue
		}
		rawTs := strings.TrimSpace(record[iDate])
		if rawTs == "" {
			if dbg != nil {
				dbg.SkipParseErr++
			}
			continue
		}

		// Parsiranje u lokalnoj zoni (po potrebi pređi na fiksnu lokaciju)
		t, perr := time.ParseInLocation(simpleDateLayout, rawTs, time.Local)
		if perr != nil {
			if t2, e2 := time.Parse(time.RFC3339, rawTs); e2 == nil {
				t = t2.In(time.Local)
			} else {
				if dbg != nil {
					dbg.SkipParseErr++
				}
				continue
			}
		}

		// Filtriraj striktno po month/year — ovo eliminiše 31.08 kada obrađujemo 9.
		if int(t.Month()) != month || t.Year() != year {
			if dbg != nil {
				dbg.SkipWrongMonth++
			}
			continue
		}

		// method
		var method string
		if iMethod >= 0 && iMethod < len(record) {
			method = strings.TrimSpace(record[iMethod])
		}
		if method == "" {
			if dbg != nil {
				dbg.SkipNoMethod++
			}
			continue
		}

		// status kao STRING ključ
		var statusStr string
		if iStatus >= 0 && iStatus < len(record) {
			statusStr = strings.TrimSpace(record[iStatus])
		}
		if statusStr == "" {
			if dbg != nil {
				dbg.SkipNoStatus++
			}
			continue
		}

		// META: Min/Max samo iz filtriranih redova
		if !firstFilteredSeen {
			agg.MinTS = t
			agg.MaxTS = t
			firstFilteredSeen = true
		} else {
			if t.Before(agg.MinTS) {
				agg.MinTS = t
			}
			if t.After(agg.MaxTS) {
				agg.MaxTS = t
			}
		}

		// Agregati
		agg.FilteredRows++
		agg.MethodCounts[method]++
		agg.StatusCounts[statusStr]++

		// sitemap heuristika
		if iReq >= 0 && iReq < len(record) {
			req := strings.ToLower(strings.TrimSpace(record[iReq]))
			if strings.Contains(req, "/sitemap.xml") ||
				strings.Contains(req, "/sitemap_index.xml") ||
				(strings.HasSuffix(req, ".xml.gz") && strings.Contains(req, "sitemap")) {
				agg.SitemapCount++
			}
		}
	}

	return nil
}
