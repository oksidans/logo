package csvx

import (
	"bufio"
	"encoding/csv"
	"errors"
	"io"
	"os"
	"strings"
	"time"
)

type CSVStats struct {
	Month int
	Year  int
	Rows  int64
	Min   time.Time
	Max   time.Time
}

// AnalyzeCSV: prolazi kroz CSV i vraća month/year iz PRVOG validnog reda,
// ukupan broj redova, i min/max timestamp.
func AnalyzeCSV(path string) (*CSVStats, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(bufio.NewReaderSize(f, 1<<20)) // 1MB buffer
	r.ReuseRecord = true
	r.FieldsPerRecord = -1 // ne forsiramo fiksan broj kolona

	// header
	header, err := r.Read()
	if err != nil {
		return nil, err
	}
	tsIdx := detectTSColumn(header)
	if tsIdx == -1 {
		return nil, errors.New("AnalyzeCSV: cannot find datetime/timestamp column")
	}

	var (
		stats  CSVStats
		haveTS bool
	)

	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// preskačemo pokvarene linije (po uzoru na robustan streaming)
			continue
		}
		if tsIdx >= len(rec) {
			continue
		}
		tsRaw := strings.TrimSpace(rec[tsIdx])
		if tsRaw == "" {
			continue
		}

		t, ok := parseTS(tsRaw)
		if !ok {
			continue
		}

		// month/year iz PRVOG validnog reda
		if !haveTS {
			stats.Month = int(t.Month())
			stats.Year = t.Year()
			stats.Min = t
			stats.Max = t
			haveTS = true
		} else {
			if t.Before(stats.Min) {
				stats.Min = t
			}
			if t.After(stats.Max) {
				stats.Max = t
			}
		}
		stats.Rows++
	}

	if !haveTS {
		return nil, errors.New("AnalyzeCSV: no valid timestamps found")
	}
	return &stats, nil
}

func detectTSColumn(header []string) int {
	cands := []string{"datetime", "timestamp", "time", "date"}
	for i, h := range header {
		hn := strings.ToLower(strings.TrimSpace(h))
		for _, c := range cands {
			if hn == c {
				return i
			}
		}
	}
	return -1
}

func parseTS(s string) (time.Time, bool) {
	// Pokrivamo tipične formate iz tvojih fajlova
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:05.000000",
		"2006-01-02 15:04:05.000",
		"2006-01-02",
	}
	for _, l := range layouts {
		if t, err := time.ParseInLocation(l, s, time.Local); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}
