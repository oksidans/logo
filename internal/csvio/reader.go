package csvio

import (
	"encoding/csv"
	"io"
	"os"
)

type Row map[string]string

// StreamCSV returns a channel of rows from a headered CSV.
// Memory-friendly: reads sequentially; caller ranges over the chan.
func StreamCSV(path string) (<-chan Row, func() error, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	r := csv.NewReader(f)
	r.ReuseRecord = true

	header, err := r.Read()
	if err != nil {
		f.Close()
		return nil, nil, err
	}

	out := make(chan Row, 1024)
	cancel := func() error { return f.Close() }

	go func() {
		defer close(out)
		for {
			rec, err := r.Read()
			if err == io.EOF {
				return
			}
			if err != nil {
				// skip corrupted line
				continue
			}
			row := make(Row, len(header))
			for i, h := range header {
				if i < len(rec) {
					row[h] = rec[i]
				}
			}
			out <- row
		}
	}()
	return out, cancel, nil
}
