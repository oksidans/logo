package jsonl

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/bytedance/sonic"
)

type Options struct {
	Workers  int    // broj radnika (goroutines)
	TempDir  string // direktorijum za privremene fajlove; "" -> os.TempDir()
	BufLines int    // veličina bafera za jobs kanal (linije)
}

// ConvertJSONLToCSVConcurrent: brza konverzija JSONL -> CSV u 2 faze.
// Faza 1: paralelno parsiranje + flatten, upis flattened redova u temp fajlove, skupljanje unije ključeva.
// Faza 2: piše CSV header (unija ključeva) i onda redove iz temp fajlova po header redosledu.
func ConvertJSONLToCSVConcurrent(inPath, outPath string, opt Options) error {
	if inPath == "" || outPath == "" {
		return fmt.Errorf("jsonl: --in and --out are required")
	}
	if inPath == outPath {
		return fmt.Errorf("jsonl: input and output paths must differ (got %q)", inPath)
	}
	if opt.Workers <= 0 {
		opt.Workers = 8
	}
	if opt.BufLines <= 0 {
		opt.BufLines = 8192
	}
	tmpDir := opt.TempDir
	if tmpDir == "" {
		tmpDir = os.TempDir()
	}

	// === Pass 1: streamuj JSONL u radnike ===
	in, r, err := openMaybeGzip(inPath)
	if err != nil {
		return fmt.Errorf("open input: %w", err)
	}
	defer in.Close()

	type job struct {
		line []byte
	}
	jobs := make(chan job, opt.BufLines)

	// svaki radnik upisuje u svoj temp fajl (manje contention-a)
	tmpFiles := make([]*os.File, opt.Workers)
	for i := 0; i < opt.Workers; i++ {
		f, err := os.CreateTemp(tmpDir, fmt.Sprintf("jsonlrows_w%02d_*.jsonl", i))
		if err != nil {
			return fmt.Errorf("create temp file: %w", err)
		}
		tmpFiles[i] = f
	}

	var (
		keysMu  sync.Mutex
		allKeys = make(map[string]struct{})
		wg      sync.WaitGroup
	)

	// Worker fn
	worker := func(idx int, out *os.File) {
		defer wg.Done()
		// veliki bafer za upis (1MB)
		w := bufio.NewWriterSize(out, 1<<20)

		for j := range jobs {
			line := j.line
			if len(strings.TrimSpace(string(line))) == 0 {
				continue
			}

			var v any
			// SONIC: 3-5x brži od encoding/json u praksi
			if err := sonic.Unmarshal(line, &v); err != nil {
				// preskoči lošu liniju
				continue
			}

			flat := make(map[string]string, 32)
			flatten("", v, flat)

			// upiši flatten kao JSON mapu (jedna linija) — SONIC marshal
			b, err := sonic.Marshal(flat)
			if err == nil {
				_, _ = w.Write(b)
				_, _ = w.Write([]byte("\n"))
			}

			// skupi ključeve (unija)
			keysMu.Lock()
			for k := range flat {
				allKeys[k] = struct{}{}
			}
			keysMu.Unlock()
		}
		_ = w.Flush()
	}

	// pokreni radnike
	wg.Add(opt.Workers)
	for i := 0; i < opt.Workers; i++ {
		go worker(i, tmpFiles[i])
	}

	// čitanje ulaza sa velikim baferom (1MB)
	br := bufio.NewReaderSize(r, 1<<20)
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			cp := make([]byte, len(line))
			copy(cp, line)
			jobs <- job{line: cp}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			// na grešku iz I/O izlazimo, većina obrađenog je u tmp fajlovima
			break
		}
	}
	close(jobs)
	wg.Wait()

	// formiraj stabilan header
	if len(allKeys) == 0 {
		// napiši prazan CSV (bez kolona)
		out, err := os.Create(outPath)
		if err != nil {
			return fmt.Errorf("create output: %w", err)
		}
		out.Close()
		for _, f := range tmpFiles {
			_ = os.Remove(f.Name())
		}
		return nil
	}
	keys := make([]string, 0, len(allKeys))
	for k := range allKeys {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// === Pass 2: piši CSV iz temp fajlova ===
	out, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer out.Close()

	cw := csv.NewWriter(out)
	if err := cw.Write(keys); err != nil {
		return err
	}

	row := make([]string, len(keys))
	for _, tf := range tmpFiles {
		// reset file pos
		if _, err := tf.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("seek temp: %w", err)
		}
		tr := bufio.NewReaderSize(tf, 1<<20)

		for {
			// čitamo liniju (jedan flattened red kao JSON)
			l, err := tr.ReadBytes('\n')
			if len(l) > 0 {
				var flat map[string]string
				if err := sonic.Unmarshal(l, &flat); err == nil {
					for i, k := range keys {
						row[i] = flat[k]
					}
					if err := cw.Write(row); err != nil {
						return fmt.Errorf("write row: %w", err)
					}
				}
			}
			if err != nil {
				if err == io.EOF {
					break
				}
				// na druge greške preskačemo dalje
				break
			}
		}
	}

	cw.Flush()
	if err := cw.Error(); err != nil {
		return err
	}

	// očisti temp fajlove
	for _, f := range tmpFiles {
		name := f.Name()
		_ = f.Close()
		_ = os.Remove(name)
	}

	return nil
}

func openMaybeGzip(path string) (*os.File, io.Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	var rr io.Reader = f
	// podrška i za .gz ulaz (automatski)
	if strings.EqualFold(filepath.Ext(path), ".gz") {
		gzr, gzerr := gzip.NewReader(f)
		if gzerr != nil {
			f.Close()
			return nil, nil, gzerr
		}
		rr = gzr
	}
	return f, rr, nil
}

// flatten pretvara JSON vrednost u "flat" mapu sa dot.notation ključevima.
func flatten(prefix string, v any, out map[string]string) {
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			key := k
			if prefix != "" {
				key = prefix + "." + k
			}
			flatten(key, val, out)
		}
	case []any:
		// niz kao JSON string — SONIC marshal
		if b, err := sonic.Marshal(t); err == nil {
			out[prefix] = string(b)
		} else {
			out[prefix] = "[]"
		}
	case string:
		out[prefix] = t
	case float64:
		// JSON broj -> string bez suvišnih nula kada je moguće
		s := fmt.Sprintf("%v", t)
		if strings.Contains(s, ".") {
			s = strings.TrimRight(s, "0")
			s = strings.TrimRight(s, ".")
		}
		out[prefix] = s
	case bool:
		if t {
			out[prefix] = "true"
		} else {
			out[prefix] = "false"
		}
	case nil:
		// ostavi prazno
	default:
		// fallback: JSON-encode — SONIC
		if b, err := sonic.Marshal(t); err == nil {
			out[prefix] = string(b)
		} else {
			out[prefix] = ""
		}
	}
}
