package gen

import (
	"context"
	"database/sql"
	"encoding/csv"
	"io"
	"log"
	"math"
	"os"
	"regexp"
	"sort"
	"strings"
	"unicode"
)

type Params struct {
	CSV       string
	ProjectID int64
	Month     int
	Year      int
}

func inc(m map[string]int64, k string) { m[k]++ }
func norm(s string) string             { return strings.TrimSpace(s) }

// roundN zaokružuje na n decimala
func roundN(x float64, n int) float64 {
	p := math.Pow(10, float64(n))
	return math.Round(x*p) / p
}

// --- Robustan CSV reader (',' delimiter, header mapiranje) ---

// readCSV otvara CSV, vraća header map (ime kolone -> index) i iterator preko redova ([]string)
func readCSV(path string) (func() ([]string, error), map[string]int, func() error, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, nil, err
	}

	r := csv.NewReader(f)
	r.Comma = ','
	r.LazyQuotes = true
	r.TrimLeadingSpace = true
	r.FieldsPerRecord = -1 // dozvoli promenljiv broj polja

	header, err := r.Read()
	if err != nil {
		_ = f.Close()
		return nil, nil, nil, err
	}

	// mapiraj header case/space-insensitive; ukloni BOM ako postoji
	hmap := make(map[string]int, len(header))
	for i, h := range header {
		key := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(h, "\ufeff")))
		hmap[key] = i
	}
	log.Printf("[DEBUG] CSV header keys: %v", header)

	next := func() ([]string, error) {
		rec, err := r.Read()
		if err != nil {
			return nil, err
		}
		return rec, nil
	}
	closef := func() error { return f.Close() }
	return next, hmap, closef, nil
}

func getField(rec []string, hmap map[string]int, want string) string {
	idx, ok := hmap[strings.ToLower(strings.TrimSpace(want))]
	if !ok || idx < 0 || idx >= len(rec) {
		return ""
	}
	return strings.TrimSpace(rec[idx])
}

// === helperi za kanonizaciju botName ===

// stripNumericPrefix: odbaci vodeće labele koje sadrže cifru
// npr. "66-249-66-1.googlebot.com" -> "googlebot.com"
func stripNumericPrefix(host string) string {
	h := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(host, ".")))
	if h == "" {
		return h
	}
	labels := strings.Split(h, ".")
	i := 0
	for i < len(labels) {
		hasDigit := false
		for _, r := range labels[i] {
			if unicode.IsDigit(r) {
				hasDigit = true
				break
			}
		}
		if hasDigit {
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

// baseDomain vraća eTLD+1 za tipične slučajeve (heuristika, uključuje .co.uk varijantu)
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
	// gruba podrška za UK višeslojne TLD-ove
	if last == "uk" && (second == "co" || second == "ac" || second == "gov" || second == "ltd" || second == "plc" || second == "org") && len(parts) >= 3 {
		return parts[len(parts)-3] + "." + second + "." + last
	}
	return second + "." + last
}

// canonicalBot proizvodi željeni oblik:
// - ako je format "Label|PTR", vrati bazni domen iz PTR-a (npr. "googlebot.com")
// - ako nema PTR-a, vrati sirovu vrednost (labelu)
// - ako je već domen, ostavi ga
func canonicalBot(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return s
	}
	if strings.Contains(s, "|") {
		parts := strings.Split(s, "|")
		last := strings.TrimSpace(parts[len(parts)-1]) // očekujemo PTR na kraju
		return baseDomain(stripNumericPrefix(last))
	}
	return s
}

// ==============================
// ln_genBotsMainStats — value_counts(botName), proporcija i isNumeric
// ==============================
func InsertMain(ctx context.Context, db *sql.DB, p Params) error {
	next, hmap, closef, err := readCSV(p.CSV)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := closef(); cerr != nil {
			log.Printf("[WARN] close CSV failed: %v", cerr)
		}
	}()

	if _, ok := hmap["botname"]; !ok {
		log.Printf("[WARN] CSV nema kolonu 'botName' (header=%v)", hmap)
	}

	counts := make(map[string]int64)
	var total int64

	for {
		rec, rerr := next()
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return rerr
		}
		raw := norm(getField(rec, hmap, "botName"))
		if raw == "" {
			continue
		}
		bot := canonicalBot(raw)
		if bot == "" || bot == "unable to verify bot" {
			continue
		}
		inc(counts, bot)
		total++
	}

	if total == 0 {
		log.Printf("[WARN] InsertMain: total=0 – nema redova za agregaciju")
	}

	// value_counts poredak: Count DESC, Name ASC
	type kv struct {
		Name  string
		Count int64
	}
	pairs := make([]kv, 0, len(counts))
	for name, c := range counts {
		pairs = append(pairs, kv{name, c})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].Count == pairs[j].Count {
			return pairs[i].Name < pairs[j].Name
		}
		return pairs[i].Count > pairs[j].Count
	})

	numericPattern := regexp.MustCompile(`[0-9]`)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	// linter-safe rollback
	defer func() {
		if rerr := tx.Rollback(); rerr != nil && rerr != sql.ErrTxDone {
			log.Printf("[WARN] tx.Rollback failed: %v", rerr)
		}
	}()

	stmt, err := tx.PrepareContext(ctx, insMain)
	if err != nil {
		return err
	}
	defer func() {
		if serr := stmt.Close(); serr != nil {
			log.Printf("[WARN] stmt.Close failed: %v", serr)
		}
	}()

	for _, pkv := range pairs {
		isNumeric := 0
		if numericPattern.MatchString(pkv.Name) {
			isNumeric = 1
		}
		// % u dva decimala
		prop := 0.0
		if total > 0 {
			prop = roundN((float64(pkv.Count)*100.0)/float64(total), 2)
		}

		if _, err := stmt.ExecContext(ctx,
			pkv.Name, pkv.Count, prop, isNumeric,
			p.Month, p.Year, p.ProjectID,
		); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

// ==============================
// Ostale “By*” tabele – (source, value, valueProp, month, year, project_id)
// valueProp = % u tri decimale
// ==============================
func insertByCol(ctx context.Context, db *sql.DB, p Params, col, insertSQL string) error {
	next, hmap, closef, err := readCSV(p.CSV)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := closef(); cerr != nil {
			log.Printf("[WARN] close CSV failed: %v", cerr)
		}
	}()

	if _, ok := hmap[strings.ToLower(col)]; !ok {
		log.Printf("[WARN] CSV nema kolonu %q (header=%v)", col, hmap)
	}

	counts := make(map[string]int64)
	var total int64

	for {
		rec, rerr := next()
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return rerr
		}
		v := norm(getField(rec, hmap, col))
		if v == "" {
			v = "(unknown)"
		}
		counts[v]++
		total++
	}

	if total == 0 {
		log.Printf("[WARN] insertByCol(%s): total=0 – nema redova za agregaciju", col)
	}

	// value_counts poredak i ovde (stabilnost između env-ova)
	type kv struct {
		Name  string
		Count int64
	}
	pairs := make([]kv, 0, len(counts))
	for n, c := range counts {
		pairs = append(pairs, kv{n, c})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].Count == pairs[j].Count {
			return pairs[i].Name < pairs[j].Name
		}
		return pairs[i].Count > pairs[j].Count
	})

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if rerr := tx.Rollback(); rerr != nil && rerr != sql.ErrTxDone {
			log.Printf("[WARN] tx.Rollback failed: %v", rerr)
		}
	}()

	stmt, err := tx.PrepareContext(ctx, insertSQL)
	if err != nil {
		return err
	}
	defer func() {
		if serr := stmt.Close(); serr != nil {
			log.Printf("[WARN] stmt.Close failed: %v", serr)
		}
	}()

	for _, pkv := range pairs {
		prop := 0.0
		if total > 0 {
			prop = roundN((float64(pkv.Count)*100.0)/float64(total), 3)
		}
		if _, err := stmt.ExecContext(ctx,
			pkv.Name, pkv.Count, prop,
			p.Month, p.Year, p.ProjectID,
		); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func InsertBySource(ctx context.Context, db *sql.DB, p Params) error {
	return insertByCol(ctx, db, p, "source", insBySource)
}
func InsertByMethod(ctx context.Context, db *sql.DB, p Params) error {
	return insertByCol(ctx, db, p, "method", insByMethod)
}
func InsertByVerification(ctx context.Context, db *sql.DB, p Params) error {
	return insertByCol(ctx, db, p, "verified", insByVerification)
}
func InsertByRefPage(ctx context.Context, db *sql.DB, p Params) error {
	return insertByCol(ctx, db, p, "referring_page", insByRefPage)
}
func InsertByTarget(ctx context.Context, db *sql.DB, p Params) error {
	return insertByCol(ctx, db, p, "target", insByTarget)
}
