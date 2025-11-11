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
	"strings"
)

type Params struct {
	CSV       string
	ProjectID int64
	Month     int
	Year      int
}

func inc(m map[string]int64, k string) { m[k]++ }
func norm(s string) string             { return strings.TrimSpace(s) }

// roundN zaokružuje na n decimala (bankers-not needed; dovoljno HalfUp)
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
		f.Close()
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

// ==============================
// ln_genBotsMainStats — value_counts(botName), proporcija i isNumeric
// ==============================
func InsertMain(ctx context.Context, db *sql.DB, p Params) error {
	next, hmap, closef, err := readCSV(p.CSV)
	if err != nil {
		return err
	}
	defer closef()

	if _, ok := hmap["botname"]; !ok {
		log.Printf("[WARN] CSV nema kolonu 'botName' (header=%v)", hmap)
	}

	counts := make(map[string]int64)
	var total int64

	for {
		rec, err := next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		bot := norm(getField(rec, hmap, "botName"))
		if bot == "" {
			continue
		}
		inc(counts, bot)
		total++
	}

	if total == 0 {
		log.Printf("[WARN] InsertMain: total=0 – nema redova za agregaciju")
	}

	// “sumnjivi” botovi: ime sadrži cifre
	numericPattern := regexp.MustCompile(`[0-9]`)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, insMain)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	for bot, c := range counts {
		isNumeric := 0
		if numericPattern.MatchString(bot) {
			isNumeric = 1
		}

		// proporcija po Python logici: udeo ovog bota u ukupnom uzorku
		// tabela ima DECIMAL(6,2) -> zaokruži na 2 decimale (u procentima ili frakciji?)
		// Uobičajeno: procenat. Ako želiš frakciju 0..1, zameni 100.0 sa 1.0 i zaokruži na 4+ dec.
		prop := 0.0
		if total > 0 {
			prop = roundN((float64(c)*100.0)/float64(total), 2) // npr. 12.34 (%)
		}

		if _, err := stmt.ExecContext(ctx,
			bot,  // botName
			c,    // botStats
			prop, // botStatsProp (procenat)
			isNumeric,
			p.Month,
			p.Year,
			p.ProjectID,
		); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

// ==============================
// Ostale “By*” tabele – (source, value, valueProp, month, year, project_id)
// valueProp = udeo te kategorije u ukupnom broju redova
// ==============================
func insertByCol(ctx context.Context, db *sql.DB, p Params, col, insertSQL string) error {
	next, hmap, closef, err := readCSV(p.CSV)
	if err != nil {
		return err
	}
	defer closef()

	if _, ok := hmap[strings.ToLower(col)]; !ok {
		log.Printf("[WARN] CSV nema kolonu %q (header=%v)", col, hmap)
	}

	counts := make(map[string]int64)
	var total int64

	for {
		rec, err := next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
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

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, insertSQL)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	// DECIMAL(6,3) u By* tabelama -> zaokružimo na 3 decimale (procenat)
	for source, c := range counts {
		prop := 0.0
		if total > 0 {
			prop = roundN((float64(c)*100.0)/float64(total), 3)
		}
		if _, err := stmt.ExecContext(ctx,
			source, // source
			c,      // value
			prop,   // valueProp (%)
			p.Month,
			p.Year,
			p.ProjectID,
		); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
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
