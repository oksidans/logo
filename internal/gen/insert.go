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
	"strconv"
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

// truncateRunes skraćuje string na najviše max runa (karaktera).
func truncateRunes(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max])
}

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
// Ostale “By*” tabele – generički slučaj
// (kolona, value, valueProp(6,3), month, year, project_id)
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

// ===== Specijalni slučaj: ln_genBotsMainStatsByVerification =====
// Šema: (id, verified, unverified, month, year, project_id)
// -> upisujemo JEDAN red sa sumama verified/unverified
func InsertByVerification(ctx context.Context, db *sql.DB, p Params) error {
	// Otvori CSV i mapiraj header
	f, err := os.Open(p.CSV)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := f.Close(); cerr != nil {
			log.Printf("[WARN] close CSV failed: %v", cerr)
		}
	}()

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1

	header, err := r.Read()
	if err != nil {
		return err
	}
	hmap := make(map[string]int, len(header))
	for i, h := range header {
		key := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(h, "\ufeff")))
		hmap[key] = i
	}
	idx, ok := hmap["verified"]
	if !ok {
		log.Printf("[WARN] CSV nema kolonu 'verified' (header=%v)", header)
	}

	var ver, unv int64
	readField := func(rec []string, idx int) string {
		if idx < 0 || idx >= len(rec) {
			return ""
		}
		return strings.TrimSpace(rec[idx])
	}

	for {
		rec, rerr := r.Read()
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return rerr
		}
		val := ""
		if ok {
			val = readField(rec, idx)
		}

		l := strings.ToLower(val)
		isTrue := l == "1" || l == "true" || l == "yes" || l == "y"
		if !isTrue {
			if n, err := strconv.Atoi(l); err == nil {
				isTrue = (n != 0)
			}
		}
		if isTrue {
			ver++
		} else {
			unv++
		}
	}

	// Brisanje postojećih redova da izbegnemo duplikate
	if _, err := db.ExecContext(ctx, `
		DELETE FROM ln_genBotsMainStatsByVerification
		WHERE project_id = ? AND month = ? AND year = ?`,
		p.ProjectID, p.Month, p.Year,
	); err != nil {
		return err
	}

	// Upis jednog reda: (verified, unverified, month, year, project_id)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if rerr := tx.Rollback(); rerr != nil && rerr != sql.ErrTxDone {
			log.Printf("[WARN] tx.Rollback failed: %v", rerr)
		}
	}()

	stmt, err := tx.PrepareContext(ctx, insByVerification)
	if err != nil {
		return err
	}
	defer func() {
		if serr := stmt.Close(); serr != nil {
			log.Printf("[WARN] stmt.Close failed: %v", serr)
		}
	}()

	if _, err := stmt.ExecContext(ctx,
		ver, unv, p.Month, p.Year, p.ProjectID,
	); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

// ===== Specijalni slučaj: ln_genBotsMainStatsByRefPage =====
// Šema: (id, url, value, valueProp(6,2), month, year, project_id)
// CSV kolona: "referring_page" -> url
func InsertByRefPage(ctx context.Context, db *sql.DB, p Params) error {
	const col = "referring_page"

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
		log.Printf("[WARN] InsertByRefPage: total=0 – nema redova za agregaciju")
	}

	// value_counts poredak: Count DESC, URL ASC
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

	stmt, err := tx.PrepareContext(ctx, insByRefPage)
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
			// decimal(6,2) => 2 decimale
			prop = roundN((float64(pkv.Count)*100.0)/float64(total), 2)
		}

		// ln_genBotsMainStatsByRefPage.url je VARCHAR(4050)
		url := truncateRunes(pkv.Name, 4050)

		if _, err := stmt.ExecContext(ctx,
			url, pkv.Count, prop,
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

// ===== Specijalni slučaj: ln_genBotsMainStatsByTarget =====
// Šema: (id, target, value, valueProp(6,2), month, year, project_id)
// CSV kolona: "target"
func InsertByTarget(ctx context.Context, db *sql.DB, p Params) error {
	const col = "target"

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
		log.Printf("[WARN] InsertByTarget: total=0 – nema redova za agregaciju")
	}

	// value_counts poredak: Count DESC, Name ASC
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

	stmt, err := tx.PrepareContext(ctx, insByTarget)
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
			// decimal(6,2) => 2 decimale
			prop = roundN((float64(pkv.Count)*100.0)/float64(total), 2)
		}

		// target je VARCHAR(45)
		target := truncateRunes(pkv.Name, 45)

		if _, err := stmt.ExecContext(ctx,
			target, pkv.Count, prop,
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

// ===== Specijalni slučaj: ln_genBotsMainStatsByProtVersion =====
// Šema: (id, protocol, value, valueProp(6,3), month, year, project_id)
// CSV kolona: "protocol"
func InsertByProtVersion(ctx context.Context, db *sql.DB, p Params) error {
	const col = "protocol"

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
		log.Printf("[WARN] InsertByProtVersion: total=0 – nema redova za agregaciju")
	}

	// value_counts poredak: Count DESC, Name ASC
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

	stmt, err := tx.PrepareContext(ctx, insByProtVersion)
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
			// decimal(6,3) => 3 decimale
			prop = roundN((float64(pkv.Count)*100.0)/float64(total), 3)
		}

		// protocol je VARCHAR(50)
		protocol := truncateRunes(pkv.Name, 50)

		if _, err := stmt.ExecContext(ctx,
			protocol, pkv.Count, prop,
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

// ===== Specijalni slučaj: ln_genBotsMainStatsBySitemap =====
// Šema: (id, url, value, valueProp(6,2), month, year, project_id)
// Logika: URL je iz kolone "referring_page",
// ali uzimamo SAMO one redove gde URL ima "sitemap" ili se završava na ".xml".
func InsertBySitemap(ctx context.Context, db *sql.DB, p Params) error {
	const urlCol = "referring_page"

	next, hmap, closef, err := readCSV(p.CSV)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := closef(); cerr != nil {
			log.Printf("[WARN] close CSV failed: %v", cerr)
		}
	}()

	if _, ok := hmap[strings.ToLower(urlCol)]; !ok {
		log.Printf("[WARN] CSV nema kolonu %q (header=%v)", urlCol, hmap)
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

		url := norm(getField(rec, hmap, urlCol))
		if url == "" {
			continue
		}
		low := strings.ToLower(url)

		// Uzimamo samo URL-ove koji liče na sitemap:
		// sadrže "sitemap" ili se završavaju na ".xml"
		if !strings.Contains(low, "sitemap") && !strings.HasSuffix(low, ".xml") {
			continue
		}

		counts[url]++
		total++
	}

	if total == 0 {
		log.Printf("[WARN] InsertBySitemap: total=0 – nema redova za agregaciju")
	}

	// value_counts poredak: Count DESC, URL ASC
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

	stmt, err := tx.PrepareContext(ctx, insBySitemap)
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
			// decimal(6,2) => 2 decimale
			prop = roundN((float64(pkv.Count)*100.0)/float64(total), 2)
		}

		// url je VARCHAR(4500)
		url := truncateRunes(pkv.Name, 4500)

		if _, err := stmt.ExecContext(ctx,
			url, pkv.Count, prop,
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
