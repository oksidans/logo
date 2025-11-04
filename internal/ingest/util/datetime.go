package util

import (
	"strings"
	"time"
)

// Grubi parser za tipične formate iz pipeline-a
func ParseDateTimeLoose(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, false
	}
	// "YYYY-MM-DD HH:MM:SS"
	if t, err := time.Parse("2006-01-02 15:04:05", s); err == nil {
		return t, true
	}
	// RFC3339
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, true
	}
	// "YYYY-MM-DD"
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t, true
	}
	// Apache-like: "02/Jan/2006:15:04:05 +0100" → isečemo TZ
	if i := strings.IndexByte(s, ' '); i > 0 {
		if t, err := time.Parse("02/Jan/2006:15:04:05", s[:i]); err == nil {
			return t, true
		}
	}
	// "02/Jan/2006:15:04:05"
	if t, err := time.Parse("02/Jan/2006:15:04:05", s); err == nil {
		return t, true
	}
	// Fallback: T → space
	if strings.Contains(s, "T") {
		s2 := strings.ReplaceAll(s, "T", " ")
		if t, err := time.Parse("2006-01-02 15:04:05", s2); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// MonthWindowUTC vraća [startUTC, endUTC) prozor za dati mesec/godinu u UTC.
// start = YYYY-MM-01 00:00:00 UTC
// end   = (prvog dana sledećeg meseca) 00:00:00 UTC
func MonthWindowUTC(year, month int) (time.Time, time.Time) {
	if month < 1 || month > 12 {
		// fallback: ako je nevažeći mesec, vrati "prazan" prozor
		return time.Time{}, time.Time{}
	}
	loc := time.UTC
	start := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, loc)

	// izračunaj sledeći mesec/godinu
	nextYear, nextMonth := year, month+1
	if nextMonth == 13 {
		nextMonth = 1
		nextYear++
	}
	end := time.Date(nextYear, time.Month(nextMonth), 1, 0, 0, 0, 0, loc)

	return start, end
}

// CoerceUTC vraća istu civilnu vrednost vremena ali u UTC lokaciji.
// Korisno ako želiš da normalizuješ timestampe na UTC bez promene "sata".
func CoerceUTC(t time.Time) time.Time {
	if t.IsZero() {
		return t
	}
	return time.Date(
		t.Year(), t.Month(), t.Day(),
		t.Hour(), t.Minute(), t.Second(), t.Nanosecond(),
		time.UTC,
	)
}
