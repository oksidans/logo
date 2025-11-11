package mapper

import (
	"net/url"
	"strconv"
	"strings"
	"time"

	"parser/internal/csvin"
	"parser/internal/schema"
)

const rfc3339 = time.RFC3339

// defaultScheme je fallback kada nema ClientRequestScheme i ne možemo
// da izvučemo šemu iz referera.
var defaultScheme = "https"

// SetDefaultScheme omogućava promena podrazumevane šeme iz main-a (--default-scheme).
func SetDefaultScheme(s string) {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "http" || s == "https" {
		defaultScheme = s
	}
}

// parseSchemeFromRef pokušava da izdvoji http/https iz referera.
func parseSchemeFromRef(ref string) string {
	ref = strings.TrimSpace(ref)
	if ref == "" || ref == "-" {
		return ""
	}
	// Brza detekcija bez kompletnog parsiranja
	if strings.HasPrefix(ref, "https://") {
		return "https"
	}
	if strings.HasPrefix(ref, "http://") {
		return "http"
	}
	// Pokušaj standardnog parsiranja
	if u, err := url.Parse(ref); err == nil && u.Scheme != "" {
		if u.Scheme == "http" || u.Scheme == "https" {
			return u.Scheme
		}
	}
	return ""
}

// pickScheme bira šemu po prioritetu: direktna kolona -> iz referera -> default.
func pickScheme(clientReqScheme, referer string) string {
	crs := strings.ToLower(strings.TrimSpace(clientReqScheme))
	if crs == "http" || crs == "https" {
		return crs
	}
	if s := parseSchemeFromRef(referer); s != "" {
		return s
	}
	return defaultScheme
}

// absoluteFrom konstruiše apsolutni URL od host+uri koristeći šemu.
// Ako nema host, pokuša da izvuče host i šemu iz referrera.
// Ako nema dovoljno informacija, vraća relativni uri.
func absoluteFrom(uri, host, clientReqScheme, referer string) string {
	uri = strings.TrimSpace(uri)
	host = strings.TrimSpace(host)

	scheme := pickScheme(clientReqScheme, referer)

	// Ako nemamo host, probaj iz referera
	if host == "" {
		if u, err := url.Parse(referer); err == nil && u.Host != "" {
			host = u.Host
			// scheme već dolazi iz pickScheme (ref/CRS/default), pa je konzistentan
		}
	}

	// Ako i dalje nema hosta, vrati relativni URI (bolje išta nego ništa)
	if host == "" {
		if uri == "" {
			return ""
		}
		// Ako je uri već apsolutan, samo ga vrati
		if u, err := url.Parse(uri); err == nil && u.IsAbs() {
			return uri
		}
		return uri
	}

	// Izgradi apsolutni URL
	if uri == "" {
		return scheme + "://" + host
	}
	// Ako je uri apsolutan, vrati ga (ali zadrži doslednost — preferiramo naš scheme/host)
	if u, err := url.Parse(uri); err == nil && u.IsAbs() {
		return uri
	}
	// U normalnom slučaju, uri je relativan path/query
	if !strings.HasPrefix(uri, "/") {
		uri = "/" + uri
	}
	return scheme + "://" + host + uri
}

// MapToCSV: raw input row -> Stage 1 base row (order = BaseColumns).
func MapToCSV(src map[string]string) []string {
	out := make([]string, len(schema.BaseColumns))
	get := func(k string) string { return csvin.MustGet(src, k) }

	// datetime i izvedeni delovi
	dt := get("EdgeEndTimestamp")
	day, month, year := "", "", ""
	timeZone := "" // po zahtevu: "YYYY-MM-DD HH:MM:SS" bez time zone oznake
	if dt != "" {
		if t, err := time.Parse(rfc3339, dt); err == nil {
			day = strconv.Itoa(t.Day())
			month = strconv.Itoa(int(t.Month()))
			year = strconv.Itoa(t.Year())
			// format "2006-01-02 15:04:05" (bez vremenske zone)
			timeZone = t.Format("2006-01-02 15:04:05")
		}
	}

	// odredi šemu i apsolutni URL za "naš" resurs (referring_page)
	// koristimo ClientRequestURI (relativan) + ClientRequestHost + ClientRequestScheme
	absReferring := absoluteFrom(
		get("ClientRequestURI"),
		get("ClientRequestHost"),
		get("ClientRequestScheme"),
		get("ClientRequestReferer"),
	)

	// target (ono što je traženo) — uzmi path ili fallback na URI
	target := get("ClientRequestPath")
	if target == "" {
		target = get("ClientRequestURI")
	}

	// status/size
	status := get("EdgeResponseStatus")
	size := get("EdgeResponseBytes")

	// user agent & trivijalna detekcija botova (po UA; prava verifikacija ide kasnije)
	ua := get("ClientRequestUserAgent")
	lcUA := strings.ToLower(ua)
	botName := ""
	for _, b := range []string{"googlebot", "bingbot", "duckduckbot", "ahrefsbot", "semrushbot", "yandexbot"} {
		if strings.Contains(lcUA, b) {
			switch b {
			case "googlebot":
				botName = "googlebot.com"
			case "bingbot":
				botName = "Bingbot"
			case "duckduckbot":
				botName = "DuckDuckBot"
			case "ahrefsbot":
				botName = "AhrefsBot"
			case "semrushbot":
				botName = "SemrushBot"
			case "yandexbot":
				botName = "YandexBot"
			}
			break
		}
	}

	// verified: ako VerifiedBotCategory nije prazan, označi kao "1"
	verified := ""
	if v := strings.TrimSpace(get("VerifiedBotCategory")); v != "" && v != "-" {
		verified = "1"
	}

	// protocol: biramo kroz pickScheme (dosledno sa referring_page)
	protocol := pickScheme(get("ClientRequestScheme"), get("ClientRequestReferer"))

	// Popunjavanje u Stage 1 redosledu
	for i, c := range schema.BaseColumns {
		switch c.Name {
		case "host_ip":
			out[i] = get("ClientIP")
		case "time_zone":
			out[i] = timeZone
		case "status_code":
			out[i] = status
		case "size":
			out[i] = size
		case "referrer":
			// originalni referrer (Stage 2 može da ga zameni sa "Direct Hit" po pravilu)
			out[i] = get("ClientRequestReferer")
		case "user_agent":
			out[i] = ua
		case "method":
			out[i] = get("ClientRequestMethod")
		case "referring_page":
			// apsolutni URL traženog resursa (naš URL), konzistentan sa "protocol"
			out[i] = absReferring
		case "protocol":
			out[i] = protocol
		case "day":
			out[i] = day
		case "month":
			out[i] = month
		case "year":
			out[i] = year
		case "source":
			out[i] = get("ClientDeviceType")
		case "target":
			out[i] = target
		case "botName":
			out[i] = botName
		case "verified":
			out[i] = verified
		case "datetime":
			out[i] = dt
		default:
			out[i] = ""
		}
	}
	return out
}
