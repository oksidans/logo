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

// absoluteFrom pravi apsolutni URL za traženi resurs.
// Prioritet biranja šeme (http/https):
//  1. reqScheme (npr. ClientRequestScheme, ako je popunjen: "http" ili "https")
//  2. šema iz referrera (ako je validan URL i http/https)
//  3. "https" kao podrazumevana
//
// Prioritet hosta:
//  1. reqHost (ClientRequestHost)
//  2. host iz referrera
//
// Posebno: ako je uri već apsolutan -> vrati ga; ako je "//cdn..." -> prefiksuj šemu.
func absoluteFrom(uri, reqHost, reqScheme, referrer string) string {
	u := strings.TrimSpace(uri)
	if u == "" || u == "-" {
		return ""
	}

	// Već apsolutan?
	if parsed, err := url.Parse(u); err == nil && parsed.IsAbs() {
		return parsed.String()
	}

	// Uhvati šemu iz referrera (ako postoji)
	refScheme := ""
	refHost := ""
	if rp, err := url.Parse(strings.TrimSpace(referrer)); err == nil && rp != nil {
		refScheme = strings.ToLower(rp.Scheme)
		refHost = rp.Host
	}

	// Normalizuj reqScheme
	rs := strings.ToLower(strings.TrimSpace(reqScheme))
	if rs != "http" && rs != "https" {
		rs = ""
	}
	// Ako imamo protocol-relative URL: //cdn.example.com/path
	if strings.HasPrefix(u, "//") {
		// šema: reqScheme -> referrer -> https
		scheme := rs
		if scheme == "" {
			if refScheme == "http" || refScheme == "https" {
				scheme = refScheme
			} else {
				scheme = "https"
			}
		}
		return scheme + ":" + u
	}

	// Osnovni host: ClientRequestHost ili host iz referrera
	baseHost := strings.TrimSpace(reqHost)
	if baseHost == "" {
		baseHost = refHost
	}
	if baseHost == "" {
		// Nemamo host — vrati relativan kakav jeste
		return u
	}

	// Šema: reqScheme -> referrer -> https
	scheme := rs
	if scheme == "" {
		if refScheme == "http" || refScheme == "https" {
			scheme = refScheme
		} else {
			scheme = "https"
		}
	}

	baseURL := &url.URL{Scheme: scheme, Host: baseHost}
	rel, err := url.Parse(u)
	if err != nil {
		return u
	}
	return baseURL.ResolveReference(rel).String()
}

// MapToCSV: raw input row -> Stage 1 base row (order = BaseColumns).
func MapToCSV(src map[string]string) []string {
	out := make([]string, len(schema.BaseColumns))

	get := func(k string) string { return csvin.MustGet(src, k) }

	// datetime parts
	dt := get("EdgeEndTimestamp")
	day, month, year := "", "", ""
	if dt != "" {
		if t, err := time.Parse(rfc3339, dt); err == nil {
			day = strconv.Itoa(t.Day())
			month = strconv.Itoa(int(t.Month()))
			year = strconv.Itoa(t.Year())
		}
	}

	// target (ostaje isto; Stage 2 će "target" prepisati tipom resursa)
	target := get("ClientRequestPath")
	if target == "" {
		target = get("ClientRequestURI")
	}

	status := get("EdgeResponseStatus")
	size := get("EdgeResponseBytes")

	ua := get("ClientRequestUserAgent")
	lcUA := strings.ToLower(ua)
	botName := ""
	for _, b := range []string{"googlebot", "bingbot", "duckduckbot", "ahrefsbot", "semrushbot", "yandexbot"} {
		if strings.Contains(lcUA, b) {
			switch b {
			case "googlebot":
				botName = "Googlebot"
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

	verified := ""
	if v := get("VerifiedBotCategory"); v != "" {
		verified = "1"
	}

	// Apsolutni URL ZA TRAŽENI RESURS (naš URL): host + URI, sa ispravnom šemom
	// Ako nema ClientRequestHost, padamo na host/shemu iz referrera.
	absReferring := absoluteFrom(
		get("ClientRequestURI"),
		get("ClientRequestHost"),
		get("ClientRequestScheme"), // ako ova kolona ne postoji biće "", što je OK
		get("ClientRequestReferer"),
	)

	// Popunjavanje u Stage 1 redosledu
	for i, c := range schema.BaseColumns {
		switch c.Name {
		case "host_ip":
			out[i] = get("ClientIP")
		case "time_zone":
			out[i] = ""
		case "status_code":
			out[i] = status
		case "size":
			out[i] = size
		case "referrer":
			// originalni referrer; Stage 2 ga puni sa "Direct Hit" po pravilu
			out[i] = get("ClientRequestReferer")
		case "user_agent":
			out[i] = ua
		case "method":
			out[i] = get("ClientRequestMethod")
		case "referring_page":
			// apsolutni URL traženog resursa (naš URL)
			out[i] = absReferring
		case "protocol":
			out[i] = ""
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
