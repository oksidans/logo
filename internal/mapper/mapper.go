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
func absoluteFrom(uri, reqHost, reqScheme, referrer string) string {
	u := strings.TrimSpace(uri)
	if u == "" || u == "-" {
		return ""
	}

	// već apsolutan?
	if parsed, err := url.Parse(u); err == nil && parsed.IsAbs() {
		return parsed.String()
	}

	// šema iz referrera i host iz referrera
	refScheme := ""
	refHost := ""
	if rp, err := url.Parse(strings.TrimSpace(referrer)); err == nil && rp != nil {
		refScheme = strings.ToLower(rp.Scheme)
		refHost = rp.Host
	}

	// normalizuj reqScheme
	rs := strings.ToLower(strings.TrimSpace(reqScheme))
	if rs != "http" && rs != "https" {
		rs = ""
	}

	// protocol-relative
	if strings.HasPrefix(u, "//") {
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

	// osnovni host: ClientRequestHost ili host iz referrera
	baseHost := strings.TrimSpace(reqHost)
	if baseHost == "" {
		baseHost = refHost
	}
	if baseHost == "" {
		return u // vrati relativan
	}

	// šema: reqScheme -> referrer -> https
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

	// datetime
	dt := get("EdgeEndTimestamp")
	day, month, year := "", "", ""
	if dt != "" {
		if t, err := time.Parse(rfc3339, dt); err == nil {
			day = strconv.Itoa(t.Day())
			month = strconv.Itoa(int(t.Month()))
			year = strconv.Itoa(t.Year())
		}
	}

	// target (Stage 2 prepisuje u resource type)
	target := get("ClientRequestPath")
	if target == "" {
		target = get("ClientRequestURI")
	}

	status := get("EdgeResponseStatus")
	size := get("EdgeResponseBytes")
	ua := get("ClientRequestUserAgent")

	verified := ""
	if v := get("VerifiedBotCategory"); v != "" {
		verified = "1"
	}

	absReferring := absoluteFrom(
		get("ClientRequestURI"),
		get("ClientRequestHost"),
		get("ClientRequestScheme"),
		get("ClientRequestReferer"),
	)

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
			out[i] = get("ClientRequestReferer")
		case "user_agent":
			out[i] = ua
		case "method":
			out[i] = get("ClientRequestMethod")
		case "referring_page":
			out[i] = absReferring
		case "protocol":
			out[i] = get("ClientRequestScheme")
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
			out[i] = "" // popunjava se kasnije kroz verifikaciju ako želiš merge
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
