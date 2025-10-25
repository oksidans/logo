package enrich

import (
	"net/url"
	"path"
	"strings"
)

func ResourceTypeFromURL(u string) string {
	u = strings.TrimSpace(u)
	if u == "" || u == "-" {
		return ""
	}
	lc := strings.ToLower(u)

	parsed, err := url.Parse(lc)
	var p string
	if err == nil && parsed != nil {
		p = parsed.Path
	} else {
		p = lc
	}
	p = strings.TrimSpace(p)
	if p == "" {
		return ""
	}

	ext := strings.TrimPrefix(strings.ToLower(path.Ext(p)), ".")
	switch ext {
	case "jpg", "jpeg", "png", "gif", "webp", "avif", "svg", "ico", "bmp", "tif", "tiff":
		return "Image"
	case "css":
		return "Stylesheet"
	case "js", "mjs":
		return "Script"
	case "json", "jsonl":
		return "JSON"
	case "html", "htm":
		return "HTML"
	case "php", "asp", "aspx", "jsp", "cfm":
		return "ServerScript"
	case "xml":
		if strings.Contains(p, "sitemap") {
			return "Sitemap"
		}
		if strings.Contains(p, "rss") || strings.Contains(p, "atom") {
			return "Feed"
		}
		return "XML"
	case "pdf", "txt", "csv", "tsv", "doc", "docx", "xls", "xlsx", "ppt", "pptx":
		return "Document"
	case "woff", "woff2", "ttf", "otf", "eot":
		return "Font"
	case "mp4", "webm", "ogv", "mov", "mpeg", "mpg":
		return "Video"
	case "mp3", "ogg", "wav", "m4a", "aac":
		return "Audio"
	case "zip", "tar", "gz", "tgz", "7z", "rar":
		return "Archive"
	case "map":
		return "SourceMap"
	case "webmanifest", "manifest":
		return "Manifest"
	}

	if strings.Contains(p, "/wp-json") || strings.Contains(p, "/graphql") || strings.Contains(p, "/api/") {
		return "API"
	}
	if strings.HasSuffix(p, "/") || !strings.Contains(p, ".") {
		return "Page"
	}
	return "Page"
}
