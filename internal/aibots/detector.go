package aibots

import (
	"regexp"
	"strings"
)

// Proširiva lista AI bot indikatora (slobodno dopuni)
var aiBotLabels = []string{
	"OAI-SearchBot",
	"GPTBot",
	"ChatGPT-User",
	"PerplexityBot",
	"Perplexity-User", // koristimo ASCII varijantu radi jednostavnosti
	"AI2Bot",
	"Ai2Bot-Dolma",
	"Amazonbot",
	"anthropic-ai",
	"antropic-ai", // česta tipografska greška
	"Claude-Web",
	"ClaudeBot",
	"Claude-SearchBot",
	"Claude-User",
	"cohere-ai",
	"Google-Extended",
	"Google-CloudVertexBot",
}

var aiRegex *regexp.Regexp

func init() {
	alts := make([]string, 0, len(aiBotLabels))
	for _, lbl := range aiBotLabels {
		alts = append(alts, regexp.QuoteMeta(lbl))
	}
	pat := "(?i)(" + strings.Join(alts, "|") + ")"
	aiRegex = regexp.MustCompile(pat)
}

// Detect vraća sve prepoznate AI bot labele u UA (može biti nil/empty).
func Detect(ua string) []string {
	ua = strings.TrimSpace(ua)
	if ua == "" {
		return nil
	}
	m := aiRegex.FindAllString(ua, -1)
	if len(m) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(m))
	out := make([]string, 0, len(m))
	for _, s := range m {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
