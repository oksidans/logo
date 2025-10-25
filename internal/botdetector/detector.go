package botdetector

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

type Rule struct {
	Name  string `json:"name" yaml:"name"`
	Regex string `json:"regex" yaml:"regex"`
}

type compiled struct {
	name string
	re   *regexp.Regexp
}

type Detector struct {
	rules []compiled
}

var global *Detector

// InitFromFile initializes global detector from a JSON or YAML file.
// If path == "", or loading fails, it falls back to defaults.
func InitFromFile(path string) error {
	if path == "" {
		global = defaults()
		return nil
	}
	d, err := loadFromFile(path)
	if err != nil {
		global = defaults()
		return fmt.Errorf("botdetector: %w (fallback to defaults)", err)
	}
	global = d
	return nil
}

func loadFromFile(path string) (*Detector, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	ext := strings.ToLower(filepath.Ext(path))
	var rules []Rule
	switch ext {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(b, &rules); err != nil {
			return nil, err
		}
	case ".json":
		if err := json.Unmarshal(b, &rules); err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unsupported bots file format (use .json or .yaml/.yml)")
	}
	if len(rules) == 0 {
		return nil, errors.New("no rules found in bots file")
	}
	return compile(rules)
}

func compile(rules []Rule) (*Detector, error) {
	cs := make([]compiled, 0, len(rules))
	for _, r := range rules {
		rx := r.Regex
		if rx == "" {
			continue
		}
		if !strings.HasPrefix(rx, "(?i)") {
			rx = "(?i)" + rx
		}
		re, err := regexp.Compile(rx)
		if err != nil {
			return nil, fmt.Errorf("compile %q: %w", r.Name, err)
		}
		cs = append(cs, compiled{name: r.Name, re: re})
	}
	if len(cs) == 0 {
		return nil, errors.New("no valid regex rules compiled")
	}
	return &Detector{rules: cs}, nil
}

func defaults() *Detector {
	d, _ := compile([]Rule{
		{Name: "Googlebot", Regex: "googlebot"},
		{Name: "Bingbot", Regex: "bingbot"},
		{Name: "DuckDuckBot", Regex: "duckduckbot"},
		{Name: "AhrefsBot", Regex: "ahrefsbot"},
		{Name: "SemrushBot", Regex: "semrush(bot)?"},
		{Name: "YandexBot", Regex: "yandex(bot)?"},
		{Name: "LinkedInBot", Regex: "linkedin(bot)?|LinkedInBot"},
		{Name: "FacebookBot", Regex: "facebookexternalhit|facebot"},
		{Name: "TwitterBot", Regex: "twitter(bot)?|TweetmemeBot"},
	})
	return d
}

// Match returns (name, true) if string matches a rule, else ("", false).
func Match(s string) (string, bool) {
	if global == nil {
		global = defaults()
	}
	for _, c := range global.rules {
		if c.re.MatchString(s) {
			return c.name, true
		}
	}
	return "", false
}
