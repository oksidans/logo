package aggregators

import "time"

type AggregateBucket struct {
	FilteredRows int64

	MethodCounts map[string]int64 // "GET", "POST", ...
	StatusCounts map[string]int64 // "200", "404", ...
	SitemapCount int64

	AIBotCounts map[string]int64 // "GPTBot", "PerplexityBot", ...

	// Meta isključivo za FILTRIRANE redove (target month/year)
	MinTS time.Time
	MaxTS time.Time
}

func NewAggregateBucket() *AggregateBucket {
	return &AggregateBucket{
		MethodCounts: make(map[string]int64),
		StatusCounts: make(map[string]int64),
		AIBotCounts:  make(map[string]int64),
	}
}
