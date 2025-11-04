package writer

import (
	"context"
	"database/sql"
	"fmt"
)

type SitemapPayload struct {
	ProjectID   int
	Month       int
	Year        int
	SitemapHits int64
}

func InsertSitemap(ctx context.Context, db *sql.DB, p SitemapPayload) error {
	// očisti prethodno za (project,month,year)
	if _, err := db.ExecContext(ctx,
		"DELETE FROM ln_sitemapHits WHERE project_id=? AND month=? AND year=?",
		p.ProjectID, fmt.Sprintf("%d", p.Month), fmt.Sprintf("%d", p.Year),
	); err != nil {
		return err
	}
	prop := 0.0 // ako imaš total sitemap universe, može se računati procentualno
	rows := [][]any{{
		p.SitemapHits,
		prop,
		fmt.Sprintf("%d", p.Month),
		fmt.Sprintf("%d", p.Year),
		p.ProjectID,
	}}
	return chunkedExec(ctx, db, "ln_sitemapHits",
		[]string{"value", "valueProp", "month", "year", "project_id"},
		rows, 100,
	)
}
