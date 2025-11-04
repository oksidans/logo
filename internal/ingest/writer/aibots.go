package writer

import (
	"context"
	"database/sql"
	"fmt"
)

type AIBotPayload struct {
	ProjectID int
	Month     int
	Year      int

	AIBotCnt  map[string]int64
	TotalRows int64
}

func InsertAIBots(ctx context.Context, db *sql.DB, p AIBotPayload) error {
	// očisti prethodno
	if _, err := db.ExecContext(ctx,
		"DELETE FROM ln_aiBotHitsByName WHERE project_id=? AND month=? AND year=?",
		p.ProjectID, fmt.Sprintf("%d", p.Month), fmt.Sprintf("%d", p.Year),
	); err != nil {
		return err
	}

	rows := make([][]any, 0, len(p.AIBotCnt))
	for name, cnt := range p.AIBotCnt {
		prop := 0.0
		if p.TotalRows > 0 {
			prop = twoDec(float64(cnt) * 100.0 / float64(p.TotalRows))
		}
		rows = append(rows, []any{
			name,
			cnt,
			prop,
			fmt.Sprintf("%d", p.Month),
			fmt.Sprintf("%d", p.Year),
			p.ProjectID,
		})
	}
	return chunkedExec(ctx, db, "ln_aiBotHitsByName",
		[]string{"botName", "value", "valueProp", "month", "year", "project_id"},
		rows, 2000,
	)
}
