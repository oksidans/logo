package writer

import (
	"context"
	"database/sql"
	"fmt"
)

type GeneralPayload struct {
	ProjectID int
	Month     int
	Year      int

	MethodCnt map[string]int64
	StatusCnt map[int]int64

	TotalRows int64
}

// INSERT u ln_genBotsMainStatsByMethod: (method, value, valueProp, month, year, project_id)
func insertMethods(ctx context.Context, db *sql.DB, p GeneralPayload) error {
	rows := make([][]any, 0, len(p.MethodCnt))
	for method, cnt := range p.MethodCnt {
		prop := 0.0
		if p.TotalRows > 0 {
			prop = twoDec(float64(cnt) * 100.0 / float64(p.TotalRows))
		}
		rows = append(rows, []any{
			method,
			cnt,
			prop,
			fmt.Sprintf("%d", p.Month),
			fmt.Sprintf("%d", p.Year),
			p.ProjectID,
		})
	}
	return chunkedExec(ctx, db, "ln_genBotsMainStatsByMethod",
		[]string{"method", "value", "valueProp", "month", "year", "project_id"},
		rows, 2000,
	)
}

// INSERT u ln_genRespCodes: (status_code, value, valueProp, month, year, project_id)
func insertRespCodes(ctx context.Context, db *sql.DB, p GeneralPayload) error {
	rows := make([][]any, 0, len(p.StatusCnt))
	for code, cnt := range p.StatusCnt {
		prop := 0.0
		if p.TotalRows > 0 {
			prop = twoDec(float64(cnt) * 100.0 / float64(p.TotalRows))
		}
		rows = append(rows, []any{
			code,
			cnt,
			prop,
			fmt.Sprintf("%d", p.Month),
			fmt.Sprintf("%d", p.Year),
			p.ProjectID,
		})
	}
	return chunkedExec(ctx, db, "ln_genRespCodes",
		[]string{"status_code", "value", "valueProp", "month", "year", "project_id"},
		rows, 2000,
	)
}

func InsertGeneral(ctx context.Context, db *sql.DB, p GeneralPayload) error {
	// (opciono) očisti prethodne vrednosti za isti (project,month,year)
	if _, err := db.ExecContext(ctx,
		"DELETE FROM ln_genBotsMainStatsByMethod WHERE project_id=? AND month=? AND year=?",
		p.ProjectID, fmt.Sprintf("%d", p.Month), fmt.Sprintf("%d", p.Year),
	); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx,
		"DELETE FROM ln_genRespCodes WHERE project_id=? AND month=? AND year=?",
		p.ProjectID, fmt.Sprintf("%d", p.Month), fmt.Sprintf("%d", p.Year),
	); err != nil {
		return err
	}

	if err := insertMethods(ctx, db, p); err != nil {
		return err
	}
	if err := insertRespCodes(ctx, db, p); err != nil {
		return err
	}
	return nil
}
