package writer

import (
	"context"
	"database/sql"
)

type MethodRow struct {
	Method    string
	Value     int64
	ValueProp float64 // DECIMAL(6,2)
	Month     string  // varchar(45)
	Year      string  // varchar(45)
	ProjectID int64
}

// Minimalni “smoke” insert u ln_genBotsMainStatsByMethod
func InsertMethodStatsSmoke(ctx context.Context, db *sql.DB, rows []MethodRow) error {
	if len(rows) == 0 {
		return nil
	}
	q := `INSERT INTO ln_genBotsMainStatsByMethod
		(method, value, valueProp, month, year, project_id)
		VALUES `
	args := make([]any, 0, len(rows)*6)
	valHolders := make([]string, 0, len(rows))
	for _, r := range rows {
		valHolders = append(valHolders, "(?, ?, ?, ?, ?, ?)")
		args = append(args, r.Method, r.Value, r.ValueProp, r.Month, r.Year, r.ProjectID)
	}
	q += joinComma(valHolders)

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, q, args...); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func joinComma(ss []string) string {
	if len(ss) == 0 {
		return ""
	}
	out := ss[0]
	for i := 1; i < len(ss); i++ {
		out += "," + ss[i]
	}
	return out
}
