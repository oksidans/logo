package writer

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
)

func twoDec(f float64) float64 {
	return math.Round(f*100) / 100
}

func chunkedExec(ctx context.Context, db *sql.DB, table string, cols []string, rows [][]any, chunk int) error {
	if len(rows) == 0 {
		return nil
	}
	if chunk <= 0 {
		chunk = 2000
	}
	for i := 0; i < len(rows); i += chunk {
		j := i + chunk
		if j > len(rows) {
			j = len(rows)
		}
		part := rows[i:j]
		if err := bulkInsert(ctx, db, table, cols, part); err != nil {
			return err
		}
	}
	return nil
}

func bulkInsert(ctx context.Context, db *sql.DB, table string, cols []string, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}
	pl := "(" + strings.TrimRight(strings.Repeat("?,", len(cols)), ",") + ")"
	valPlace := strings.TrimRight(strings.Repeat(pl+",", len(rows)), ",")
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", table, strings.Join(cols, ","), valPlace)

	args := make([]any, 0, len(rows)*len(cols))
	for _, r := range rows {
		args = append(args, r...)
	}
	_, err := db.ExecContext(ctx, query, args...)
	return err
}
