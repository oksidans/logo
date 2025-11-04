package schema

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// Check proverava da li postoje obavezne tabele i (opciono) AI-bot tabela
// u trenutno selektovanoj bazi (SELECT DATABASE()).
// Vraća: (hasRequired, hasAIBots, err).
func Check(ctx context.Context, conn *sql.DB) (bool, bool, error) {
	// 1) Aktivni schema
	var dbName sql.NullString
	if err := conn.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&dbName); err != nil {
		return false, false, fmt.Errorf("SELECT DATABASE() failed: %w", err)
	}
	if !dbName.Valid || strings.TrimSpace(dbName.String) == "" {
		return false, false, fmt.Errorf("n+o active database selected")
	}
	schema := dbName.String

	// 2) Sve tabele u tom schemi
	const q = `
		SELECT TABLE_NAME
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = ?
	`
	rows, err := conn.QueryContext(ctx, q, schema)
	if err != nil {
		return false, false, fmt.Errorf("schema list query failed: %w", err)
	}
	defer rows.Close()

	found := map[string]bool{}
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return false, false, fmt.Errorf("scan table failed: %w", err)
		}
		found[t] = true
	}
	if err := rows.Err(); err != nil {
		return false, false, fmt.Errorf("rows err: %w", err)
	}

	// 3) Obavezne i opciona
	required := []string{
		"logana_project",
		"ln_genBotsMainStatsByMethod",
		"ln_genRespCodes",
	}
	hasRequired := true
	for _, t := range required {
		if !found[t] {
			hasRequired = false
			break
		}
	}

	hasAIBots := found["ln_aiBotHitsByName"]
	return hasRequired, hasAIBots, nil
}
