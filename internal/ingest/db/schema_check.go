package db

import (
	"context"
	"database/sql"
	"errors"
)

// CurrentSchema vraća trenutno izabranu MySQL šemu (DATABASE()) iz aktivne konekcije.
func CurrentSchema(ctx context.Context, conn *sql.DB) (string, error) {
	var s sql.NullString
	if err := conn.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&s); err != nil {
		return "", err
	}
	if !s.Valid || s.String == "" {
		return "", errors.New("no database selected")
	}
	return s.String, nil
}

// CheckRequiredTables proverava postojanje liste tabela u zadatoj šemi.
// Vraća slice onih koje NEDOSTAJU.
func CheckRequiredTables(ctx context.Context, conn *sql.DB, schema string, tables []string) ([]string, error) {
	missing := make([]string, 0, len(tables))
	for _, t := range tables {
		ok, err := tableExists(ctx, conn, schema, t)
		if err != nil {
			return nil, err
		}
		if !ok {
			missing = append(missing, t)
		}
	}
	return missing, nil
}

// tableExists proverava da li tabela postoji u information_schema.tables.
func tableExists(ctx context.Context, conn *sql.DB, schema, table string) (bool, error) {
	const q = `
		SELECT COUNT(*)
		FROM information_schema.tables
		WHERE table_schema = ? AND table_name = ?
		LIMIT 1
	`
	var c int
	if err := conn.QueryRowContext(ctx, q, schema, table).Scan(&c); err != nil {
		return false, err
	}
	return c > 0, nil
}
