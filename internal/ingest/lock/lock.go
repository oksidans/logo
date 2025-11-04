package lock

import (
	"context"
	"database/sql"
)

func Get(ctx context.Context, db *sql.DB, key string, timeoutSeconds int) (bool, error) {
	var res sql.NullInt64
	if err := db.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", key, timeoutSeconds).Scan(&res); err != nil {
		return false, err
	}
	return res.Valid && res.Int64 == 1, nil
}

func Release(ctx context.Context, db *sql.DB, key string) error {
	_, err := db.ExecContext(ctx, "SELECT RELEASE_LOCK(?)", key)
	return err
}
