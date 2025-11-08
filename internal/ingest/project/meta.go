package project

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Vraća aktivni project_id (is_active=1) uz FOR UPDATE (ako ti je potreban).
func GetActiveProjectIDForUpdate(ctx context.Context, db *sql.DB) (int64, error) {
	const q = `SELECT id FROM logana_project WHERE is_active=1 ORDER BY id DESC LIMIT 1`
	var id sql.NullInt64
	if err := db.QueryRowContext(ctx, q).Scan(&id); err != nil {
		return 0, fmt.Errorf("GetActiveProjectIDForUpdate: %w", err)
	}
	if !id.Valid {
		return 0, errors.New("GetActiveProjectIDForUpdate: no active project")
	}
	return id.Int64, nil
}

// Auto-odabir ciljnog projekta:
// - ako je explicitID != 0 -> vrati njega
// - inače uzmi NAJSKOROJI neaktivan red bez meta (NULL datumi)
func GetTargetProjectID(ctx context.Context, db *sql.DB, explicitID int64) (int64, error) {
	if explicitID != 0 {
		return explicitID, nil
	}
	const q = `
		SELECT id
		FROM logana_project
		WHERE is_active=0
		  AND logs_start_date IS NULL
		  AND logs_end_date   IS NULL
		ORDER BY id DESC
		LIMIT 1`
	var id sql.NullInt64
	if err := db.QueryRowContext(ctx, q).Scan(&id); err != nil {
		return 0, fmt.Errorf("GetTargetProjectID: %w", err)
	}
	if !id.Valid {
		return 0, errors.New("GetTargetProjectID: no inactive empty project found")
	}
	return id.Int64, nil
}

// Stari "ne-safe" update (ostavljen zbog kompatibilnosti).
func UpdateProjectMeta(ctx context.Context, db *sql.DB, projectID int64, min, max time.Time, rows int64) error {
	const q = `
		UPDATE logana_project
		SET logs_start_date=?, logs_end_date=?, no_rows=?, no_cols=17, is_active=1
		WHERE id=?`
	_, err := db.ExecContext(ctx, q, min, max, rows, projectID)
	if err != nil {
		return fmt.Errorf("UpdateProjectMeta: %w", err)
	}
	return nil
}

// Novi "safe" update: neće prepisati projekat koji već ima meta osim ako allowOverwrite=true.
func UpdateProjectMetaSafe(ctx context.Context, db *sql.DB, projectID int64, min, max time.Time, rows int64, allowOverwrite bool) error {
	var (
		q   string
		arg []any
	)
	if allowOverwrite {
		q = `
			UPDATE logana_project
			SET logs_start_date=?, logs_end_date=?, no_rows=?, no_cols=17, is_active=1
			WHERE id=?`
		arg = []any{min, max, rows, projectID}
	} else {
		q = `
			UPDATE logana_project
			SET logs_start_date=?, logs_end_date=?, no_rows=?, no_cols=17, is_active=1
			WHERE id=?
			  AND logs_start_date IS NULL
			  AND logs_end_date   IS NULL`
		arg = []any{min, max, rows, projectID}
	}

	res, err := db.ExecContext(ctx, q, arg...)
	if err != nil {
		return fmt.Errorf("UpdateProjectMetaSafe exec: %w", err)
	}
	aff, _ := res.RowsAffected()
	if aff != 1 {
		if allowOverwrite {
			return fmt.Errorf("UpdateProjectMetaSafe: rows_affected=%d (expected 1)", aff)
		}
		return errors.New("UpdateProjectMetaSafe: target row not empty (use -allow-overwrite to force)")
	}
	return nil
}

func MarkInactive(ctx context.Context, db *sql.DB, projectID int64) error {
	const q = `UPDATE logana_project SET is_active=0 WHERE id=?`
	_, err := db.ExecContext(ctx, q, projectID)
	if err != nil {
		return fmt.Errorf("MarkInactive: %w", err)
	}
	return nil
}
