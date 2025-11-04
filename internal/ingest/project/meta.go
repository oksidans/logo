package project

import (
	"context"
	"database/sql"
	"time"
)

// GetActiveProjectIDForUpdate vraća ID poslednjeg aktivnog projekta (is_active=1).
// Ako želiš striktno zaključavanje, koristi FOR UPDATE unutar eksplicitne transakcije.
// Ovde radimo jednostavan SELECT jer već imamo globalni GET_LOCK na nivou procesa.
func GetActiveProjectIDForUpdate(ctx context.Context, db *sql.DB) (int64, error) {
	const q = `
		SELECT id
		FROM logana_project
		WHERE is_active = 1
		ORDER BY id DESC
		LIMIT 1
	`
	var id int64
	if err := db.QueryRowContext(ctx, q).Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

// UpdateProjectMeta upisuje meta podatke za projekat:
// logs_start_date, logs_end_date, no_rows, i takođe postavlja creation_date.
// Napomena: creation_date podešavamo samo ako je NULL (da ne prepisujemo postojeću vrednost).
func UpdateProjectMeta(ctx context.Context, db *sql.DB, projectID int64, minTS, maxTS time.Time, rows int64) error {
	const q = `
		UPDATE logana_project
		SET
			logs_start_date = ?,
			logs_end_date   = ?,
			no_rows         = ?,
			creation_date   = IF(creation_date IS NULL, NOW(), creation_date)
		WHERE id = ?
	`
	_, err := db.ExecContext(ctx, q, minTS, maxTS, rows, projectID)
	return err
}

// MarkInactive postavlja is_active=0 za dati project_id.
// Ako u šemi postoji updated_at, možeš dodati i ", updated_at = NOW()" u SET deo.
func MarkInactive(ctx context.Context, db *sql.DB, projectID int64) error {
	const q = `
		UPDATE logana_project
		SET is_active = 0
		WHERE id = ?
	`
	_, err := db.ExecContext(ctx, q, projectID)
	return err
}
