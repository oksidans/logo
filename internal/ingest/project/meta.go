package project

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// SelectTargetProject bira "placeholder" projekat koji je trenutno NEaktivan
// i čeka popunjavanje meta-podataka. Biramo najskoriji koji nema upisane datume
// (ili je is_active=0) – po ID-u opadajuće.
func SelectTargetProject(ctx context.Context, conn *sql.DB) (int64, error) {
	// Prioritet: is_active=0 i (logs_start_date IS NULL OR logs_end_date IS NULL)
	const q = `
		SELECT id
		FROM logana_project
		WHERE is_active = 0
		  AND (logs_start_date IS NULL OR logs_end_date IS NULL)
		ORDER BY id DESC
		LIMIT 1
	`
	var id sql.NullInt64
	if err := conn.QueryRowContext(ctx, q).Scan(&id); err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("no suitable inactive placeholder project found")
		}
		return 0, fmt.Errorf("select target project failed: %w", err)
	}
	if !id.Valid {
		return 0, fmt.Errorf("no suitable inactive placeholder project found (null id)")
	}
	return id.Int64, nil
}

// (Zadržali smo i "staru" funkciju ako je negde koristiš.)
// Ako želiš potpuno uklanjanje, možeš je obrisati.
func GetActiveProjectIDForUpdate(ctx context.Context, conn *sql.DB) (int64, error) {
	return SelectTargetProject(ctx, conn)
}

// UpdateProjectMeta ažurira meta podatke (start/end/no_rows/no_cols) za dati project_id.
// Ne dira is_active – po dogovoru ostaje 0 dok se ceo sprint uspešno ne završi.
func UpdateProjectMeta(
	ctx context.Context,
	conn *sql.DB,
	projectID int64,
	start time.Time,
	end time.Time,
	noRows int64,
	noCols int,
) error {
	const q = `
		UPDATE logana_project
		SET
			logs_start_date = ?,
			logs_end_date   = ?,
			no_rows         = ?,
			no_cols         = ?
		WHERE id = ?
	`
	res, err := conn.ExecContext(ctx, q, start, end, noRows, noCols, projectID)
	if err != nil {
		return fmt.Errorf("update project meta failed: %w", err)
	}
	aff, _ := res.RowsAffected()
	if aff == 0 {
		return fmt.Errorf("update project meta: no rows affected (id=%d)", projectID)
	}
	return nil
}

// MarkInactive je ostavljen za kasnije faze (ne koristimo ga u ovom koraku).
func MarkInactive(ctx context.Context, conn *sql.DB, projectID int64) error {
	const q = `UPDATE logana_project SET is_active = 0 WHERE id = ?`
	if _, err := conn.ExecContext(ctx, q, projectID); err != nil {
		return fmt.Errorf("mark inactive failed: %w", err)
	}
	return nil
}
