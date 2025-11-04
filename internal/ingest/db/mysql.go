package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"parser/internal/ingest/config"

	_ "github.com/go-sql-driver/mysql"
)

func Open(cfg *config.Config) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&charset=utf8mb4&collation=utf8mb4_unicode_ci&timeout=%ds&readTimeout=%ds&writeTimeout=%ds",
		cfg.MySQLUser, cfg.MySQLPassword, cfg.MySQLHost, cfg.MySQLPort, cfg.MySQLDB,
		int(cfg.ConnectTimeout.Seconds()),
		int(cfg.QueryTimeout.Seconds()),
		int(cfg.QueryTimeout.Seconds()),
	)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	// Pool tuning
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(2 * time.Hour)

	// Health + session setup sa timeout-om
	ctx, cancel := context.WithTimeout(context.Background(), cfg.QueryTimeout)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, err
	}

	// Forsiramo UTC za trenutnu sesiju.
	// Napomena: ovo važi za konekciju na kojoj se izvršava.
	// Ako kasnije pool otvori nove konekcije, i one bi trebalo da dobiju UTC (vidi napomenu ispod).
	if _, err := db.ExecContext(ctx, "SET time_zone = '+00:00'"); err != nil {
		return nil, fmt.Errorf("set session time_zone UTC failed: %w", err)
	}

	return db, nil
}
