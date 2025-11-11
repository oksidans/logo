package db

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func Open() (*sql.DB, error) {
	_ = godotenv.Load(".env")

	host := env("MYSQL_HOST", "127.0.0.1")
	port := env("MYSQL_PORT", "3306")
	user := env("MYSQL_USER", "root")
	pass := env("MYSQL_PASSWORD", "")
	name := env("MYSQL_DB", "logana")

	params := env("DB_PARAMS",
		"parseTime=true&charset=utf8mb4&collation=utf8mb4_unicode_ci&loc=Local&clientFoundRows=true")

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?%s", user, pass, host, port, name, params)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}
