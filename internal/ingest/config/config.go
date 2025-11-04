package config

import (
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	MySQLHost      string
	MySQLPort      int
	MySQLUser      string
	MySQLPassword  string
	MySQLDB        string
	CSVPath        string
	ConnectTimeout time.Duration
	QueryTimeout   time.Duration
}

func Load() (*Config, error) {
	_ = godotenv.Load() // optional

	return &Config{
		MySQLHost:      getenv("MYSQL_HOST", "127.0.0.1"),
		MySQLPort:      getenvInt("MYSQL_PORT", 3306),
		MySQLUser:      getenv("MYSQL_USER", "root"),
		MySQLPassword:  getenv("MYSQL_PASSWORD", ""),
		MySQLDB:        getenv("MYSQL_DB", "logana"),
		CSVPath:        getenv("CSV_PATH", "./merged_ai.csv"),
		ConnectTimeout: time.Duration(getenvInt("DB_CONNECT_TIMEOUT", 5)) * time.Second,
		QueryTimeout:   time.Duration(getenvInt("DB_QUERY_TIMEOUT", 30)) * time.Second,
	}, nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
