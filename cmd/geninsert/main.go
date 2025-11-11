package main

import (
	"context"
	"database/sql"
	"flag"
	"log"
	"time"

	"parser/internal/db"
	"parser/internal/gen"
)

func main() {
	var (
		csv   = flag.String("csv", "merged_ai.csv", "Path to merged_ai.csv")
		month = flag.Int("month", 0, "Month (1..12)")
		year  = flag.Int("year", 0, "Year (e.g. 2025)")
		pid   = flag.Int64("project-id", 0, "Project ID")

		all   = flag.Bool("all", true, "Run all gen inserts")
		mainF = flag.Bool("main", false, "Only ln_genBotsMainStats")
		srcF  = flag.Bool("by-source", false, "ln_genBotsMainStatsBySource")
		mtdF  = flag.Bool("by-method", false, "ln_genBotsMainStatsByMethod")
		verF  = flag.Bool("by-verification", false, "ln_genBotsMainStatsByVerification")
		refF  = flag.Bool("by-refpage", false, "ln_genBotsMainStatsByRefPage")
		tgtF  = flag.Bool("by-target", false, "ln_genBotsMainStatsByTarget")
	)
	flag.Parse()

	if *pid == 0 || *month == 0 || *year == 0 {
		log.Fatal("project-id, month, year are required")
	}

	dbh, err := db.Open()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if cerr := dbh.Close(); cerr != nil {
			log.Printf("[WARN] db close failed: %v", cerr)
		}
	}()

	p := gen.Params{CSV: *csv, ProjectID: *pid, Month: *month, Year: *year}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Hour)
	defer cancel()

	run := func(name string, fn func(context.Context, *sql.DB, gen.Params) error) {
		log.Printf("[RUN] %s", name)
		if err := fn(ctx, dbh, p); err != nil {
			log.Fatalf("[FAIL] %s: %v", name, err)
		}
		log.Printf("[OK ] %s", name)
	}

	if *all || *mainF {
		run("ln_genBotsMainStats", gen.InsertMain)
	}
	if *all || *srcF {
		run("ln_genBotsMainStatsBySource", gen.InsertBySource)
	}
	if *all || *mtdF {
		run("ln_genBotsMainStatsByMethod", gen.InsertByMethod)
	}
	if *all || *verF {
		run("ln_genBotsMainStatsByVerification", gen.InsertByVerification)
	}
	if *all || *refF {
		run("ln_genBotsMainStatsByRefPage", gen.InsertByRefPage)
	}
	if *all || *tgtF {
		run("ln_genBotsMainStatsByTarget", gen.InsertByTarget)
	}

	log.Printf("✅ geninsert complete")
}
