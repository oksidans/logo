package main

import (
	"context"
	"flag"
	"log"
	"strconv"
	"time"

	"parser/internal/ingest/aggregators"
	"parser/internal/ingest/config"
	"parser/internal/ingest/csvx"
	"parser/internal/ingest/db"
	"parser/internal/ingest/lock"
	"parser/internal/ingest/project"
	"parser/internal/ingest/schema"
	"parser/internal/ingest/util"
	"parser/internal/ingest/writer"
)

func main() {
	// CLI flags
	var (
		flagProjectID int64
		flagMonth     int
		flagYear      int
		flagCSV       string
		flagDryRun    bool
		flagCheck     bool
		flagOnlyProj  bool
	)
	flag.Int64Var(&flagProjectID, "project-id", 0, "Target project_id (default: pick an inactive placeholder automatically)")
	flag.IntVar(&flagMonth, "month", 0, "Target month (1..12). If 0, autodetect from CSV")
	flag.IntVar(&flagYear, "year", 0, "Target year. If 0, autodetect from CSV")
	flag.StringVar(&flagCSV, "csv", "", "Path to CSV (default from .env CSV_PATH)")
	flag.BoolVar(&flagDryRun, "dry-run", false, "Do not write to DB (just aggregate and log)")
	flag.BoolVar(&flagCheck, "check-schema", false, "Only check schema and exit")
	flag.BoolVar(&flagOnlyProj, "only-project", false, "Only update logana_project (no inserts into other tables)")
	flag.Parse()

	// Config + DB
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config load error: %v", err)
	}
	if flagCSV != "" {
		cfg.CSVPath = flagCSV
	}
	conn, err := db.Open(cfg)
	if err != nil {
		log.Fatalf("db open error: %v", err)
	}
	defer conn.Close()

	// Opcija: samo schema check i izlaz
	if flagCheck {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		hasRequired, hasAIBots, err := schema.Check(ctx, conn)
		if err != nil {
			log.Fatalf("[SCHEMA] error: %v", err)
		}
		if hasRequired {
			log.Printf("[SCHEMA] required OK: [logana_project ln_genBotsMainStatsByMethod ln_genRespCodes]")
		} else {
			log.Printf("[SCHEMA] required MISSING one or more tables")
		}
		if !hasAIBots {
			log.Printf("[SCHEMA] optional missing: [ln_aiBotHitsByName] (those inserts will be skipped)")
		}
		log.Printf("[SCHEMA] check completed (only-check mode). Exiting.")
		return
	}

	// project_id (auto ako nije zadat): biramo placeholder koji je neaktivan
	if flagProjectID == 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		pid, err := project.SelectTargetProject(ctx, conn)
		if err != nil {
			log.Fatalf("select placeholder project error: %v", err)
		}
		flagProjectID = pid
	}
	log.Printf("[INFO] project_id=%d", flagProjectID)

	// DB lock (kratak timeout)
	lockKey := "logana_ingest_" + strconv.FormatInt(flagProjectID, 10)
	{
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		got, err := lock.Get(ctx, conn, lockKey, 10)
		cancel()
		if err != nil {
			log.Fatalf("GET_LOCK error: %v", err)
		}
		if !got {
			log.Fatalf("another ingest run is active for project %d", flagProjectID)
		}
		defer func() { _ = lock.Release(context.Background(), conn, lockKey) }()
	}

	// 0) Schema guard (samo required)
	var (
		hasRequired bool
		hasAIBots   bool
	)
	{
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		var err error
		hasRequired, hasAIBots, err = schema.Check(ctx, conn)
		if err != nil {
			log.Fatalf("[SCHEMA] error: %v", err)
		}
		if !hasRequired {
			log.Fatalf("[SCHEMA] required tables missing: need [logana_project ln_genBotsMainStatsByMethod ln_genRespCodes]")
		}
	}

	// 1) Scan CSV meta (globalno)
	stats, err := csvx.AnalyzeCSV(cfg.CSVPath)
	if err != nil {
		log.Fatalf("csv analyze error: %v", err)
	}

	// Odredi month/year za stream
	useMonth := stats.Month
	useYear := stats.Year
	if flagMonth != 0 {
		useMonth = flagMonth
	}
	if flagYear != 0 {
		useYear = flagYear
	}

	// 2) Streaming agregacija sa filtriranjem po (useMonth,useYear)
	agg := aggregators.NewAggregateBucket()
	dbg := &csvx.DebugInfo{Enabled: true}
	if err := csvx.StreamAndAggregate(cfg.CSVPath, useMonth, useYear, agg, dbg); err != nil {
		log.Fatalf("aggregate stream error: %v", err)
	}

	// Ako je korisnik eksplicitno zadao -month/-year, prikaži window (UTC)
	if flagMonth != 0 && flagYear != 0 {
		winStart, winEnd := util.MonthWindowUTC(useYear, useMonth)
		winEnd = winEnd.Add(-time.Second) // vizuelno inkluzivan kraj
		log.Printf("[INFO] meta_mode=filtered min=%s max=%s rows=%d",
			winStart.Format("2006-01-02 15:04:05"),
			winEnd.Format("2006-01-02 15:04:05"),
			agg.FilteredRows,
		)
	}
	// Operativni debug
	log.Printf("[DEBUG] read=%d skip_month=%d parse_err=%d no_method=%d no_status=%d header=%v",
		dbg.TotalRead, dbg.SkipWrongMonth, dbg.SkipParseErr, dbg.SkipNoMethod, dbg.SkipNoStatus, dbg.LastHeader)

	// Uvek prikaži i globalni CSV raspon za referencu
	log.Printf("[INFO] month=%d year=%d rows=%d start=%s end=%s",
		useMonth, useYear, stats.Rows,
		stats.Min.Format("2006-01-02 15:04:05"),
		stats.Max.Format("2006-01-02 15:04:05"),
	)

	// 3) Meta za update projekta: ako je zadat -month/-year, koristimo FILTERED (UTC prozor)
	var metaMin, metaMax time.Time
	var metaRows int64
	if flagMonth != 0 && flagYear != 0 && agg.FilteredRows > 0 && !agg.MinTS.IsZero() && !agg.MaxTS.IsZero() {
		metaMin, metaMax, metaRows = agg.MinTS, agg.MaxTS, agg.FilteredRows
		log.Printf("[INFO] meta_mode=filtered min=%s max=%s rows=%d",
			metaMin.Format("2006-01-02 15:04:05"),
			metaMax.Format("2006-01-02 15:04:05"),
			metaRows,
		)
	} else {
		metaMin, metaMax, metaRows = stats.Min, stats.Max, stats.Rows
		log.Printf("[INFO] meta_mode=auto (using full CSV stats)")
	}

	// broj kolona iz headera
	noCols := len(dbg.LastHeader)

	// 4) Ako je -dry-run, ne diramo bazu – samo izveštaj i izlaz
	if flagDryRun {
		log.Printf("[DRY] filtered_rows=%d methods=%d respCodes=%d sitemap=%d aiBots=%d",
			agg.FilteredRows, len(agg.MethodCounts), len(agg.StatusCounts), agg.SitemapCount, len(agg.AIBotCounts),
		)
		log.Printf("[DONE] Dry-run finished.")
		return
	}

	// 5) U ovom koraku možda želimo SAMO projekat (bez drugih tabela)
	if flagOnlyProj {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		err = project.UpdateProjectMeta(ctx, conn, flagProjectID, metaMin, metaMax, metaRows, noCols)
		cancel()
		if err != nil {
			log.Fatalf("update project meta error: %v", err)
		}
		log.Printf("[OK] logana_project updated (id=%d, no_cols=%d)", flagProjectID, noCols)
		return
	}

	// === NORMALIZACIJA TIPOVA ZA writer.GeneralPayload ===
	statusCnt := make(map[int]int64, len(agg.StatusCounts))
	for k, v := range agg.StatusCounts {
		if k == "" {
			continue
		}
		if code, err := strconv.Atoi(k); err == nil {
			statusCnt[code] = v
		}
	}

	// 6a) general INSERTs
	{
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		err = writer.InsertGeneral(ctx, conn, writer.GeneralPayload{
			ProjectID: int(flagProjectID),
			Month:     useMonth,
			Year:      useYear,
			MethodCnt: agg.MethodCounts, // map[string]int64
			StatusCnt: statusCnt,        // map[int]int64
			TotalRows: agg.FilteredRows,
		})
		if err != nil {
			log.Fatalf("insert general error: %v", err)
		}
		log.Printf("[OK] general inserts done")
	}

	// 6b) sitemap INSERTs
	{
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		err = writer.InsertSitemap(ctx, conn, writer.SitemapPayload{
			ProjectID:   int(flagProjectID),
			Month:       useMonth,
			Year:        useYear,
			SitemapHits: agg.SitemapCount,
		})
		if err != nil {
			log.Fatalf("insert sitemap error: %v", err)
		}
		log.Printf("[OK] sitemap inserts done")
	}

	// 6c) AI bot INSERTs (ako tabela postoji)
	{
		if !hasAIBots {
			log.Printf("[SKIP] ln_aiBotHitsByName not present — skipping aibot inserts")
		} else {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			err = writer.InsertAIBots(ctx, conn, writer.AIBotPayload{
				ProjectID: int(flagProjectID),
				Month:     useMonth,
				Year:      useYear,
				AIBotCnt:  agg.AIBotCounts,
				TotalRows: agg.FilteredRows,
			})
			if err != nil {
				log.Fatalf("insert aibots error: %v", err)
			}
			log.Printf("[OK] aibot inserts done")
		}
	}

	// Ne diramo is_active ovde – ostaje 0, po dogovoru.
	log.Printf("[DONE] Ingest finished at %s", time.Now().Format(time.RFC3339))
}
