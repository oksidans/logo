package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"parser/internal/csvin"
	"parser/internal/csvout"
	"parser/internal/enrich"
	"parser/internal/iox"
	"parser/internal/mapper"
	"parser/internal/schema"
)

var (
	inPath   = flag.String("in", "", "Input CSV path (raw for normalize; normalized for enrich). Supports .gz")
	outPath  = flag.String("out", "", "Output CSV path. Supports .gz")
	stage    = flag.String("stage", "normalize", "Stage to run: normalize | enrich")
	showPlan = flag.Bool("plan", true, "Print plan and exit")
)

func main() {
	flag.Parse()
	if *inPath == "" || *outPath == "" {
		log.Fatal("usage: --in <in.csv[.gz]> --out <out.csv[.gz]> --stage normalize|enrich [--plan=false]")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		<-ch
		fmt.Fprintln(os.Stderr, "\n⏹️  Signal received, shutting down...")
		cancel()
	}()

	if *showPlan {
		fmt.Println("Plan:")
		fmt.Printf("  Stage : %s\n", *stage)
		fmt.Printf("  Input : %s\n", *inPath)
		fmt.Printf("  Output: %s\n", *outPath)
		switch *stage {
		case "normalize":
			fmt.Printf("  Header (out): %v\n", schema.BaseHeader())
		case "enrich":
			fmt.Printf("  Header (out): %v\n", schema.BaseHeader())
			fmt.Println("  Note  : 'target' will be overwritten with resource type; empty 'referrer' will be set to 'Direct Hit' when 'referring_page' has a URL.")
		}
		return
	}

	switch *stage {
	case "normalize":
		if err := runNormalize(ctx, *inPath, *outPath); err != nil {
			log.Fatalf("normalize error: %v", err)
		}
	case "enrich":
		if err := runEnrich(ctx, *inPath, *outPath); err != nil {
			log.Fatalf("enrich error: %v", err)
		}
	default:
		log.Fatalf("unknown stage: %s", *stage)
	}
}

func runNormalize(ctx context.Context, inPath, outPath string) error {
	in, err := iox.OpenAuto(inPath)
	if err != nil {
		return fmt.Errorf("open input: %w", err)
	}
	defer in.Close()

	out, err := iox.CreateAuto(outPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer out.Close()

	reader := csvin.New(in, csvin.Options{Comma: ',', TrimSpace: true})
	if _, _, err := reader.Header(); err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	writer := csvout.New(out)
	if err := writer.WriteHeader(schema.BaseHeader()); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	var rowsIn, rowsOut, bad int64
	start := time.Now()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			log.Printf("progress: in=%d out=%d bad=%d", rowsIn, rowsOut, bad)
		default:
			row, err := reader.Next()
			if err != nil {
				if err == io.EOF {
					goto DONE
				}
				bad++
				continue
			}
			rowsIn++
			mapped := mapper.MapToCSV(row)
			if err := writer.WriteRow(mapped); err != nil {
				return fmt.Errorf("write row: %w", err)
			}
			rowsOut++
		}
	}

DONE:
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	log.Printf("normalize done. in=%d out=%d bad=%d time=%s", rowsIn, rowsOut, bad, time.Since(start))
	return nil
}

func runEnrich(ctx context.Context, inPath, outPath string) error {
	in, err := iox.OpenAuto(inPath)
	if err != nil {
		return fmt.Errorf("open input: %w", err)
	}
	defer in.Close()

	out, err := iox.CreateAuto(outPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer out.Close()

	reader := csvin.New(in, csvin.Options{Comma: ',', TrimSpace: true})
	header, _, err := reader.Header()
	if err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	has := func(name string) bool {
		for _, h := range header {
			if h == name {
				return true
			}
		}
		return false
	}
	if !has("target") && !has("referring_page") {
		log.Printf("warning: neither 'target' nor 'referring_page' columns were found; target will remain empty")
	}

	writer := csvout.New(out)
	if err := writer.WriteHeader(schema.BaseHeader()); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	var rowsIn, rowsOut, bad int64
	start := time.Now()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			log.Printf("progress: in=%d out=%d bad=%d", rowsIn, rowsOut, bad)
		default:
			row, err := reader.Next()
			if err != nil {
				if err == io.EOF {
					goto DONE
				}
				bad++
				continue
			}
			rowsIn++

			// 1) Overwrite target with resource type (from target URL, fallback to referring_page)
			url := row["target"]
			if url == "" {
				url = row["referring_page"]
			}
			class := enrich.ResourceTypeFromURL(url)
			row["target"] = class

			// 2) Fill 'referrer' with "Direct Hit" if empty while referring_page has a URL
			ref := strings.TrimSpace(row["referrer"])
			refpg := strings.TrimSpace(row["referring_page"])
			if (ref == "" || ref == "-") && refpg != "" && refpg != "-" {
				row["referrer"] = "Direct Hit"
			}

			// Preserve column order
			outRow := make([]string, len(schema.BaseColumns))
			for i, c := range schema.BaseColumns {
				outRow[i] = row[c.Name]
			}
			if err := writer.WriteRow(outRow); err != nil {
				return fmt.Errorf("write row: %w", err)
			}
			rowsOut++
		}
	}

DONE:
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	log.Printf("enrich done. in=%d out=%d bad=%d time=%s", rowsIn, rowsOut, bad, time.Since(start))
	return nil
}
