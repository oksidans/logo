# Makefile for parser pipeline
# Run full pipeline: make all
# Clean temporary artifacts: make clean

# Input/output files
JSONL_INPUT = logs.jsonl
RAW_CSV     = raw.csv
NORMALIZED  = normalized.csv
ENRICHED    = final.csv
VERIFIED    = verified.csv
MERGED      = merged.csv
MERGED_AI   = merged_ai.csv

# Default scheme and workers
DEFAULT_SCHEME = https
VERIFY_WORKERS = 50
JSONL_WORKERS  = 16

# Optional bot rules (leave empty if unused)
BOTS_FILE = bots.json

# ---- STAGES ----

jsonl:
	go run ./cmd/parser \
		--stage jsonl \
		--in $(JSONL_INPUT) \
		--out $(RAW_CSV) \
		--jsonl-workers $(JSONL_WORKERS) \
		--plan=false

normalize:
	go run ./cmd/parser \
		--stage normalize \
		--in $(RAW_CSV) \
		--out $(NORMALIZED) \
		--default-scheme $(DEFAULT_SCHEME) \
		--plan=false

enrich:
	go run ./cmd/parser \
		--stage enrich \
		--in $(NORMALIZED) \
		--out $(ENRICHED) \
		--plan=false

verify:
	go run ./cmd/parser \
		--stage verify \
		--in $(NORMALIZED) \
		--out $(VERIFIED) \
		--workers $(VERIFY_WORKERS) \
		--bots $(BOTS_FILE) \
		--plan=false

merge:
	go run ./cmd/parser \
		--stage merge \
		--in $(ENRICHED) \
		--out $(MERGED) \
		--plan=false

aibots:
	go run ./cmd/parser \
		--stage aibots \
		--in $(MERGED) \
		--out $(MERGED_AI) \
		--plan=false

# Full pipeline
all: jsonl normalize enrich verify merge aibots
	@echo "✅ All stages complete — final output: $(MERGED_AI)"

# Cleanup
clean:
	rm -f $(RAW_CSV) $(NORMALIZED) $(ENRICHED) $(VERIFIED) $(MERGED) $(MERGED_AI)
	@echo "🧹 Clean complete"
