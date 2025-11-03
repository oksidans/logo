SHELL := /bin/bash

GO      := go
BIN     := bin/parser

# NVMe putanje
TMPDIR      := /mnt/nvme/tmp
GOCACHE     := /mnt/nvme/gocache
GOMODCACHE  := /mnt/nvme/gomod
GOPATH      := /mnt/nvme/gopath

ENV := TMPDIR=$(TMPDIR) GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) GOPATH=$(GOPATH)

# Tuning
JSONL_WORKERS   ?= 16
VERIFY_WORKERS  ?= 50
DEFAULT_SCHEME  ?= https
BOTS_FILE       ?= bots.json

# I/O fajlovi
JSONL_IN   ?= logs.jsonl
RAW_CSV    ?= raw.csv
NORM_CSV   ?= normalized.csv
FINAL_CSV  ?= final.csv
VERI_CSV   ?= verified.csv
MERGE_CSV  ?= merged.csv
AIBOT_CSV  ?= merged_ai.csv

# Intermedijeri i final
INTERMEDIATE_CSVS := $(RAW_CSV) $(NORM_CSV) $(FINAL_CSV) $(VERI_CSV) $(MERGE_CSV)
FINAL_ARTIFACT    := $(AIBOT_CSV)

.NOTPARALLEL:
.PHONY: all build help clean clean-all deepclean check-env cleanup-temp
# Ako recept padne, nemoj brisati target (zadrži za debug)
.PRECIOUS: $(RAW_CSV) $(NORM_CSV) $(FINAL_CSV) $(VERI_CSV) $(MERGE_CSV) $(AIBOT_CSV)

all: $(FINAL_ARTIFACT)
	@echo "✅ All stages complete — final: $(FINAL_ARTIFACT)"
	@$(MAKE) cleanup-temp

help:
	@echo "Targets:"
	@echo "  make all         # ceo pipeline; zadržava samo $(FINAL_ARTIFACT)"
	@echo "  make clean       # briše SAMO međurezultate (ostavlja final)"
	@echo "  make clean-all   # briše SVE CSV fajlove (i final)"
	@echo "  make deepclean   # clean-all + Go keševi + bin/"
	@echo "  make build       # build binarnog parsera"

# Build jednom
$(BIN): check-env
	@mkdir -p bin
	$(ENV) $(GO) build -trimpath -ldflags="-s -w" -o $(BIN) ./cmd/parser

# Faze (reda radi)
$(RAW_CSV): $(JSONL_IN) | $(BIN)
	$(ENV) $(BIN) --stage jsonl --in $(JSONL_IN) --out $(RAW_CSV) --jsonl-workers $(JSONL_WORKERS) --plan=false

$(NORM_CSV): $(RAW_CSV) | $(BIN)
	$(ENV) $(BIN) --stage normalize --in $(RAW_CSV) --out $(NORM_CSV) --default-scheme $(DEFAULT_SCHEME) --plan=false

$(FINAL_CSV): $(NORM_CSV) | $(BIN)
	$(ENV) $(BIN) --stage enrich --in $(NORM_CSV) --out $(FINAL_CSV) --plan=false

$(VERI_CSV): $(NORM_CSV) | $(BIN)
	$(ENV) $(BIN) --stage verify --in $(NORM_CSV) --out $(VERI_CSV) --workers $(VERIFY_WORKERS) --bots $(BOTS_FILE) --plan=false

$(MERGE_CSV): $(FINAL_CSV) $(VERI_CSV) | $(BIN)
	$(ENV) $(BIN) --stage merge --in $(FINAL_CSV) --out $(MERGE_CSV) --plan=false

$(AIBOT_CSV): $(MERGE_CSV) | $(BIN)
	$(ENV) $(BIN) --stage aibots --in $(MERGE_CSV) --out $(AIBOT_CSV) --plan=false

# Čišćenje

# OVO briše SAMO međurezultate – final ostaje
clean:
	rm -f $(INTERMEDIATE_CSVS)

# OVO briše SVE, uključujući final
clean-all:
	rm -f $(INTERMEDIATE_CSVS) $(FINAL_ARTIFACT)

deepclean: clean-all
	$(ENV) $(GO) clean -cache -modcache -testcache
	rm -f $(BIN)
	@echo "🧹 Removing temp remnants under $(TMPDIR)…"
	-find $(TMPDIR) -maxdepth 1 -user "$(USER)" -mindepth 1 -print -exec rm -rf {} +

# Poziva se automatski posle 'all' — briše samo intermedijere
cleanup-temp:
	@echo "🧹 Removing intermediate CSVs..."
	@rm -f $(INTERMEDIATE_CSVS)
	@echo "💾 Kept: $(FINAL_ARTIFACT)"

check-env:
	@sudo mkdir -p $(TMPDIR) $(GOCACHE) $(GOMODCACHE) $(GOPATH)
	@sudo chown -R $(USER):$(USER) $(TMPDIR) $(GOCACHE) $(GOMODCACHE) $(GOPATH)
	@sudo chmod 1777 $(TMPDIR)
	@chmod -R u+rwX $(GOCACHE) $(GOMODCACHE) $(GOPATH)
