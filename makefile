# =========================
# Config (override via CLI)
# =========================
JSONL ?= logs.jsonl
RAW   ?= raw.csv
NORM  ?= normalized.csv
FINAL ?= final.csv
VERIF ?= verified.csv
MERGD ?= merged.csv

WORKERS ?= 50
BOTS    ?=
UA_PTR_VERIFY ?= true

# =========================
# Helpers
# =========================
.PHONY: check
check:
	@command -v go >/dev/null 2>&1 || { echo "Go is not installed"; exit 1; }

# =========================
# 0) JSONL -> raw.csv
# =========================
.PHONY: jsonl
jsonl: check
	@echo ">> JSONL -> CSV (auto columns + flatten): $(JSONL) -> $(RAW)"
	go run ./cmd/parser --stage jsonl --in "$(JSONL)" --out "$(RAW)" --plan=false

# =========================
# 1) normalize
# =========================
.PHONY: normalize
normalize: check
	@echo ">> normalize: $(RAW) -> $(NORM)"
	go run ./cmd/parser --stage normalize --in "$(RAW)" --out "$(NORM)" --plan=false

# =========================
# 2) enrich
# =========================
.PHONY: enrich
enrich: check
	@echo ">> enrich: $(NORM) -> $(FINAL)"
	go run ./cmd/parser --stage enrich --in "$(NORM)" --out "$(FINAL)" --plan=false

# =========================
# 3) verify (reverse PTR)
# =========================
.PHONY: verify
verify: check
	@echo ">> verify: $(NORM) -> $(VERIF)  (workers=$(WORKERS))"
	go run ./cmd/parser --stage verify --in "$(NORM)" --out "$(VERIF)" --workers $(WORKERS) --bots "$(BOTS)" --plan=false

# =========================
# 4) merge (UA<->PTR heuristic)
# =========================
.PHONY: merge
merge: check
	@echo ">> merge: $(FINAL) + $(VERIF) -> $(MERGD)  (ua-ptr-verify=$(UA_PTR_VERIFY))"
	go run ./cmd/parser --stage merge --out "$(MERGD)" --ua-ptr-verify=$(UA_PTR_VERIFY) --plan=false

# =========================
# Run everything
# =========================
.PHONY: all
all: jsonl normalize enrich verify merge
	@echo ">> DONE: $(MERGD)"

# =========================
# Clean outputs
# =========================
.PHONY: clean
clean:
	@rm -f "$(RAW)" "$(NORM)" "$(FINAL)" "$(VERIF)" "$(MERGD)"
	@echo ">> cleaned"
