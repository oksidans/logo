
# ==== Konfiguracija ====
JSONL ?= logs.jsonl
RAW   ?= raw.csv
NORM  ?= normalized.csv
FINAL ?= final.csv
VERIF ?= verified.csv
MERGD ?= merged.csv
WORKERS ?= 50
BOTS ?=
UA_PTR_VERIFY ?= true

# ==== Provera alata ====
.PHONY: check
check:
	@command -v jq >/dev/null 2>&1 || { echo "jq nije instaliran (sudo apt-get install jq)"; exit 1; }
	@command -v go >/dev/null 2>&1 || { echo "go nije instaliran"; exit 1; }

# ==== 0) JSONL -> raw.csv ====
.PHONY: jsonl
jsonl: check
	@echo ">> JSONL -> CSV: $(JSONL) -> $(RAW)"
	@printf 'ClientIP,ClientRequestHost,ClientRequestMethod,ClientRequestURI,EdgeEndTimestamp,EdgeResponseBytes,EdgeResponseStatus,ClientRequestPath,ClientRequestUserAgent,VerifiedBotCategory,ClientDeviceType,ClientRequestReferer,ClientRequestScheme\n' > $(RAW)
	@jq -r '[.ClientIP // "",
	         .ClientRequestHost // "",
	         .ClientRequestMethod // "",
	         .ClientRequestURI // "",
	         .EdgeEndTimestamp // "",
	         (.EdgeResponseBytes // 0 | tostring),
	         (.EdgeResponseStatus // 0 | tostring),
	         .ClientRequestPath // "",
	         .ClientRequestUserAgent // "",
	         .VerifiedBotCategory // "",
	         .ClientDeviceType // "",
	         .ClientRequestReferer // "",
	         .ClientRequestScheme // ""
	        ] | @csv' $(JSONL) >> $(RAW)

# ==== 1) normalize ====
.PHONY: normalize
normalize: check
	@echo ">> normalize: $(RAW) -> $(NORM)"
	go run ./cmd/parser --stage normalize --in $(RAW) --out $(NORM) --plan=false

# ==== 2) enrich ====
.PHONY: enrich
enrich: check
	@echo ">> enrich: $(NORM) -> $(FINAL)"
	go run ./cmd/parser --stage enrich --in $(NORM) --out $(FINAL) --plan=false

# ==== 3) verify (reverse PTR) ====
.PHONY: verify
verify: check
	@echo ">> verify: $(NORM) -> $(VERIF)  (workers=$(WORKERS))"
	go run ./cmd/parser --stage verify --in $(NORM) --out $(VERIF) --workers $(WORKERS) --bots "$(BOTS)" --plan=false

# ==== 4) merge (UA<->PTR heuristika po domenima) ====
.PHONY: merge
merge: check
	@echo ">> merge: $(FINAL) + $(VERIF) -> $(MERGD)  (ua-ptr-verify=$(UA_PTR_VERIFY))"
	go run ./cmd/parser --stage merge --out $(MERGD) --ua-ptr-verify=$(UA_PTR_VERIFY) --plan=false

# ==== Sve faze redom ====
.PHONY: all
all: jsonl normalize enrich verify merge
	@echo ">> DONE: $(MERGD)"

# ==== Čišćenje izlaza ====
.PHONY: clean
clean:
	@rm -f $(RAW) $(NORM) $(FINAL) $(VERIF) $(MERGD)
	@echo ">> cleaned"
