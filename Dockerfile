# ==================================
# FAZA 1: IZGRADNJA (BUILDER STAGE)
# ==================================
FROM golang:1.23-alpine AS builder

ENV CGO_ENABLED=0

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

# Kopiramo sav izvorni kod projekta
COPY . .

# --------------- IZGRADNJA APLIKACIJA ---------------
# LIN 27: IZGRADNJA PRVOG BINARNOG FAJLA (parser)
RUN go build -o /app/parser-app -v -trimpath -ldflags "-s -w" ./cmd/parser

# LIN 28: IZGRADNJA DRUGOG BINARNOG FAJLA (verifier)
RUN go build -o /app/verifier-app -v -trimpath -ldflags "-s -w" ./cmd/verifier


# ==================================
# FAZA 2: ZAVRŠNA (FINAL STAGE)
# Kreiranje minimalističkog image-a od nule
# ==================================
FROM scratch

# Kopiramo OBA binarna fajla iz builder faze
COPY --from=builder /app/parser-app /usr/local/bin/parser-app
COPY --from=builder /app/verifier-app /usr/local/bin/verifier-app

# Definišemo podrazumevani ulaz u kontejner.
# Koristimo verifier-app kao podrazumevanu komandu.
# Korisnik će morati da je prepiše pri pokretanju!
ENTRYPOINT ["/usr/local/bin/verifier-app"]

# Primer komande koju korisnik može videti na Docker Hubu
CMD ["help"]
