package schema

type Kind int

const (
	String Kind = iota
	Int
	Bool
	TimeISO
)

type Column struct {
	Name string
	Kind Kind
}

// BaseColumns: Stage 1 output schema (normalize)
var BaseColumns = []Column{
	{Name: "host_ip", Kind: String},
	{Name: "time_zone", Kind: String},
	{Name: "status_code", Kind: Int},
	{Name: "size", Kind: Int},
	{Name: "referrer", Kind: String},
	{Name: "user_agent", Kind: String},
	{Name: "method", Kind: String},
	{Name: "referring_page", Kind: String},
	{Name: "protocol", Kind: String},
	{Name: "day", Kind: Int},
	{Name: "month", Kind: Int},
	{Name: "year", Kind: Int},
	{Name: "source", Kind: String},
	{Name: "target", Kind: String},
	{Name: "botName", Kind: String},
	{Name: "verified", Kind: String},
	{Name: "datetime", Kind: TimeISO},
}

func BaseHeader() []string {
	out := make([]string, len(BaseColumns))
	for i, c := range BaseColumns {
		out[i] = c.Name
	}
	return out
}
