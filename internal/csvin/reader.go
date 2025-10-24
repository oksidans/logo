package csvin

import (
	"bufio"
	"encoding/csv"
	"io"
	"strings"
)

type Reader struct {
	cr     *csv.Reader
	header []string
	index  map[string]int
	inited bool
}

type Options struct {
	Comma      rune
	Comment    rune
	LazyQuotes bool
	TrimSpace  bool
}

func New(r io.Reader, opt Options) *Reader {
	br := bufio.NewReaderSize(r, 1<<20)
	cr := csv.NewReader(br)
	if opt.Comma != 0 {
		cr.Comma = opt.Comma
	}
	if opt.Comment != 0 {
		cr.Comment = opt.Comment
	}
	cr.LazyQuotes = opt.LazyQuotes
	cr.TrimLeadingSpace = opt.TrimSpace
	return &Reader{cr: cr}
}

func (r *Reader) init() error {
	if r.inited {
		return nil
	}
	h, err := r.cr.Read()
	if err != nil {
		return err
	}
	r.header = h
	r.index = make(map[string]int, len(h))
	for i, name := range h {
		r.index[strings.TrimSpace(name)] = i
	}
	r.inited = true
	return nil
}

func (r *Reader) Header() ([]string, map[string]int, error) {
	if err := r.init(); err != nil {
		return nil, nil, err
	}
	return r.header, r.index, nil
}

func (r *Reader) Next() (map[string]string, error) {
	if err := r.init(); err != nil {
		return nil, err
	}
	rec, err := r.cr.Read()
	if err != nil {
		return nil, err
	}
	row := make(map[string]string, len(r.header))
	for name, idx := range r.index {
		if idx < len(rec) {
			row[name] = rec[idx]
		} else {
			row[name] = ""
		}
	}
	return row, nil
}

func MustGet(row map[string]string, key string) string {
	if v, ok := row[key]; ok {
		return v
	}
	return ""
}
