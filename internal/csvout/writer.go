package csvout

import (
	"bufio"
	"encoding/csv"
	"io"
)

type Writer struct {
	w   *csv.Writer
	buf *bufio.Writer
}

func New(w io.Writer) *Writer {
	bw := bufio.NewWriterSize(w, 1<<20)
	return &Writer{
		w:   csv.NewWriter(bw),
		buf: bw,
	}
}

func (cw *Writer) WriteHeader(header []string) error {
	return cw.w.Write(header)
}

func (cw *Writer) WriteRow(row []string) error {
	return cw.w.Write(row)
}

func (cw *Writer) Flush() error {
	cw.w.Flush()
	if err := cw.w.Error(); err != nil {
		return err
	}
	return cw.buf.Flush()
}
