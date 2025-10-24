package iox

import (
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
)

func OpenAuto(path string) (io.ReadCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if filepath.Ext(path) == ".gz" {
		gr, err := gzip.NewReader(f)
		if err != nil {
			f.Close()
			return nil, err
		}
		return &rc{Reader: gr, Closers: []io.Closer{gr, f}}, nil
	}
	return f, nil
}

func CreateAuto(path string) (io.WriteCloser, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	if filepath.Ext(path) == ".gz" {
		gw := gzip.NewWriter(f)
		return &wc{Writer: gw, Closers: []io.Closer{gw, f}}, nil
	}
	return f, nil
}

type rc struct {
	io.Reader
	Closers []io.Closer
}

func (r *rc) Close() error {
	var err error
	for i := range r.Closers {
		if e := r.Closers[i].Close(); err == nil && e != nil {
			err = e
		}
	}
	return err
}

type wc struct {
	io.Writer
	Closers []io.Closer
}

func (w *wc) Close() error {
	var err error
	for i := range w.Closers {
		if e := w.Closers[i].Close(); err == nil && e != nil {
			err = e
		}
	}
	return err
}
