package main

import (
	"embed"
	"errors"
	"io/fs"
	"net/http"
	_ "net/http/pprof"
	"strings"
)

//go:embed dist
var dist embed.FS

type clientRouterFS struct {
	fs fs.FS
}

func (cr *clientRouterFS) Open(name string) (fs.File, error) {
	f, err := cr.fs.Open(name)
	if errors.Is(err, fs.ErrNotExist) {
		return cr.fs.Open("index.html")
	}
	return f, err
}

func createUIHandler() http.Handler {
	assets, err := fs.Sub(dist, "dist")
	if err != nil {
		panic(err)
	}
	return http.FileServer(http.FS(&clientRouterFS{fs: assets}))
}

type treeMux struct {
	h   http.Handler
	sub map[string]treeMux
}

func (t treeMux) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if strings.HasPrefix(req.URL.Path, "/debug/pprof") {
		http.DefaultServeMux.ServeHTTP(w, req)
		return
	}

	for prefix, c := range t.sub {
		if strings.HasPrefix(req.URL.Path, prefix) {
			req.URL.Path = strings.TrimPrefix(req.URL.Path, prefix)
			c.ServeHTTP(w, req)
			return
		}
	}
	t.h.ServeHTTP(w, req)
}
