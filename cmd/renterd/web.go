package main

import (
	"net/http"
	_ "net/http/pprof"
	"strings"
)

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
	if t.h != nil {
		t.h.ServeHTTP(w, req)
		return
	}
	http.NotFound(w, req)
}
