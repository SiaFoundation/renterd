package api

import (
	"net/http"
	"strings"
)

type TreeMux struct {
	Handler http.Handler
	Sub     map[string]TreeMux
}

func (t TreeMux) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if strings.HasPrefix(req.URL.Path, "/debug/pprof") {
		http.DefaultServeMux.ServeHTTP(w, req)
		return
	}

	for prefix, c := range t.Sub {
		if strings.HasPrefix(req.URL.Path, prefix) {
			req.URL.Path = strings.TrimPrefix(req.URL.Path, prefix)
			c.ServeHTTP(w, req)
			return
		}
	}
	if t.Handler != nil {
		t.Handler.ServeHTTP(w, req)
		return
	}
	http.NotFound(w, req)
}
