package api

import (
	"net/http"
	_ "net/http/pprof"
	"strings"
)

type TreeMux struct {
	Handler http.Handler
	Sub     map[string]TreeMux
	Pprof   bool
}

func (t TreeMux) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if t.Pprof && strings.HasPrefix(req.URL.Path, "/debug/pprof") {
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
