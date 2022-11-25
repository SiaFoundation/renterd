package node

import (
	"net/http"
	"strings"
)

type TreeMux struct {
	H   http.Handler
	Sub map[string]http.Handler
}

func (t TreeMux) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	for prefix, c := range t.Sub {
		if strings.HasPrefix(req.URL.Path, prefix) {
			req.URL.Path = strings.TrimPrefix(req.URL.Path, prefix)
			c.ServeHTTP(w, req)
			return
		}
	}
	t.H.ServeHTTP(w, req)
}
