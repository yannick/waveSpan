// Command wavespan-gateway is the optional stateless gateway. At M0 it is a compile-only
// stub that serves /healthz; routing, auth, and Cypher planning arrive in later milestones.
package main

import (
	"flag"
	"log"
	"net/http"
)

func main() {
	addr := flag.String("listen", ":7910", "gateway admin listen address")
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	log.Printf("wavespan-gateway (stub) listening on %s", *addr)
	srv := &http.Server{Addr: *addr, Handler: mux}
	log.Fatal(srv.ListenAndServe())
}
