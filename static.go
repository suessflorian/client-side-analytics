package main

import (
	_ "embed"
	"fmt"
	"net/http"
)

var (
	//go:embed markup.html
	markup string
	//go:embed script.js
	script string
)

func homeHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	fmt.Fprint(w, markup)
}

func scriptHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/javascript")
	fmt.Fprint(w, script)
}
