package middleware

import (
	"net/http"
	"time"
)

func Delay(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
    time.Sleep(200 * time.Millisecond)
		next(w, r)
	}
}
