package main

import (
	"net"
	"net/http"
	"sync"
)

var locks sync.Map

func rateLimit(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
      w.WriteHeader(http.StatusInternalServerError)
      return
		}
		loaded, _ := locks.LoadOrStore(ip, new(sync.Mutex))
		lock := loaded.(*sync.Mutex)

		if !lock.TryLock() {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		defer lock.Unlock()

		next(w, r)
	}
}
