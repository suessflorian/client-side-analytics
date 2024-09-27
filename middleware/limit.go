package middleware

import (
	"net"
	"net/http"
	"sync"
)

// WithLimitOneAtATime ensures that every client can have one active request fullfilled
// at any given time. We 429 the client on attempted concurrent request.
func WithLimitOneAtATime(next http.HandlerFunc) http.HandlerFunc {
	var locks sync.Map

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
