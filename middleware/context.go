package middleware

import (
	"context"
	"net/http"

	"github.com/sirupsen/logrus"
	"github.com/suessflorian/client-side-analytics/telemetry"
)

type contextKey int

const (
	contextUtils contextKey = iota
)

type utilities struct {
	Logger   *logrus.Logger
	Reporter *telemetry.Reporter
}

func ContextUtils(ctx context.Context) *utilities {
	return ctx.Value(contextUtils).(*utilities)
}

func WithContextUtils(next http.HandlerFunc, lg *logrus.Logger, reporter *telemetry.Reporter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(context.WithValue(r.Context(), contextUtils, &utilities{
			Logger:   lg,
			Reporter: reporter,
		}))
		next(w, r)
	}
}
