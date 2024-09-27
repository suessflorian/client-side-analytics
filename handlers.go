package main

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/sirupsen/logrus"
	"github.com/suessflorian/client-side-analytics/middleware"
	"github.com/suessflorian/client-side-analytics/telemetry"
)

type handler struct {
	generator *generator
}

func (h *handler) GenerateHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	generated, err := h.generator.create(ctx, lg(ctx), reporter(ctx), 1)
	if err != nil {
		lg(ctx).WithError(err).Error("failed to generate artefacts")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(generated)
}

func lg(ctx context.Context) *logrus.Logger {
	return middleware.ContextUtils(ctx).Logger
}

func reporter(ctx context.Context) *telemetry.Reporter {
	return middleware.ContextUtils(ctx).Reporter
}
