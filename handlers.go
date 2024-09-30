package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/suessflorian/client-side-analytics/middleware"
	"github.com/suessflorian/client-side-analytics/telemetry"
)

type handler struct {
	generator *generator
	analytics *analytics
}

func (h *handler) generateHandler(w http.ResponseWriter, r *http.Request) {
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

func (h *handler) analyticsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	merchantID, err := uuid.Parse(r.PathValue("merchant_id"))
	if err != nil {
		lg(ctx).Error("invalid merchant_id uuid")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	top, err := h.analytics.GetTopProducts(ctx, merchantID)
	if err != nil {
		lg(ctx).WithError(err).Error("failed to get top products")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(top)
	if err != nil {
		log.Printf("failed to marshal response: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
}

func lg(ctx context.Context) *logrus.Logger {
	return middleware.ContextUtils(ctx).Logger
}

func reporter(ctx context.Context) *telemetry.Reporter {
	return middleware.ContextUtils(ctx).Reporter
}
