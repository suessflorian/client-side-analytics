package main

import (
	"context"
	"encoding/json"
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

	lg(ctx).Info("generated merchant data")
}

func (h *handler) analyticsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	merchantID, err := uuid.Parse(r.PathValue("merchant_id"))
	if err != nil {
		lg(ctx).Error("invalid merchant_id uuid")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	lg := lg(ctx).WithField("merchant", merchantID)

	top, err := h.analytics.GetTopProducts(ctx, merchantID)
	if err != nil {
		lg.WithError(err).Error("failed to get top products")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(top)
	if err != nil {
		lg.WithError(err).Error("failed to marshal top products")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	lg.Info("served merchant analytics")
}

func (h *handler) loaderHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	merchantID, err := uuid.Parse(r.PathValue("merchant_id"))
	if err != nil {
		lg(ctx).Error("invalid merchant_id uuid")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	lg := lg(ctx).WithField("merchant", merchantID)

	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", "attachment;filename=data.zip")

	lg.Info("streaming merchant data")
	err = h.analytics.csvDump(ctx, w, merchantID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		lg.WithError(err).Error("failed to dump data")
		return
	}

	lg.Info("streaming finished streaming")
}

func lg(ctx context.Context) *logrus.Logger {
	return middleware.ContextUtils(ctx).Logger
}

func reporter(ctx context.Context) *telemetry.Reporter {
	return middleware.ContextUtils(ctx).Reporter
}
