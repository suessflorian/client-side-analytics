package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/suessflorian/client-side-analytics/middleware"
	"github.com/suessflorian/client-side-analytics/store/duckdb"
	"github.com/suessflorian/client-side-analytics/telemetry"
)

func main() {
	ctx := context.Background()
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, os.Kill)
	defer cancel()

	lg := logrus.New()
	lg.SetLevel(logrus.InfoLevel)
	lg.SetFormatter(&logrus.JSONFormatter{})

	engine, reporter := telemetry.New(ctx, lg)

	connector, err := duckdb.Init(ctx, lg, "duck.db")
	if err != nil {
		lg.WithError(err).Fatal("database connection failure")
	}
	defer connector.Close()

	generator, err := newMerchantGenerator(ctx, lg, reporter, connector)
	if err != nil {
		lg.WithError(err).Fatal("failed to initialise merchant generator")
	}

	mux := http.NewServeMux()
	h := &handler{generator: generator, analytics: &analytics{connector}}

	var register = func(pattern string, handler http.HandlerFunc) {
		mux.HandleFunc(pattern, middleware.WithContextUtils(handler, lg, reporter))
	}

	register("POST /generate", middleware.WithLimitOneAtATime(h.generateHandler))
	register("GET /analytics/{merchant_id}", h.analyticsHandler)
	register("GET /loader/{merchant_id}", middleware.WithLimitOneAtATime(h.loaderHandler))
	register("GET /telemetry", engine.ServeHTTP)
	register("/", http.FileServer(http.Dir("./static")).ServeHTTP)

	server := http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		address, err := getLANIPAddress()
		if err != nil {
			if errors.Is(err, ErrNoLANIPAddressFound) {
				lg.Info("no local ip address found", address)
			} else {
				lg.WithError(err).Error("failed to get LAN IP address")
			}
		} else {
			lg.Infof("⚡️ listening on http://%s:8080 ⚡️", address)
		}

		lg.Info("⚡️ listening on http://localhost:8080 ⚡️")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			lg.WithError(err).Info("error starting localhost server")
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := server.Close(); err != nil {
		lg.WithError(err).Error("failed to gracefully shutdown http server")
	}
	if err := engine.Close(shutdownCtx); err != nil {
		lg.WithError(err).Error("failed to gracefully shutdown telemetry engine")
	}
}

var ErrNoLANIPAddressFound = errors.New("no local area network ip address found")

func getLANIPAddress() (net.IP, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return net.IP{}, err
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP, nil
			}
		}
	}
	return net.IP{}, ErrNoLANIPAddressFound
}
