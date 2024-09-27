package main

import (
	"context"
	"errors"
	"net"
	"net/http"

	"github.com/sirupsen/logrus"
	"github.com/suessflorian/client-side-analytics/diagnostics"
	"github.com/suessflorian/client-side-analytics/store/duckdb"
)

func main() {
	ctx := context.Background()
	lg := logrus.New()
	lg.SetLevel(logrus.InfoLevel)
	lg.SetFormatter(&logrus.JSONFormatter{})

	d := diagnostics.Begin(ctx, lg)
	ctx = diagnostics.ContextWithDiagnostics(ctx, d)

	connector, err := duckdb.Init(ctx, lg, "duck.db")
	if err != nil {
		lg.WithError(err).Fatal("database connection failure")
	}
	defer connector.Close()

	g, err := newGenerator(ctx, connector)
	if err != nil {
		lg.WithError(err).Fatal("failed to initialise data generator")
	}

	http.Handle("/gen", rateLimit(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusBadRequest)
		}
		if err := g.create(ctx, lg, 1); err != nil {
			lg.WithError(err).Error("failed to generate artefacts")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))

	http.Handle("/", http.FileServer(http.Dir("./static")))
	http.HandleFunc("/diagnostics", d.MetricsHandler)

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
	if err = http.ListenAndServe(":8080", nil); err != nil {
		lg.WithError(err).Info("error starting localhost server")
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
