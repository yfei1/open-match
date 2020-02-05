// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package swaggerui is a simple webserver for hosting Open Match Swagger UI.
package swaggerui

import (
	"os"

	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/sirupsen/logrus"
	"open-match.dev/open-match/internal/config"
	"open-match.dev/open-match/internal/logging"
	"open-match.dev/open-match/internal/rpc"
	"open-match.dev/open-match/internal/telemetry"
)

var (
	logger = logrus.WithFields(logrus.Fields{
		"app":       "openmatch",
		"component": "swaggerui",
	})
)

// RunApplication is the main for swagger ui http reverse proxy.
func RunApplication() {
	cfg, err := config.Read()
	if err != nil {
		logger.WithFields(logrus.Fields{
			"error": err.Error(),
		}).Fatalf("cannot read configuration.")
	}
	logging.ConfigureLogging(cfg)

	serve(cfg)
}

func serve(cfg config.View) {
	mux := &http.ServeMux{}
	closer := telemetry.Setup("swaggerui", mux, cfg)
	defer closer()
	port := cfg.GetInt("api.swaggerui.httpport")
	dataPath, ok := os.LookupEnv("KO_DATA_PATH")
	if !ok {
		logger.Fatalf("KO_DATA_PATH not presented")
	}

	_, err := os.Stat(dataPath)
	if err != nil {
		logger.WithError(err).Fatalf("cannot access directory %s", dataPath)
	}

	mux.Handle("/", http.FileServer(http.Dir(dataPath)))
	mux.Handle(telemetry.HealthCheckEndpoint, telemetry.NewAlwaysReadyHealthCheck())
	bindHandler(mux, cfg, "/v1/frontend/", "frontend")
	bindHandler(mux, cfg, "/v1/backend/", "backend")
	bindHandler(mux, cfg, "/v1/queryservice/", "queryservice")
	bindHandler(mux, cfg, "/v1/synchronizer/", "synchronizer")
	bindHandler(mux, cfg, "/v1/evaluator/", "evaluator")
	bindHandler(mux, cfg, "/v1/matchfunction/", "functions")
	addr := fmt.Sprintf(":%d", port)
	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	logger.Infof("SwaggerUI for Open Match")
	logger.Infof("Serving static directory: %s", dataPath)
	logger.Infof("Serving on %s", addr)
	logger.Fatal(srv.ListenAndServe())
}

func bindHandler(mux *http.ServeMux, cfg config.View, path string, service string) {
	client, endpoint, err := rpc.HTTPClientFromConfig(cfg, "api."+service)
	if err != nil {
		panic(err)
	}
	logger.Infof("Registering reverse proxy %s -> %s", path, endpoint)
	mux.Handle(path, overlayURLProxy(mustURLParse(endpoint), client))
}

// Reference implementation: https://golang.org/src/net/http/httputil/reverseproxy.go?s=3330:3391#L98
func overlayURLProxy(target *url.URL, client *http.Client) *httputil.ReverseProxy {
	targetQuery := target.RawQuery
	director := func(req *http.Request) {
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		if targetQuery == "" || req.URL.RawQuery == "" {
			req.URL.RawQuery = targetQuery + req.URL.RawQuery
		} else {
			req.URL.RawQuery = targetQuery + "&" + req.URL.RawQuery
		}
		if _, ok := req.Header["User-Agent"]; !ok {
			// explicitly disable User-Agent so it's not set to default value
			req.Header.Set("User-Agent", "")
		}
		logger.Debugf("URL: %s", req.URL)
	}
	return &httputil.ReverseProxy{
		Director:  director,
		Transport: client.Transport,
	}
}

func mustURLParse(u string) *url.URL {
	url, err := url.Parse(u)
	if err != nil {
		logger.Fatalf("failed to parse URL %s, %v", u, err)
	}
	return url
}
