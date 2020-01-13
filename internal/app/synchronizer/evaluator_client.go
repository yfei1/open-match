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

package synchronizer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/golang/protobuf/jsonpb"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"open-match.dev/open-match/internal/config"
	"open-match.dev/open-match/internal/rpc"
	"open-match.dev/open-match/pkg/pb"
)

var (
	evaluatorClientLogger = logrus.WithFields(logrus.Fields{
		"app":       "openmatch",
		"component": "app.synchronizer.evaluator_client",
	})
)

type evaluator interface {
	evaluate(context.Context, []*pb.Match) ([]*pb.Match, error)
}

var errNoEvaluatorType = grpc.Errorf(codes.FailedPrecondition, "unable to determine evaluator type, either api.evaluator.grpcport or api.evaluator.httpport must be specified in the config")

func newEvaluator(cfg config.View) evaluator {
	newInstance := func(cfg config.View) (interface{}, error) {
		// grpc is preferred over http.
		if cfg.IsSet("api.evaluator.grpcport") {
			return newGrpcEvaluator(cfg)
		}
		if cfg.IsSet("api.evaluator.httpport") {
			return newHTTPEvaluator(cfg)
		}
		return nil, errNoEvaluatorType
	}

	return &deferredEvaluator{
		cacher: config.NewCacher(cfg, newInstance),
	}
}

type deferredEvaluator struct {
	cacher *config.Cacher
}

func (de *deferredEvaluator) evaluate(ctx context.Context, proposals []*pb.Match) ([]*pb.Match, error) {
	e, err := de.cacher.Get()
	if err != nil {
		return nil, err
	}

	matches, err := e.(evaluator).evaluate(ctx, proposals)
	if err != nil {
		de.cacher.ForceReset()
	}
	return matches, err
}

type grcpEvaluatorClient struct {
	evaluator pb.EvaluatorClient
}

func newGrpcEvaluator(cfg config.View) (evaluator, error) {
	grpcAddr := fmt.Sprintf("%s:%d", cfg.GetString("api.evaluator.hostname"), cfg.GetInt64("api.evaluator.grpcport"))
	conn, err := rpc.GRPCClientFromEndpoint(cfg, grpcAddr)
	if err != nil {
		return nil, fmt.Errorf("Failed to create grpc evaluator client: %w", err)
	}

	evaluatorClientLogger.WithFields(logrus.Fields{
		"endpoint": grpcAddr,
	}).Info("Created a GRPC client for evaluator endpoint.")

	return &grcpEvaluatorClient{
		evaluator: pb.NewEvaluatorClient(conn),
	}, nil
}

func (ec *grcpEvaluatorClient) evaluate(ctx context.Context, proposals []*pb.Match) ([]*pb.Match, error) {
	stream, err := ec.evaluator.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("Error starting evaluator call: %w", err)
	}

	for _, proposal := range proposals {
		if err = stream.Send(&pb.EvaluateRequest{Match: proposal}); err != nil {
			return nil, fmt.Errorf("Error sending proposals to evaluator: %w", err)
		}
	}

	if err = stream.CloseSend(); err != nil {
		return nil, fmt.Errorf("failed to close the send stream: %w", err)
	}

	var results = []*pb.Match{}
	for {
		// TODO: add grpc timeouts for this call.
		resp, err := stream.Recv()
		if err == io.EOF {
			// read done.
			break
		}
		if err != nil {
			return nil, fmt.Errorf("Error streaming results from evaluator: %w", err)
		}
		results = append(results, resp.GetMatch())
	}

	return results, nil
}

type httpEvaluatorClient struct {
	httpClient *http.Client
	baseURL    string
}

func newHTTPEvaluator(cfg config.View) (evaluator, error) {
	httpAddr := fmt.Sprintf("%s:%d", cfg.GetString("api.evaluator.hostname"), cfg.GetInt64("api.evaluator.httpport"))
	client, baseURL, err := rpc.HTTPClientFromEndpoint(cfg, httpAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to get a HTTP client from the endpoint %v: %w", httpAddr, err)
	}

	evaluatorClientLogger.WithFields(logrus.Fields{
		"endpoint": httpAddr,
	}).Info("Created a HTTP client for evaluator endpoint.")

	return &httpEvaluatorClient{
		httpClient: client,
		baseURL:    baseURL,
	}, nil
}

func (ec *httpEvaluatorClient) evaluate(ctx context.Context, proposals []*pb.Match) ([]*pb.Match, error) {
	reqr, reqw := io.Pipe()
	proposalIDs := getMatchIds(proposals)
	var wg sync.WaitGroup
	wg.Add(1)

	sc := make(chan error, 1)
	defer close(sc)
	go func() {
		var m jsonpb.Marshaler
		defer func() {
			wg.Done()
			if reqw.Close() != nil {
				logger.Warning("failed to close response body read closer")
			}
		}()
		for _, proposal := range proposals {
			buf, err := m.MarshalToString(&pb.EvaluateRequest{Match: proposal})
			if err != nil {
				sc <- status.Errorf(codes.FailedPrecondition, "failed to marshal proposal to string: %s", err.Error())
				return
			}
			_, err = io.WriteString(reqw, buf)
			if err != nil {
				sc <- status.Errorf(codes.FailedPrecondition, "failed to write proto string to io writer: %s", err.Error())
				return
			}
		}
	}()

	req, err := http.NewRequest("POST", ec.baseURL+"/v1/evaluator/matches:evaluate", reqr)
	if err != nil {
		return nil, status.Errorf(codes.Aborted, "failed to create evaluator http request for proposals %s: %s", proposalIDs, err.Error())
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Transfer-Encoding", "chunked")

	resp, err := ec.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, status.Errorf(codes.Aborted, "failed to get response from evaluator for proposals %s: %s", proposalIDs, err.Error())
	}
	defer func() {
		if resp.Body.Close() != nil {
			logger.Warning("failed to close response body read closer")
		}
	}()

	wg.Add(1)
	var results = []*pb.Match{}
	rc := make(chan error, 1)
	defer close(rc)
	go func() {
		defer wg.Done()

		dec := json.NewDecoder(resp.Body)
		for {
			var item struct {
				Result json.RawMessage        `json:"result"`
				Error  map[string]interface{} `json:"error"`
			}
			err := dec.Decode(&item)
			if err == io.EOF {
				break
			}
			if err != nil {
				rc <- status.Errorf(codes.Unavailable, "failed to read response from HTTP JSON stream: %s", err.Error())
				return
			}
			if len(item.Error) != 0 {
				rc <- status.Errorf(codes.Unavailable, "failed to execute evaluator.Evaluate: %v", item.Error)
				return
			}
			resp := &pb.EvaluateResponse{}
			if err = jsonpb.UnmarshalString(string(item.Result), resp); err != nil {
				rc <- status.Errorf(codes.Unavailable, "failed to execute jsonpb.UnmarshalString(%s, &proposal): %v.", item.Result, err)
				return
			}
			results = append(results, resp.GetMatch())
		}
	}()

	wg.Wait()
	if len(sc) != 0 {
		return nil, <-sc
	}
	if len(rc) != 0 {
		return nil, <-rc
	}
	return results, nil
}
