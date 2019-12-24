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

package main

import (
	"fmt"
	"net"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"open-match.dev/open-match/pkg/pb"

	utilTesting "open-match.dev/open-match/internal/util/testing"

	"open-match.dev/open-match/examples/scale/scenarios"
)

var (
	logger = logrus.WithFields(logrus.Fields{
		"app":       "openmatch",
		"component": "server",
	})
)

// MatchFunctionService implements pb.MatchFunctionServer, the server generated
// by compiling the protobuf, by fulfilling the pb.MatchFunctionServer interface.
type MatchFunctionService struct {
	grpc          *grpc.Server
	mmlogicClient pb.MmLogicClient
	mmfScenario   *scenarios.Scenario
}

func main() error {
	activeScenario := scenarios.ActiveScenario

	conn, err := grpc.Dial(activeScenario.MmlogicAddr, utilTesting.NewGRPCDialOptions(activeScenario.Logger)...)
	if err != nil {
		logger.Fatalf("Failed to connect to Open Match, got %v", err)
	}
	defer conn.Close()

	server := grpc.NewServer(utilTesting.NewGRPCServerOptions(activeScenario.Logger)...)
	pb.RegisterMatchFunctionServer(server, &activeScenario)
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", activeScenario.MmfServerPort))
	if err != nil {
		logger.WithFields(logrus.Fields{
			"error": err.Error(),
			"port":  activeScenario.MmfServerPort,
		}).Error("net.Listen() error")
		return err
	}

	logger.WithFields(logrus.Fields{
		"port": activeScenario.MmfServerPort,
	}).Info("TCP net listener initialized")

	logger.Info("Serving gRPC endpoint")
	err = server.Serve(ln)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"error": err.Error(),
		}).Error("gRPC serve() error")
		return err
	}

	return nil
}
