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

package scenarios

import (
	"context"
	// "sync"

	"github.com/sirupsen/logrus"
	"open-match.dev/open-match/pkg/pb"
)

// TODO:
// - add images for scale-mmf and scale-evaluator, have this use it.
// - add an evaluator.
// - add ticket generation function and profiles to the scenario, can just pull from existing
//     packages for now

// after that start on getting metrics to show up from the scale-backend and scale-frontend.

var ActiveScenario = BasicScenario{}

type MatchFunction func(*pb.RunRequest, pb.MatchFunction_RunServer) error
type EvaluatorFunction func(pb.Evaluator_EvaluateServer) error


type Scenario interface {
	MatchFunction(*pb.RunRequest, pb.MatchFunction_RunServer) error
}

type BasicScenario struct {
	TicketQps int
	MMF       MatchFunction
	Evaluator EvaluatorFunction
	MmlogicAddr string
	MmfServerPort int32
	Logger *logrus.Entry
}

// TODO: FINISH THIS
func (s BasicScenario) Run(r *pb.RunRequest, stream pb.MatchFunction_RunServer) error {
	tickets := hydrate(stream.Context(), r)("everyone")

	for i := 0; i < len(tickets)+1; i += 2 {
		// Form pair.
	}

	return nil
}

// TODO: FINISH THIS
func hydrate(ctx context.Context, r *pb.RunRequest) func(string) []*pb.Ticket {
	// result := make(chan map[string][]*pb.Ticket, 1)
	// result <- make(map[string][]*pb.Ticket)
	// wg := sync.WaitGroup{}

	// for _, pool := range r.Profile.Pools {
	// 	wg.Add(1)
	// 	go func(pool *pb.Pool) {
	// 		defer wg.Done()

	// 		// tickets :=

	// 		m := <-result
	// 		m[pool.Name()] = tickets
	// 		result <- m
	// 	}(pool)
	// }

	// wg.Wait()
	// m := <-result
	// return func(name string) []*pb.Ticket {
	// 	tickets, ok := m[name]
	// 	if !ok {
	// 		panic("No pool: ", name)
	// 	}
	// 	return tickets
	// }
	return nil
}
