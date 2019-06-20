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

package minimatch

import (
	"context"
	"io"
	"testing"

	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	internalTesting "open-match.dev/open-match/internal/testing"
	"open-match.dev/open-match/pkg/pb"
)

func TestAssignTickets(t *testing.T) {
	assert := assert.New(t)
	tc := createMinimatchForTest(t)
	defer tc.Close()

	fe := pb.NewFrontendClient(tc.MustGRPC())
	be := pb.NewBackendClient(tc.MustGRPC())

	ctResp, err := fe.CreateTicket(tc.Context(), &pb.CreateTicketRequest{Ticket: &pb.Ticket{}})
	assert.Nil(err)

	var tt = []struct {
		// description string
		req  *pb.AssignTicketsRequest
		resp *pb.AssignTicketsResponse
		code codes.Code
	}{
		{
			&pb.AssignTicketsRequest{},
			nil,
			codes.InvalidArgument,
		},
		{
			&pb.AssignTicketsRequest{
				TicketId: []string{"1"},
			},
			nil,
			codes.InvalidArgument,
		},
		{
			&pb.AssignTicketsRequest{
				TicketId: []string{"2"},
				Assignment: &pb.Assignment{
					Connection: "localhost",
				},
			},
			nil,
			codes.NotFound,
		},
		{
			&pb.AssignTicketsRequest{
				TicketId: []string{ctResp.Ticket.Id},
				Assignment: &pb.Assignment{
					Connection: "localhost",
				},
			},
			&pb.AssignTicketsResponse{},
			codes.OK,
		},
	}

	for _, test := range tt {
		resp, err := be.AssignTickets(tc.Context(), test.req)
		assert.Equal(test.resp, resp)
		if err != nil {
			assert.Equal(test.code, status.Convert(err).Code())
		} else {
			gtResp, err := fe.GetTicket(tc.Context(), &pb.GetTicketRequest{TicketId: ctResp.Ticket.Id})
			assert.Nil(err)
			assert.Equal(test.req.Assignment.Connection, gtResp.Assignment.Connection)
		}
	}
}

// TestFrontendService tests creating, getting and deleting a ticket using Frontend service.
func TestFrontendService(t *testing.T) {
	assert := assert.New(t)

	tc := createMinimatchForTest(t)
	fe := pb.NewFrontendClient(tc.MustGRPC())
	assert.NotNil(fe)

	ticket := &pb.Ticket{
		Properties: &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"test-property": {Kind: &structpb.Value_NumberValue{NumberValue: 1}},
			},
		},
		Assignment: &pb.Assignment{
			Connection: "test-tbd",
		},
	}

	// Create a ticket, validate that it got an id and set its id in the expected ticket.
	resp, err := fe.CreateTicket(context.Background(), &pb.CreateTicketRequest{Ticket: ticket})
	assert.NotNil(resp)
	assert.Nil(err)
	want := resp.Ticket
	assert.NotNil(want)
	assert.NotNil(want.Id)
	ticket.Id = want.Id
	validateTicket(t, resp.Ticket, ticket)

	// Fetch the ticket and validate that it is identical to the expected ticket.
	gotTicket, err := fe.GetTicket(context.Background(), &pb.GetTicketRequest{TicketId: ticket.Id})
	assert.NotNil(gotTicket)
	assert.Nil(err)
	validateTicket(t, gotTicket, ticket)

	// Delete the ticket and validate that it was actually deleted.
	_, err = fe.DeleteTicket(context.Background(), &pb.DeleteTicketRequest{TicketId: ticket.Id})
	assert.Nil(err)
	validateDelete(t, fe, ticket.Id)
}

func TestQueryTickets(t *testing.T) {
	tests := []struct {
		description   string
		req           *pb.QueryTicketsRequest
		preAction     func(fe pb.FrontendClient, t *testing.T)
		wantCode      codes.Code
		wantTickets   []*pb.Ticket
		wantPageCount int
	}{
		{
			description:   "expects invalid argument code since pool is empty",
			preAction:     func(_ pb.FrontendClient, _ *testing.T) {},
			req:           &pb.QueryTicketsRequest{},
			wantCode:      codes.InvalidArgument,
			wantTickets:   nil,
			wantPageCount: 0,
		},
		{
			description: "expects response with no tickets since the store is empty",
			preAction:   func(_ pb.FrontendClient, _ *testing.T) {},
			req: &pb.QueryTicketsRequest{
				Pool: &pb.Pool{
					Filter: []*pb.Filter{{
						Attribute: "ok",
					}},
				},
			},
			wantCode:      codes.OK,
			wantTickets:   nil,
			wantPageCount: 0,
		},
		{
			description: "expects response with no tickets since all tickets in the store are filtered out",
			preAction: func(fe pb.FrontendClient, t *testing.T) {
				tickets := internalTesting.GenerateTickets(
					internalTesting.PropertyManifest{Name: map1attribute, Min: 0, Max: 10, Interval: 2},
					internalTesting.PropertyManifest{Name: map2attribute, Min: 0, Max: 10, Interval: 2},
				)

				for _, ticket := range tickets {
					resp, err := fe.CreateTicket(context.Background(), &pb.CreateTicketRequest{Ticket: ticket})
					assert.NotNil(t, resp)
					assert.Nil(t, err)
				}
			},
			req: &pb.QueryTicketsRequest{
				Pool: &pb.Pool{
					Filter: []*pb.Filter{{
						Attribute: skillattribute,
					}},
				},
			},
			wantCode:      codes.OK,
			wantTickets:   nil,
			wantPageCount: 0,
		},
		{
			description: "expects response with 5 tickets with map1attribute=2 and map2attribute in range of [0,10)",
			preAction: func(fe pb.FrontendClient, t *testing.T) {
				tickets := internalTesting.GenerateTickets(
					internalTesting.PropertyManifest{Name: map1attribute, Min: 0, Max: 10, Interval: 2},
					internalTesting.PropertyManifest{Name: map2attribute, Min: 0, Max: 10, Interval: 2},
				)

				for _, ticket := range tickets {
					resp, err := fe.CreateTicket(context.Background(), &pb.CreateTicketRequest{Ticket: ticket})
					assert.NotNil(t, resp)
					assert.Nil(t, err)
				}
			},
			req: &pb.QueryTicketsRequest{
				Pool: &pb.Pool{
					Filter: []*pb.Filter{{
						Attribute: map1attribute,
						Min:       1,
						Max:       3,
					}},
				},
			},
			wantCode: codes.OK,
			wantTickets: internalTesting.GenerateTickets(
				internalTesting.PropertyManifest{Name: map1attribute, Min: 2, Max: 3, Interval: 2},
				internalTesting.PropertyManifest{Name: map2attribute, Min: 0, Max: 10, Interval: 2},
			),
			wantPageCount: 1,
		},
		{
			// Test inclusive filters and paging works as expected
			description: "expects response with 15 tickets with map1attribute=2,4,6 and map2attribute=[0,10)",
			preAction: func(fe pb.FrontendClient, t *testing.T) {
				tickets := internalTesting.GenerateTickets(
					internalTesting.PropertyManifest{Name: map1attribute, Min: 0, Max: 10, Interval: 2},
					internalTesting.PropertyManifest{Name: map2attribute, Min: 0, Max: 10, Interval: 2},
				)

				for _, ticket := range tickets {
					resp, err := fe.CreateTicket(context.Background(), &pb.CreateTicketRequest{Ticket: ticket})
					assert.NotNil(t, resp)
					assert.Nil(t, err)
				}
			},
			req: &pb.QueryTicketsRequest{
				Pool: &pb.Pool{
					Filter: []*pb.Filter{{
						Attribute: map1attribute,
						Min:       2,
						Max:       6,
					}},
				},
			},
			wantCode: codes.OK,
			wantTickets: internalTesting.GenerateTickets(
				internalTesting.PropertyManifest{Name: map1attribute, Min: 2, Max: 7, Interval: 2},
				internalTesting.PropertyManifest{Name: map2attribute, Min: 0, Max: 10, Interval: 2},
			),
			wantPageCount: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			tc := createMinimatchForTest(t)
			defer tc.Close()

			mml := pb.NewMmLogicClient(tc.MustGRPC())
			fe := pb.NewFrontendClient(tc.MustGRPC())
			pageCounts := 0

			test.preAction(fe, t)

			stream, err := mml.QueryTickets(tc.Context(), test.req)
			assert.Nil(t, err)

			var actualTickets []*pb.Ticket

			for {
				resp, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					assert.Equal(t, test.wantCode, status.Convert(err).Code())
					break
				}

				actualTickets = append(actualTickets, resp.Ticket...)
				pageCounts++
			}

			require.Equal(t, len(test.wantTickets), len(actualTickets))
			// Test fields by fields because of the randomness of the ticket ids...
			// TODO: this makes testing overcomplicated. Should figure out a way to avoid the randomness
			// This for loop also relies on the fact that redis range query and the ticket generator both returns tickets in sorted order.
			// If this fact changes, we might need an ugly nested for loop to do the validness checks.
			for i := 0; i < len(actualTickets); i++ {
				assert.Equal(t, test.wantTickets[i].GetAssignment(), actualTickets[i].GetAssignment())
				assert.Equal(t, test.wantTickets[i].GetProperties(), actualTickets[i].GetProperties())
			}
			assert.Equal(t, test.wantPageCount, pageCounts)
		})
	}
}
