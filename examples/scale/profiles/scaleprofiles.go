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

package profiles

import (
	"fmt"
	"math"

	"open-match.dev/open-match/examples/scale/tickets"
	"open-match.dev/open-match/pkg/pb"
)

func scaleProfiles() []*pb.MatchProfile {
	// mmrRanges := makeRangeFilters(&rangeConfig{
	// 	name:         "mmr",
	// 	min:          0,
	// 	max:          100,
	// 	rangeSize:    10,
	// 	rangeOverlap: 0,
	// })

	var profiles []*pb.MatchProfile
	for _, region := range tickets.Regions {
		for _, platform := range tickets.Platforms {
			// for _, playlist := range tickets.Playlists {
			// for _, mmrRange := range mmrRanges {
			poolName := fmt.Sprintf("%s_%s", region, platform)
			p := &pb.Pool{
				Name: poolName,
				DoubleRangeFilters: []*pb.DoubleRangeFilter{
					{
						DoubleArg: region,
						Min:       0,
						Max:       math.MaxFloat64,
					},
					{
						DoubleArg: platform,
						Min:       0,
						Max:       math.MaxFloat64,
					},
					// {
					// 	DoubleArg: playlist,
					// 	Min:       float64(mmrRange.min),
					// 	Max:       float64(mmrRange.max),
					// },
				},
			}
			prof := &pb.MatchProfile{
				Name:    fmt.Sprintf("Profile_%s", poolName),
				Pools:   []*pb.Pool{p},
				Rosters: []*pb.Roster{makeRosterSlots(p.GetName(), 4)},
			}

			profiles = append(profiles, prof)
			// }
			// }
		}
	}

	return profiles
}
