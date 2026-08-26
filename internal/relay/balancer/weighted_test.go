package balancer

import (
	"math/rand"
	"testing"

	"github.com/bestruirui/octopus/internal/model"
)

func TestWeightedCandidatesUsesProportionalFirstChoice(t *testing.T) {
	items := []model.GroupItem{
		{ChannelID: 1, Weight: 1},
		{ChannelID: 2, Weight: 9},
	}
	rng := rand.New(rand.NewSource(1))
	const samples = 10000
	highWeightFirst := 0
	for range samples {
		candidates := weightedCandidates(items, rng.Float64)
		if len(candidates) != len(items) {
			t.Fatalf("expected %d candidates, got %d", len(items), len(candidates))
		}
		if candidates[0].ChannelID == 2 {
			highWeightFirst++
		}
	}

	ratio := float64(highWeightFirst) / samples
	if ratio < 0.87 || ratio > 0.93 {
		t.Fatalf("expected weight 9/10 to win about 90%% of first choices, got %.2f%%", ratio*100)
	}
}

func TestWeightedCandidatesFallsBackWithoutDuplicates(t *testing.T) {
	items := []model.GroupItem{
		{ChannelID: 1, Weight: 1},
		{ChannelID: 2, Weight: 3},
		{ChannelID: 3, Weight: 6},
	}
	candidates := weightedCandidates(items, func() float64 { return 0.99 })
	if len(candidates) != len(items) {
		t.Fatalf("expected %d candidates, got %d", len(items), len(candidates))
	}
	seen := make(map[int]struct{}, len(candidates))
	for _, item := range candidates {
		if _, ok := seen[item.ChannelID]; ok {
			t.Fatalf("duplicate channel %d in weighted candidates", item.ChannelID)
		}
		seen[item.ChannelID] = struct{}{}
	}
}
