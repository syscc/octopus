package balancer

import (
	"testing"

	"github.com/bestruirui/octopus/internal/model"
)

func TestIteratorPreferProtocolRankStableReorder(t *testing.T) {
	it := &Iterator{
		candidates: []model.GroupItem{{ChannelID: 1}, {ChannelID: 2}, {ChannelID: 3}, {ChannelID: 4}},
		index:      -1,
		stickyIdx:  -1,
	}
	it.PreferProtocolRank(func(item model.GroupItem) int {
		if item.ChannelID == 2 || item.ChannelID == 4 {
			return 0
		}
		return 1
	})

	assertIteratorChannelOrder(t, it, []int{2, 4, 1, 3})
}

func TestIteratorPreferProtocolRankKeepsStickyFirstWithoutLosingKey(t *testing.T) {
	it := &Iterator{
		candidates:  []model.GroupItem{{ChannelID: 5}, {ChannelID: 1}, {ChannelID: 2}, {ChannelID: 3}},
		index:       -1,
		stickyIdx:   0,
		stickyKeyID: 9,
	}
	it.PreferProtocolRank(func(item model.GroupItem) int {
		if item.ChannelID == 2 || item.ChannelID == 3 {
			return 0
		}
		return 1
	})

	assertIteratorChannelOrder(t, it, []int{5, 2, 3, 1})
	if it.stickyIdx != 0 {
		t.Fatalf("expected sticky channel to remain first, got index %d", it.stickyIdx)
	}
	if it.stickyKeyID != 9 {
		t.Fatalf("expected sticky key id preserved, got %d", it.stickyKeyID)
	}
}

func TestIteratorPreferProtocolRankAllEqualKeepsOrder(t *testing.T) {
	it := &Iterator{
		candidates: []model.GroupItem{{ChannelID: 1}, {ChannelID: 2}},
		index:      -1,
		stickyIdx:  -1,
	}
	it.PreferProtocolRank(func(model.GroupItem) int { return 0 })
	assertIteratorChannelOrder(t, it, []int{1, 2})
}

func assertIteratorChannelOrder(t *testing.T, it *Iterator, want []int) {
	t.Helper()
	got := make([]int, 0, len(want))
	for it.Next() {
		got = append(got, it.Item().ChannelID)
	}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected %v, got %v", want, got)
		}
	}
}
