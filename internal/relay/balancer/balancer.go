package balancer

import (
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/bestruirui/octopus/internal/model"
)

var roundRobinCounter uint64

// Balancer 根据负载均衡模式选择通道
type Balancer interface {
	// Candidates 返回按策略排序的候选列表
	// 调用方在遍历候选列表时自行检查熔断状态
	Candidates(items []model.GroupItem) []model.GroupItem
}

// GetBalancer 根据模式返回对应的负载均衡器
func GetBalancer(mode model.GroupMode) Balancer {
	switch mode {
	case model.GroupModeRoundRobin:
		return &RoundRobin{}
	case model.GroupModeRandom:
		return &Random{}
	case model.GroupModeFailover:
		return &Failover{}
	case model.GroupModeWeighted:
		return &Weighted{}
	default:
		return &RoundRobin{}
	}
}

// RoundRobin 轮询：从上次位置开始轮转排列
type RoundRobin struct{}

func (b *RoundRobin) Candidates(items []model.GroupItem) []model.GroupItem {
	n := len(items)
	if n == 0 {
		return nil
	}
	idx := int(atomic.AddUint64(&roundRobinCounter, 1) % uint64(n))
	result := make([]model.GroupItem, n)
	for i := 0; i < n; i++ {
		result[i] = items[(idx+i)%n]
	}
	return result
}

// Random 随机：随机打乱所有 items
type Random struct{}

func (b *Random) Candidates(items []model.GroupItem) []model.GroupItem {
	n := len(items)
	if n == 0 {
		return nil
	}
	result := make([]model.GroupItem, n)
	copy(result, items)
	rand.Shuffle(n, func(i, j int) {
		result[i], result[j] = result[j], result[i]
	})
	return result
}

// Failover 故障转移：按优先级排序
type Failover struct{}

func (b *Failover) Candidates(items []model.GroupItem) []model.GroupItem {
	if len(items) == 0 {
		return nil
	}
	return sortByPriority(items)
}

// Weighted 加权分配：按权重无放回抽样，首选概率与权重成正比。
type Weighted struct{}

func (b *Weighted) Candidates(items []model.GroupItem) []model.GroupItem {
	return weightedCandidates(items, rand.Float64)
}

func weightedCandidates(items []model.GroupItem, random func() float64) []model.GroupItem {
	if len(items) == 0 {
		return nil
	}
	if random == nil {
		random = rand.Float64
	}

	remaining := append([]model.GroupItem(nil), items...)
	result := make([]model.GroupItem, 0, len(remaining))
	for len(remaining) > 0 {
		totalWeight := 0
		for _, item := range remaining {
			weight := item.Weight
			if weight <= 0 {
				weight = 1
			}
			totalWeight += weight
		}

		target := random() * float64(totalWeight)
		cumulative := 0.0
		selected := len(remaining) - 1
		for i, item := range remaining {
			weight := item.Weight
			if weight <= 0 {
				weight = 1
			}
			cumulative += float64(weight)
			if target < cumulative {
				selected = i
				break
			}
		}
		result = append(result, remaining[selected])
		remaining = append(remaining[:selected], remaining[selected+1:]...)
	}
	return result
}

func sortByPriority(items []model.GroupItem) []model.GroupItem {
	sorted := make([]model.GroupItem, len(items))
	copy(sorted, items)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Priority < sorted[j].Priority
	})
	return sorted
}

// Reset clears in-memory balancer state for tests.
func Reset() {
	roundRobinCounter = 0
	globalBreaker = sync.Map{}
	globalSession = sync.Map{}
}
