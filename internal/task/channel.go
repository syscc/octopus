package task

import (
	"context"
	"time"

	"github.com/bestruirui/octopus/internal/helper"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/utils/log"
)

func ChannelBaseUrlDelayTask() {
	log.Debugf("channel base url delay task started")
	startTime := time.Now()
	defer func() {
		log.Debugf("channel base url delay task finished, update time: %s", time.Since(startTime))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	channels, err := op.ChannelList(ctx)
	if err != nil {
		log.Errorf("failed to list channels: %v", err)
		return
	}
	for _, channel := range channels {
		helper.ChannelBaseUrlDelayUpdate(&channel, ctx)
	}
}

// OpenAIProtocolUnsupportedExpiryTask 把过了 TTL 的 unsupported 协议判定回落为
// unknown。运行时学习只写 supported/unsupported，从不写回 unknown，所以没有这个
// 任务，一个上游后来补上的协议永远不会被重新试探。到期后该协议重新变成 unknown，
// 下次请求会付出最多一次探路成本；探到支持就自动打勾，仍不支持则重新标记并续期。
func OpenAIProtocolUnsupportedExpiryTask() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	reset, err := op.ChannelExpireOpenAIProtocolUnsupported(ctx, model.OpenAIProtocolUnsupportedTTL, time.Now())
	if err != nil {
		log.Warnf("openai protocol unsupported expiry task failed: %v", err)
	}
	if reset > 0 {
		log.Debugf("openai protocol unsupported expiry reset %d capability verdicts", reset)
	}
}
