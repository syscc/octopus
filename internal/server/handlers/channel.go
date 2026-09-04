package handlers

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bestruirui/octopus/internal/helper"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/relay"
	"github.com/bestruirui/octopus/internal/server/middleware"
	"github.com/bestruirui/octopus/internal/server/resp"
	"github.com/bestruirui/octopus/internal/server/router"
	"github.com/bestruirui/octopus/internal/task"
	"github.com/bestruirui/octopus/internal/utils/log"
	"github.com/bestruirui/octopus/internal/utils/safe"
	"github.com/gin-gonic/gin"
)

func init() {
	router.NewGroupRouter("/api/v1/channel").
		Use(middleware.Auth()).
		Use(middleware.RequireJSON()).
		AddRoute(
			router.NewRoute("/list", http.MethodGet).
				Handle(listChannel),
		).
		AddRoute(
			router.NewRoute("/create", http.MethodPost).
				Handle(createChannel),
		).
		AddRoute(
			router.NewRoute("/update", http.MethodPost).
				Handle(updateChannel),
		).
		AddRoute(
			router.NewRoute("/enable", http.MethodPost).
				Handle(enableChannel),
		).
		AddRoute(
			router.NewRoute("/delete/:id", http.MethodDelete).
				Handle(deleteChannel),
		).
		AddRoute(
			router.NewRoute("/fetch-model", http.MethodPost).
				Handle(fetchModel),
		).
		AddRoute(
			router.NewRoute("/probe-openai-protocol", http.MethodPost).
				Handle(probeOpenAIProtocol),
		)
	router.NewGroupRouter("/api/v1/channel").
		Use(middleware.Auth()).
		AddRoute(
			router.NewRoute("/sync", http.MethodPost).
				Handle(syncChannel),
		).
		AddRoute(
			router.NewRoute("/last-sync-time", http.MethodGet).
				Handle(getLastSyncTime),
		)
}

func listChannel(c *gin.Context) {
	channels, err := op.ChannelList(c.Request.Context())
	if err != nil {
		resp.Error(c, http.StatusInternalServerError, err.Error())
		return
	}
	channelIDs := make([]int, 0, len(channels))
	for _, channel := range channels {
		channelIDs = append(channelIDs, channel.ID)
	}
	bindingMap, err := op.SiteChannelBindingMapByChannelIDs(channelIDs, c.Request.Context())
	if err != nil {
		resp.Error(c, http.StatusInternalServerError, err.Error())
		return
	}
	for i, channel := range channels {
		stats := op.StatsChannelGet(channel.ID)
		channels[i].Stats = &stats
		if binding, ok := bindingMap[channel.ID]; ok {
			channels[i].Managed = true
			channels[i].ManagedSource = &model.ManagedChannelSource{
				SiteID:          binding.SiteID,
				SiteAccountID:   binding.SiteAccountID,
				SiteUserGroupID: binding.SiteUserGroupID,
				GroupKey:        binding.GroupKey,
			}
		}
	}
	resp.Success(c, channels)
}

func createChannel(c *gin.Context) {
	var channel model.Channel
	if err := c.ShouldBindJSON(&channel); err != nil {
		resp.InvalidJSON(c)
		return
	}
	if channel.ProxyMode == "" {
		channel.ProxyMode = model.ProxyUsageModeDirect
	}
	if err := channel.ProxyMode.Validate(false); err != nil {
		resp.Error(c, http.StatusBadRequest, err.Error())
		return
	}
	if channel.ProxyMode == model.ProxyUsageModePool && (channel.ProxyConfigID == nil || *channel.ProxyConfigID <= 0) {
		resp.Error(c, http.StatusBadRequest, "proxy config id is required when proxy mode is pool")
		return
	}
	if channel.ProxyMode == model.ProxyUsageModePool {
		if _, err := op.ProxyURLForConfig(*channel.ProxyConfigID, c.Request.Context()); err != nil {
			resp.Error(c, http.StatusBadRequest, err.Error())
			return
		}
	}
	if channel.ProxyMode != model.ProxyUsageModePool {
		channel.ProxyConfigID = nil
	}
	if err := op.ChannelCreate(&channel, c.Request.Context()); err != nil {
		resp.ErrorWithAppError(c, http.StatusInternalServerError, channelError(codeChannelCreateFailed, "channel create failed", err))
		return
	}
	stats := op.StatsChannelGet(channel.ID)
	channel.Stats = &stats
	createdChannel := channel
	safe.Go("channel-create-postprocess", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		modelStr := createdChannel.Model + "," + createdChannel.CustomModel
		modelArray := strings.Split(modelStr, ",")
		helper.LLMPriceAddToDB(modelArray, ctx)
		helper.ChannelBaseUrlDelayUpdate(&createdChannel, ctx)
		helper.ChannelAutoGroup(&createdChannel, ctx)
	})
	resp.Success(c, channel)
	if shouldTriggerOpenAIProtocolProbe(&channel, nil) {
		triggerOpenAIProtocolProbe(&channel)
	}
}

func updateChannel(c *gin.Context) {
	var req model.ChannelUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		resp.InvalidJSON(c)
		return
	}
	channel, err := op.ChannelUpdate(&req, c.Request.Context())
	if err != nil {
		resp.ErrorWithAppError(c, http.StatusInternalServerError, channelError(codeChannelUpdateFailed, "channel update failed", err))
		return
	}
	stats := op.StatsChannelGet(channel.ID)
	channel.Stats = &stats
	updatedChannel := *channel
	safe.Go("channel-update-postprocess", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		modelStr := updatedChannel.Model + "," + updatedChannel.CustomModel
		modelArray := strings.Split(modelStr, ",")
		helper.LLMPriceAddToDB(modelArray, ctx)
		helper.ChannelBaseUrlDelayUpdate(&updatedChannel, ctx)
		helper.ChannelAutoGroup(&updatedChannel, ctx)
	})
	resp.Success(c, channel)
	if shouldTriggerOpenAIProtocolProbe(channel, &req) {
		triggerOpenAIProtocolProbe(channel)
	}
}

func probeOpenAIProtocol(c *gin.Context) {
	var request struct {
		ID int `json:"id" binding:"required"`
	}
	if err := c.ShouldBindJSON(&request); err != nil {
		resp.InvalidJSON(c)
		return
	}
	if request.ID <= 0 {
		resp.InvalidParam(c)
		return
	}

	// Probe directly: relay.ProbeChannelOpenAIProtocols performs its own
	// authoritative load, so an extra pre-check here would only add a
	// check-then-act race (TOCTOU) and a second database round trip.
	report, err := relay.ProbeChannelOpenAIProtocols(request.ID, c.Request.Context())
	if err != nil {
		// Missing channels are a stable typed 404. Every other failure (database
		// or upstream) stays an opaque route-specific 500. Do not serialize the
		// underlying error: it may contain a private base URL or upstream body.
		if errors.Is(err, op.ErrChannelNotFound) {
			resp.ErrorWithCode(c, http.StatusNotFound, codeChannelNotFound, "channel not found")
			return
		}
		log.Warnf("OpenAI protocol probe failed (channel=%d)", request.ID)
		resp.ErrorWithAppError(c, http.StatusInternalServerError, channelError(codeChannelProbeFailed, "channel protocol probe failed", err))
		return
	}

	// Reload once after persistence so the response reflects the authoritative
	// mode and effective capabilities, including manual overrides. A reload
	// failure never fails the request: BuildProtocolProbeAPIResponse falls back
	// to the safe snapshot captured inside the report.
	after, reloadErr := op.ChannelGetAuthoritative(request.ID, c.Request.Context())
	if reloadErr != nil {
		log.Warnf("authoritative channel reload for protocol probe failed (channel=%d)", request.ID)
		after = nil
	}
	resp.Success(c, relay.BuildProtocolProbeAPIResponse(request.ID, report, after))
}

// shouldTriggerOpenAIProtocolProbe limits automatic probes to persisted changes
// that can affect endpoint compatibility or the transport used to check it.
func shouldTriggerOpenAIProtocolProbe(channel *model.Channel, req *model.ChannelUpdateRequest) bool {
	if channel == nil || channel.ID <= 0 || !model.IsOpenAITextChannelType(channel.Type) ||
		channel.OpenAIProtocolMode.Normalize() != model.OpenAIProtocolModeAuto {
		return false
	}
	if req == nil {
		return true
	}
	if req.Type != nil || req.BaseUrls != nil || req.Model != nil || req.CustomModel != nil ||
		req.CustomHeader != nil || req.ParamOverride != nil || req.ProxyMode != nil || req.ProxyConfigID != nil ||
		len(req.KeysToAdd) > 0 || len(req.KeysToDelete) > 0 {
		return true
	}
	if req.OpenAIProtocolMode != nil {
		return req.OpenAIProtocolMode.Normalize() == model.OpenAIProtocolModeAuto
	}
	for _, key := range req.KeysToUpdate {
		if key.Enabled != nil || key.ChannelKey != nil {
			return true
		}
	}
	return false
}

func triggerOpenAIProtocolProbe(channel *model.Channel) {
	if channel == nil || !model.IsOpenAITextChannelType(channel.Type) || channel.ID <= 0 ||
		channel.OpenAIProtocolMode.Normalize() != model.OpenAIProtocolModeAuto {
		return
	}
	channelID := channel.ID
	safe.Go("channel-openai-protocol-probe", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		if _, err := relay.ProbeChannelOpenAIProtocols(channelID, ctx); err != nil {
			// The create/update request has already succeeded; probing is best effort.
			// Keep the detail in server logs only and never return upstream content.
			log.Warnf("automatic OpenAI protocol probe failed (channel=%d): %v", channelID, err)
		}
	})
}

func enableChannel(c *gin.Context) {
	var request struct {
		ID      int  `json:"id"`
		Enabled bool `json:"enabled"`
	}
	if err := c.ShouldBindJSON(&request); err != nil {
		resp.InvalidJSON(c)
		return
	}
	if err := op.ChannelEnabled(request.ID, request.Enabled, c.Request.Context()); err != nil {
		resp.ErrorWithAppError(c, http.StatusInternalServerError, channelError(codeChannelUpdateFailed, "channel update failed", err))
		return
	}
	resp.Success(c, nil)
}

func deleteChannel(c *gin.Context) {
	id := c.Param("id")
	idNum, err := strconv.Atoi(id)
	if err != nil {
		resp.InvalidParam(c)
		return
	}
	if err := op.ChannelDel(idNum, c.Request.Context()); err != nil {
		resp.ErrorWithAppError(c, http.StatusInternalServerError, channelError(codeChannelDeleteFailed, "channel delete failed", err))
		return
	}
	resp.Success(c, nil)
}
func fetchModel(c *gin.Context) {
	var request model.Channel
	if err := c.ShouldBindJSON(&request); err != nil {
		resp.InvalidJSON(c)
		return
	}
	models, err := helper.FetchModels(c.Request.Context(), request)
	if err != nil {
		resp.ErrorWithAppError(c, http.StatusInternalServerError, channelError(codeChannelFetchModelsFailed, "channel fetch models failed", err))
		return
	}
	resp.Success(c, models)
}

func syncChannel(c *gin.Context) {
	task.SyncModelsTask()
	resp.Success(c, nil)
}

func getLastSyncTime(c *gin.Context) {
	time := task.GetLastSyncModelsTime()
	resp.Success(c, time)
}
