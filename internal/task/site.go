package task

import (
	"context"
	"time"

	"github.com/bestruirui/octopus/internal/site"
	"github.com/bestruirui/octopus/internal/utils/log"
)

func SiteSyncTask() {
	log.Debugf("site sync task started")
	startTime := time.Now()
	defer func() {
		log.Debugf("site sync task finished, update time: %s", time.Since(startTime))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()
	site.SyncAll(ctx)
}

func Sub2APISessionRefreshTask() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	refreshed, err := site.RefreshDueSub2APISessions(ctx)
	if err != nil {
		log.Warnf("sub2api session refresh task failed: %v", err)
	} else if refreshed > 0 {
		log.Infof("sub2api session refresh task updated %d account(s)", refreshed)
	}
}

func SiteCheckinTask() {
	log.Debugf("site checkin task started")
	startTime := time.Now()
	defer func() {
		log.Debugf("site checkin task finished, update time: %s", time.Since(startTime))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()
	site.CheckinAll(ctx)
}

func SiteRandomCheckinTask() {
	log.Debugf("site random checkin task started")
	startTime := time.Now()
	defer func() {
		log.Debugf("site random checkin task finished, update time: %s", time.Since(startTime))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()
	site.CheckinRandomDue(ctx)
}
