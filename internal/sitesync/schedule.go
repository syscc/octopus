package sitesync

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
)

func buildRandomCheckinDueAt(account *model.SiteAccount, baseline time.Time) *time.Time {
	if account == nil || !account.Enabled || !account.AutoCheckin || !account.RandomCheckin {
		return nil
	}

	windowMinutes := account.CheckinRandomWindowMinutes
	if windowMinutes < 0 {
		windowMinutes = 0
	}
	dueAt := baseline
	if windowMinutes > 0 {
		dueAt = dueAt.Add(time.Duration(rand.Intn(windowMinutes+1)) * time.Minute)
	}

	intervalHours := account.CheckinIntervalHours
	if intervalHours <= 0 {
		intervalHours = 24
	}
	if account.LastCheckinAt != nil && !account.LastCheckinAt.IsZero() && account.LastCheckinStatus == model.SiteExecutionStatusSuccess {
		earliest := account.LastCheckinAt.Add(time.Duration(intervalHours) * time.Hour)
		if earliest.After(dueAt) {
			dueAt = earliest
		}
	}

	next := dueAt
	return &next
}

func armRandomCheckinPending(ctx context.Context, account *model.SiteAccount, baseline time.Time) (*time.Time, error) {
	if account == nil || !account.RandomCheckin {
		return nil, nil
	}
	if account.NextAutoCheckinAt != nil && !account.NextAutoCheckinAt.IsZero() {
		return account.NextAutoCheckinAt, nil
	}

	nextAt := buildRandomCheckinDueAt(account, baseline)
	if err := persistNextAutoCheckinAt(ctx, account.ID, nextAt); err != nil {
		return nil, err
	}
	account.NextAutoCheckinAt = nextAt
	return nextAt, nil
}

func persistNextAutoCheckinAt(ctx context.Context, accountID int, nextAt *time.Time) error {
	return db.GetDB().WithContext(ctx).
		Model(&model.SiteAccount{}).
		Where("id = ?", accountID).
		Update("next_auto_checkin_at", nextAt).Error
}

func RefreshAccountRandomCheckinSchedule(ctx context.Context, accountID int) error {
	account, err := op.SiteAccountGet(accountID, ctx)
	if err != nil {
		return fmt.Errorf("site account not found")
	}
	if account.Enabled && account.AutoCheckin && account.RandomCheckin {
		// The global interval/cron task owns random work creation. Preserve an
		// existing pending execution, but do not invent one while an account is
		// merely being created or edited.
		return nil
	}
	return persistNextAutoCheckinAt(ctx, account.ID, nil)
}
