package stores

import (
	"context"

	"go.sia.tech/renterd/webhooks"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type (
	dbWebhook struct {
		Model

		Module string `gorm:"uniqueIndex:idx_module_event_url;NOT NULL;size:255"`
		Event  string `gorm:"uniqueIndex:idx_module_event_url;NOT NULL;size:255"`
		URL    string `gorm:"uniqueIndex:idx_module_event_url;NOT NULL;size:255"`

		Headers map[string]string `gorm:"serializer:json"`
	}
)

func (dbWebhook) TableName() string {
	return "webhooks"
}

func (s *SQLStore) DeleteWebhook(ctx context.Context, wb webhooks.Webhook) error {
	return s.retryTransaction(ctx, func(tx *gorm.DB) error {
		res := tx.Exec("DELETE FROM webhooks WHERE module = ? AND event = ? AND url = ?",
			wb.Module, wb.Event, wb.URL)
		if res.Error != nil {
			return res.Error
		} else if res.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}
		return nil
	})
}

func (s *SQLStore) AddWebhook(ctx context.Context, wh webhooks.Webhook) error {
	return s.retryTransaction(ctx, func(tx *gorm.DB) error {
		return tx.Clauses(clause.OnConflict{
			DoNothing: true,
		}).Create(&dbWebhook{
			Module:  wh.Module,
			Event:   wh.Event,
			URL:     wh.URL,
			Headers: wh.Headers,
		}).Error
	})
}

func (s *SQLStore) Webhooks(ctx context.Context) ([]webhooks.Webhook, error) {
	var dbWebhooks []dbWebhook
	if err := s.db.WithContext(ctx).Find(&dbWebhooks).Error; err != nil {
		return nil, err
	}
	var whs []webhooks.Webhook
	for _, wb := range dbWebhooks {
		whs = append(whs, webhooks.Webhook{
			Module:  wb.Module,
			Event:   wb.Event,
			URL:     wb.URL,
			Headers: wb.Headers,
		})
	}
	return whs, nil
}
