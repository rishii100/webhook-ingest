// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const recordingWork = 50 * time.Millisecond

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger
	wg    sync.WaitGroup
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	return &Service{store: s, cache: c, rdb: rdb, log: log}
}

// Wait blocks until all background goroutines (e.g. recording processing)
// have finished. Call this during graceful shutdown after the HTTP server has
// stopped accepting new requests.
func (s *Service) Wait() {
	s.wg.Wait()
}

// Stats returns the cached totals for an account.
func (s *Service) Stats(accountID string) stats.AccountStats {
	return s.cache.Get(accountID)
}

// Ingest stores a delivery and kicks off processing. Processing runs
// asynchronously so the provider gets a fast acknowledgement.
//
// Idempotency is guaranteed by two layers:
//  1. Redis SET NX — fast-path dedup that avoids hitting Postgres for
//     known redeliveries. The key expires after 24 hours.
//  2. Postgres UNIQUE constraint on event_id with INSERT … ON CONFLICT
//     DO NOTHING — durable guarantee that survives Redis restarts.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	// Layer 1: Redis fast-path dedup.
	// SET key NX EX 86400 — only succeeds if the key does not already exist.
	dedupKey := "event:" + evt.EventID
	set, err := s.rdb.SetNX(ctx, dedupKey, "1", 24*time.Hour).Result()
	if err != nil {
		// Redis errors are not fatal — fall through to Postgres dedup.
		s.log.Warn("redis dedup check failed, falling through to postgres",
			"event_id", evt.EventID, "err", err)
	} else if !set {
		// Key already existed — this is a known redelivery.
		s.log.Info("duplicate delivery rejected by redis", "event_id", evt.EventID)
		return nil
	}

	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}

	// Layer 2: Postgres INSERT … ON CONFLICT (event_id) DO NOTHING.
	// Returns true only if the row was actually inserted.
	inserted, err := s.store.InsertEvent(ctx, rec)
	if err != nil {
		return err
	}
	if !inserted {
		// Another concurrent request inserted this event_id between our
		// Redis check and this INSERT — safe to bail out.
		s.log.Info("duplicate delivery rejected by postgres", "event_id", evt.EventID)
		return nil
	}

	if err := s.store.UpsertCall(ctx, rec); err != nil {
		return err
	}
	if err := s.store.IncrementAccountStats(ctx, rec.AccountID, rec.DurationSec); err != nil {
		return err
	}
	s.cache.Record(rec.AccountID, rec.DurationSec)

	// Recordings are slow to fetch, so that part does not block the provider.
	// We use context.Background() because the request context (r.Context())
	// is cancelled as soon as the HTTP handler returns, which would cause the
	// DB update in MarkRecordingProcessed to fail with context.Canceled.
	if rec.RecordingURL != "" {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := s.processRecording(context.Background(), rec); err != nil {
				s.log.Error("process recording failed",
					"call_id", rec.CallID,
					"event_id", rec.EventID,
					"err", err,
				)
			}
		}()
	}

	return nil
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(ctx context.Context, rec store.Event) error {
	time.Sleep(recordingWork)
	return s.store.MarkRecordingProcessed(ctx, rec.CallID)
}
