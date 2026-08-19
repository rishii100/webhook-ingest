package ingest_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/testutil"
)

// eventJSON builds a well-formed call-completion payload.
func eventJSON(eventID, callID, accountID string) string {
	return fmt.Sprintf(`{
	  "event_id":      %q,
	  "call_id":       %q,
	  "account_id":    %q,
	  "status":        "completed",
	  "duration_sec":  143,
	  "recording_url": "https://recordings.example.com/%s.wav",
	  "occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, callID)
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestWebhookStoresEventAndCall(t *testing.T) {
	srv, st, rdb := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st, rdb)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	exists, err := st.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected the event to be stored")
	}

	var gotAccount string
	row := st.Pool().QueryRow(ctx, `SELECT account_id FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&gotAccount); err != nil {
		t.Fatalf("expected a call record for %s: %v", callID, err)
	}
	if gotAccount != accountID {
		t.Fatalf("call belongs to %q, want %q", gotAccount, accountID)
	}
}

func TestDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st, rdb := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st, rdb)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	for i := 0; i < 3; i++ {
		if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	// Only one event row should exist.
	var n int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d copies of %s, want 1", n, eventID)
	}

	// Account stats should reflect exactly one call, not three.
	var callCount int64
	statsRow := st.Pool().QueryRow(ctx,
		`SELECT call_count FROM account_stats WHERE account_id = $1`, accountID)
	if err := statsRow.Scan(&callCount); err != nil {
		t.Fatalf("scan account_stats: %v", err)
	}
	if callCount != 1 {
		t.Fatalf("account_stats call_count = %d, want 1 (duplicates were counted)", callCount)
	}
}

// TestRecordingGetsProcessed verifies that a call's recording is eventually
// marked processed. Before the fix, the background goroutine used the HTTP
// request's context, which was cancelled after the 200 response — so the
// UPDATE never ran and no error was logged.
func TestRecordingGetsProcessed(t *testing.T) {
	srv, st, rdb := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st, rdb)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	// The recording goroutine sleeps for 50ms, so give it some room.
	time.Sleep(200 * time.Millisecond)

	var processed bool
	row := st.Pool().QueryRow(ctx,
		`SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&processed); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !processed {
		t.Fatal("expected recording_processed to be true, but it was false — " +
			"the background goroutine likely failed silently")
	}
}
