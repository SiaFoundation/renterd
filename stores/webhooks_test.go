package stores

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.sia.tech/renterd/webhooks"
)

func TestWebhooks(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	wh1 := webhooks.Webhook{
		Module: "foo",
		Event:  "bar",
		URL:    "http://example.com",
	}
	wh2 := webhooks.Webhook{
		Module: "foo2",
		Event:  "bar2",
		URL:    "http://example2.com",
	}

	// Add hook.
	if err := ss.AddWebhook(wh1); err != nil {
		t.Fatal(err)
	}
	whs, err := ss.Webhooks()
	if err != nil {
		t.Fatal(err)
	} else if len(whs) != 1 {
		t.Fatal("expected 1 webhook")
	} else if !cmp.Equal(whs[0], wh1) {
		t.Fatal("unexpected webhook", cmp.Diff(whs[0], wh1))
	}

	// Add it again. Should be a no-op.
	if err := ss.AddWebhook(wh1); err != nil {
		t.Fatal(err)
	}
	whs, err = ss.Webhooks()
	if err != nil {
		t.Fatal(err)
	} else if len(whs) != 1 {
		t.Fatal("expected 1 webhook")
	} else if !cmp.Equal(whs[0], wh1) {
		t.Fatal("unexpected webhook", cmp.Diff(whs[0], wh1))
	}

	// Add another.
	if err := ss.AddWebhook(wh2); err != nil {
		t.Fatal(err)
	}
	whs, err = ss.Webhooks()
	if err != nil {
		t.Fatal(err)
	} else if len(whs) != 2 {
		t.Fatal("expected 2 webhooks", len(whs))
	} else if !cmp.Equal(whs[0], wh1) {
		t.Fatal("unexpected webhook", cmp.Diff(whs[0], wh1))
	} else if !cmp.Equal(whs[1], wh2) {
		t.Fatal("unexpected webhook", cmp.Diff(whs[1], wh2))
	}

	// Remove one.
	if err := ss.DeleteWebhook(wh1); err != nil {
		t.Fatal(err)
	}
	whs, err = ss.Webhooks()
	if err != nil {
		t.Fatal(err)
	} else if len(whs) != 1 {
		t.Fatal("expected 1 webhook")
	} else if !cmp.Equal(whs[0], wh2) {
		t.Fatal("unexpected webhook", cmp.Diff(whs[0], wh2))
	}
}
