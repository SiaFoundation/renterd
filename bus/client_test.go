package bus_test

import (
	"context"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/internal/node"
)

var defaultSettings = api.RedundancySettings{
	MinShards:   10,
	TotalShards: 30,
}

func TestClient(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	c, serveFn, shutdownFn, err := newTestClient(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := shutdownFn(ctx); err != nil {
			t.Error(err)
		}
	}()
	go serveFn()

	// assert setting 'foo' is not found
	if _, err := c.Setting("foo"); err == nil || !strings.Contains(err.Error(), api.ErrSettingNotFound.Error()) {
		t.Fatal("unexpected err", err)
	}

	// update setting 'foo'
	if err := c.UpdateSetting("foo", "bar"); err != nil {
		t.Fatal(err)
	}

	// fetch setting 'foo' and assert it matches
	if value, err := c.Setting("foo"); err != nil {
		t.Fatal("unexpected err", err)
	} else if value != "bar" {
		t.Fatal("unexpected result", value)
	}

	// fetch redundancy settings and assert they're configured to the default values
	if rs, err := c.RedundancySettings(); err != nil {
		t.Fatal(err)
	} else if rs.MinShards != defaultSettings.MinShards || rs.TotalShards != defaultSettings.TotalShards {
		t.Fatal("unexpected redundancy settings", rs)
	}
}

func newTestClient(dir string) (*bus.Client, func() error, func(context.Context) error, error) {
	// create listener
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, nil, err
	}

	// create client
	client := bus.NewClient("http://"+l.Addr().String(), "test")

	b, cleanup, err := node.NewBus(node.BusConfig{
		Bootstrap:          false,
		GatewayAddr:        "127.0.0.1:0",
		Miner:              node.NewMiner(client),
		RedundancySettings: defaultSettings,
	}, filepath.Join(dir, "bus"), types.GeneratePrivateKey())
	if err != nil {
		return nil, nil, nil, err
	}

	// create server
	server := http.Server{Handler: jape.BasicAuth("test")(b)}

	serveFn := func() error {
		err := server.Serve(l)
		if err != nil && !strings.Contains(err.Error(), "Server closed") {
			return err
		}
		return nil
	}

	shutdownFn := func(ctx context.Context) error {
		server.Shutdown(ctx)
		return cleanup()
	}
	return client, serveFn, shutdownFn, nil
}
