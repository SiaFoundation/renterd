package client_test

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
	"go.sia.tech/renterd/build"
	"go.sia.tech/renterd/bus/client"
	"go.sia.tech/renterd/config"
	"go.sia.tech/renterd/internal/node"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

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
	if err := c.Setting(ctx, "foo", nil); err == nil || !strings.Contains(err.Error(), api.ErrSettingNotFound.Error()) {
		t.Fatal("unexpected err", err)
	}

	// update setting 'foo'
	if err := c.UpdateSetting(ctx, "foo", "bar"); err != nil {
		t.Fatal(err)
	}

	// fetch setting 'foo' and assert it matches
	var value string
	if err := c.Setting(ctx, "foo", &value); err != nil {
		t.Fatal("unexpected err", err)
	} else if value != "bar" {
		t.Fatal("unexpected result", value)
	}

	// fetch redundancy settings and assert they're configured to the default values
	if rs, err := c.RedundancySettings(ctx); err != nil {
		t.Fatal(err)
	} else if rs.MinShards != build.DefaultRedundancySettings.MinShards || rs.TotalShards != build.DefaultRedundancySettings.TotalShards {
		t.Fatal("unexpected redundancy settings", rs)
	}
}

func newTestClient(dir string) (*client.Client, func() error, func(context.Context) error, error) {
	// create listener
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, nil, err
	}

	// create client
	client := client.New("http://"+l.Addr().String(), "test")
	b, cleanup, err := node.NewBus(node.BusConfig{
		Bus: config.Bus{
			AnnouncementMaxAgeHours: 24 * 7 * 52, // 1 year
			Bootstrap:               false,
			GatewayAddr:             "127.0.0.1:0",
			UsedUTXOExpiry:          time.Minute,
		},
		Miner: node.NewMiner(client),
	}, filepath.Join(dir, "bus"), types.GeneratePrivateKey(), zap.New(zapcore.NewNopCore()))
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
		return cleanup(ctx)
	}
	return client, serveFn, shutdownFn, nil
}
