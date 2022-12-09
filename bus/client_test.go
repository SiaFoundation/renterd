package bus_test

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.sia.tech/jape"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/internal/node"
	"go.sia.tech/renterd/internal/utils"
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
	err = c.Setting("foo", nil)
	if err == nil || !strings.Contains(err.Error(), "setting not found") {
		t.Fatal("unexpected err", err)
	}

	// update setting 'foo' to some random type
	want := bus.RedundancySettings{
		MinShards:   10,
		TotalShards: 30,
	}
	if err := c.UpdateSetting("foo", want); err != nil {
		t.Fatal(err)
	}

	// fetch setting 'foo' and assert it matches
	var got bus.RedundancySettings
	if err := c.Setting("foo", &got); err != nil {
		t.Fatal("unexpected err", err)
	}
	if got.MinShards != want.MinShards || got.TotalShards != want.TotalShards {
		wb, _ := json.Marshal(want)
		gb, _ := json.Marshal(got)
		t.Fatal("unexpected result", string(wb), string(gb))
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
		Bootstrap:   false,
		GatewayAddr: "127.0.0.1:0",
		Miner:       node.NewMiner(client),
	}, filepath.Join(dir, "bus"), consensus.GeneratePrivateKey())
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
		return utils.JoinErrors(
			cleanup(),
			server.Shutdown(ctx),
		)
	}
	return client, serveFn, shutdownFn, nil
}
