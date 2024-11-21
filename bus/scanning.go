package bus

import (
	"context"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/utils"
	"go.uber.org/zap"
)

func (b *Bus) scanHostV1(ctx context.Context, timeout time.Duration, hostKey types.PublicKey, hostIP string) (rhpv2.HostSettings, rhpv3.HostPriceTable, time.Duration, error) {
	logger := b.logger.
		With("host", hostKey).
		With("hostIP", hostIP).
		With("timeout", timeout).
		With("version", "v1")

	// prepare a helper to create a context for scanning
	timeoutCtx := func() (context.Context, context.CancelFunc) {
		if timeout > 0 {
			return context.WithTimeout(ctx, timeout)
		}
		return ctx, func() {}
	}

	// prepare a helper for scanning
	scan := func() (rhpv2.HostSettings, rhpv3.HostPriceTable, time.Duration, error) {
		// fetch the host settings
		start := time.Now()
		scanCtx, cancel := timeoutCtx()
		settings, err := b.rhp2Client.Settings(scanCtx, hostKey, hostIP)
		cancel()
		if err != nil {
			return settings, rhpv3.HostPriceTable{}, time.Since(start), err
		}

		// fetch the host pricetable
		scanCtx, cancel = timeoutCtx()
		pt, err := b.rhp3Client.PriceTableUnpaid(scanCtx, hostKey, settings.SiamuxAddr())
		cancel()
		if err != nil {
			return settings, rhpv3.HostPriceTable{}, time.Since(start), err
		}
		return settings, pt.HostPriceTable, time.Since(start), nil
	}

	// resolve host ip, don't scan if the host is on a private network or if it
	// resolves to more than two addresses of the same type
	if err := b.shouldScanAddr(hostIP); err != nil {
		return rhpv2.HostSettings{}, rhpv3.HostPriceTable{}, 0, err
	}

	// scan: first try
	settings, pt, duration, err := scan()
	if err != nil {
		logger = logger.With(zap.Error(err))

		// scan: second try
		select {
		case <-ctx.Done():
			return rhpv2.HostSettings{}, rhpv3.HostPriceTable{}, 0, context.Cause(ctx)
		case <-time.After(time.Second):
		}
		settings, pt, duration, err = scan()

		logger = logger.With("elapsed", duration).With(zap.Error(err))
		if err == nil {
			logger.Info("successfully scanned host on second try")
		} else if !isErrHostUnreachable(err) {
			logger.Infow("failed to scan host")
		}
	}

	// check if the scan failed due to a shutdown - shouldn't be necessary but
	// just in case since recording a failed scan might have serious
	// repercussions
	select {
	case <-ctx.Done():
		return rhpv2.HostSettings{}, rhpv3.HostPriceTable{}, 0, context.Cause(ctx)
	default:
	}

	// record host scan - make sure this is interrupted by the request ctx and
	// not the context with the timeout used to time out the scan itself.
	// Otherwise scans that time out won't be recorded.
	scanErr := b.store.RecordHostScans(ctx, []api.HostScan{
		{
			HostKey:    hostKey,
			PriceTable: pt,

			// NOTE: A scan is considered successful if both fetching the price
			// table and the settings succeeded. Right now scanning can't fail
			// due to a reason that is our fault unless we are offline. If that
			// changes, we should adjust this code to account for that.
			Success:   err == nil,
			Settings:  settings,
			Timestamp: time.Now(),
		},
	})
	if scanErr != nil {
		logger.Errorw("failed to record host scan", zap.Error(scanErr))
	}
	logger.With(zap.Error(err)).Debugw("scanned host", "success", err == nil)
	return settings, pt, duration, err
}

func (b *Bus) scanHostV2(ctx context.Context, timeout time.Duration, hostKey types.PublicKey, hostIP string) (rhpv4.HostSettings, time.Duration, error) {
	logger := b.logger.
		With("host", hostKey).
		With("hostIP", hostIP).
		With("timeout", timeout).
		With("version", "v2")

	// prepare a helper for scanning
	scan := func() (rhpv4.HostSettings, time.Duration, error) {
		// apply the timeout
		scanCtx := ctx
		if timeout > 0 {
			var cancel context.CancelFunc
			scanCtx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
		// fetch the host prices
		start := time.Now()
		settings, err := b.rhp4Client.Settings(scanCtx, hostKey, hostIP)
		if err != nil {
			return settings, time.Since(start), err
		}
		return settings, time.Since(start), nil
	}

	// resolve host ip, don't scan if the host is on a private network or if it
	// resolves to more than two addresses of the same type
	if err := b.shouldScanAddr(hostIP); err != nil {
		return rhpv4.HostSettings{}, 0, err
	}

	// scan: first try
	settings, duration, err := scan()
	if err != nil {
		logger = logger.With(zap.Error(err))

		// scan: second try
		select {
		case <-ctx.Done():
			return rhpv4.HostSettings{}, 0, context.Cause(ctx)
		case <-time.After(time.Second):
		}
		settings, duration, err = scan()

		logger = logger.With("elapsed", duration).With(zap.Error(err))
		if err == nil {
			logger.Info("successfully scanned host on second try")
		} else if !isErrHostUnreachable(err) {
			logger.Infow("failed to scan host")
		}
	}

	// check if the scan failed due to a shutdown - shouldn't be necessary but
	// just in case since recording a failed scan might have serious
	// repercussions
	select {
	case <-ctx.Done():
		return rhpv4.HostSettings{}, 0, context.Cause(ctx)
	default:
	}

	// record host scan - make sure this is interrupted by the request ctx and
	// not the context with the timeout used to time out the scan itself.
	// Otherwise scans that time out won't be recorded.
	scanErr := b.store.RecordHostScans(ctx, []api.HostScan{
		{
			HostKey: hostKey,

			// NOTE: A scan is considered successful if fetching the settings succeeded.
			// Right now scanning can't fail due to a reason that is our fault unless we
			// are offline. If that changes, we should adjust this code to account for
			// that.
			Success:    err == nil,
			V2Settings: settings,
			Timestamp:  time.Now(),
		},
	})
	if scanErr != nil {
		logger.Errorw("failed to record host scan", zap.Error(scanErr))
	}
	logger.With(zap.Error(err)).Debugw("scanned host", "success", err == nil)
	return settings, duration, err
}

// shouldScanAddr checks whether the provided addr should be scanned according
// to the bus's configuration. A scanned address needs to:
// - be resolvable
// - pass the IP checks we enforce on hosts
// - not be a private IP if the bus is configured to disallow private IPs
func (b *Bus) shouldScanAddr(addr string) error {
	resolved, err := utils.ResolveHostIPs(context.Background(), []string{addr})
	if err != nil {
		return err
	} else if err := utils.PerformHostIPChecks(resolved); err != nil {
		return err
	}
	for _, ipAddr := range resolved {
		if utils.IsPrivateIP(ipAddr.IP) && !b.allowPrivateIPs {
			return api.ErrHostOnPrivateNetwork
		}
	}
	return nil
}
