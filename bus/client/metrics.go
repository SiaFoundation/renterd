package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/internal/utils"
)

func (c *Client) ContractMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractMetricsQueryOpts) ([]api.ContractMetric, error) {
	values := url.Values{}
	values.Set("start", api.TimeRFC3339(start).String())
	values.Set("n", fmt.Sprint(n))
	values.Set("interval", api.DurationMS(interval).String())
	if opts.ContractID != (types.FileContractID{}) {
		values.Set("contractid", opts.ContractID.String())
	}
	if opts.HostKey != (types.PublicKey{}) {
		values.Set("hostkey", opts.HostKey.String())
	}

	var resp []api.ContractMetric
	if err := c.metric(ctx, api.MetricContract, values, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) ContractPruneMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractPruneMetricsQueryOpts) ([]api.ContractPruneMetric, error) {
	values := url.Values{}
	values.Set("start", api.TimeRFC3339(start).String())
	values.Set("n", fmt.Sprint(n))
	values.Set("interval", api.DurationMS(interval).String())
	if opts.ContractID != (types.FileContractID{}) {
		values.Set("fcid", opts.ContractID.String())
	}
	if opts.HostKey != (types.PublicKey{}) {
		values.Set("hostkey", opts.HostKey.String())
	}
	if opts.HostVersion != "" {
		values.Set("hostversion", opts.HostVersion)
	}

	var resp []api.ContractPruneMetric
	if err := c.metric(ctx, api.MetricContractPrune, values, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) WalletMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.WalletMetricsQueryOpts) ([]api.WalletMetric, error) {
	values := url.Values{}
	values.Set("start", api.TimeRFC3339(start).String())
	values.Set("n", fmt.Sprint(n))
	values.Set("interval", api.DurationMS(interval).String())

	var resp []api.WalletMetric
	if err := c.metric(ctx, api.MetricWallet, values, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) RecordContractPruneMetric(ctx context.Context, metrics ...api.ContractPruneMetric) error {
	return c.recordMetric(ctx, api.MetricContractPrune, api.ContractPruneMetricRequestPUT{Metrics: metrics})
}

func (c *Client) PruneMetrics(ctx context.Context, metric string, cutoff time.Time) error {
	values := url.Values{}
	values.Set("cutoff", api.TimeRFC3339(cutoff).String())
	c.c.Custom("DELETE", fmt.Sprintf("/metric/%s?"+values.Encode(), metric), nil, nil)

	u, err := url.Parse(fmt.Sprintf("%s/metric/%s", c.c.BaseURL, metric))
	if err != nil {
		panic(err)
	}
	u.RawQuery = values.Encode()
	req, err := http.NewRequestWithContext(ctx, "DELETE", u.String(), http.NoBody)
	if err != nil {
		panic(err)
	}
	req.SetBasicAuth("", c.c.Password)
	_, _, err = utils.DoRequest(req, nil)
	return err
}

func (c *Client) recordMetric(ctx context.Context, key string, d interface{}) error {
	c.c.Custom("PUT", fmt.Sprintf("/metric/%s", key), (interface{})(nil), nil)

	js, err := json.Marshal(d)
	if err != nil {
		return err
	}

	u, err := url.Parse(fmt.Sprintf("%s/metric/%s", c.c.BaseURL, key))
	if err != nil {
		panic(err)
	}
	req, err := http.NewRequestWithContext(ctx, "PUT", u.String(), bytes.NewReader(js))
	if err != nil {
		panic(err)
	}
	req.SetBasicAuth("", c.c.Password)
	_, _, err = utils.DoRequest(req, nil)
	return err
}

func (c *Client) metric(ctx context.Context, key string, values url.Values, res interface{}) error {
	c.c.Custom("GET", fmt.Sprintf("/metric/%s", key), nil, (*interface{})(nil))

	u, err := url.Parse(fmt.Sprintf("%s/metric/%s", c.c.BaseURL, key))
	if err != nil {
		panic(err)
	}
	u.RawQuery = values.Encode()
	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), http.NoBody)
	if err != nil {
		panic(err)
	}
	req.SetBasicAuth("", c.c.Password)
	_, _, err = utils.DoRequest(req, &res)
	return err
}
