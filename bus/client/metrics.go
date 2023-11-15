package client

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

func (c *Client) ContractMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractMetricsQueryOpts) ([]api.ContractMetric, error) {
	values := url.Values{}
	values.Set("start", api.TimeRFC3339(start).String())
	values.Set("n", fmt.Sprint(n))
	values.Set("interval", api.DurationMS(interval).String())
	if opts.ContractID != (types.FileContractID{}) {
		values.Set("fcid", opts.ContractID.String())
	}
	if opts.HostKey != (types.PublicKey{}) {
		values.Set("hostKey", opts.HostKey.String())
	}

	var resp []api.ContractMetric
	if err := c.metric(ctx, api.MetricContract, values, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) ContractSetChurnMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractSetChurnMetricsQueryOpts) ([]api.ContractSetChurnMetric, error) {
	values := url.Values{}
	values.Set("start", api.TimeRFC3339(start).String())
	values.Set("n", fmt.Sprint(n))
	values.Set("interval", api.DurationMS(interval).String())
	if opts.Name != "" {
		values.Set("name", opts.Name)
	}
	if opts.Direction != "" {
		values.Set("direction", string(opts.Direction))
	}
	if opts.Reason != "" {
		values.Set("reason", string(opts.Reason))
	}

	var resp []api.ContractSetChurnMetric
	if err := c.metric(ctx, api.MetricContractSetChurn, values, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) ContractSetMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractSetMetricsQueryOpts) ([]api.ContractSetMetric, error) {
	values := url.Values{}
	values.Set("start", api.TimeRFC3339(start).String())
	values.Set("n", fmt.Sprint(n))
	values.Set("interval", api.DurationMS(interval).String())
	if opts.Name != "" {
		values.Set("name", opts.Name)
	}

	var resp []api.ContractSetMetric
	if err := c.metric(ctx, api.MetricContractSet, values, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) RecordContractSetChurnMetric(ctx context.Context, metrics ...api.ContractSetChurnMetric) error {
	return c.c.WithContext(ctx).PUT(fmt.Sprintf("/metric/%s", api.MetricContractSetChurn), api.ContractSetChurnMetricRequestPUT{
		Metrics: metrics,
	})
}

func (c *Client) metric(ctx context.Context, key string, values url.Values, resp interface{}) error {
	return c.c.WithContext(ctx).GET(fmt.Sprintf("/metric/%s?"+values.Encode(), key), &resp)
}
