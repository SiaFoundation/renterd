package client

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

func (c *Client) ContractSetMetrics(ctx context.Context, opts api.ContractSetMetricsQueryOpts) ([]api.ContractSetMetric, error) {
	values := url.Values{}
	if opts.After != (time.Time{}) {
		values.Set("after", api.TimeRFC3339(opts.After).String())
	}
	if opts.Before != (time.Time{}) {
		values.Set("before", api.TimeRFC3339(opts.Before).String())
	}
	if opts.Name != "" {
		values.Set("name", opts.Name)
	}
	if opts.Offset != 0 {
		values.Set("offset", fmt.Sprint(opts.Offset))
	}
	if opts.Limit != 0 {
		values.Set("limit", fmt.Sprint(opts.Limit))
	}
	var resp []api.ContractSetMetric
	err := c.c.WithContext(ctx).GET(fmt.Sprintf("/metrics/%s?"+values.Encode(), api.MetricContractSet), &resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) ContractSetChurnMetrics(ctx context.Context, opts api.ContractSetChurnMetricsQueryOpts) ([]api.ContractSetChurnMetric, error) {
	values := url.Values{}
	if opts.After != (time.Time{}) {
		values.Set("after", api.TimeRFC3339(opts.After).String())
	}
	if opts.Before != (time.Time{}) {
		values.Set("before", api.TimeRFC3339(opts.Before).String())
	}
	if opts.Name != "" {
		values.Set("name", opts.Name)
	}
	if opts.Direction != "" {
		values.Set("direction", string(opts.Direction))
	}
	if opts.Reason != "" {
		values.Set("reason", string(opts.Reason))
	}
	if opts.Offset != 0 {
		values.Set("offset", fmt.Sprint(opts.Offset))
	}
	if opts.Limit != 0 {
		values.Set("limit", fmt.Sprint(opts.Limit))
	}
	var resp []api.ContractSetChurnMetric
	err := c.c.WithContext(ctx).GET(fmt.Sprintf("/metrics/%s?"+values.Encode(), api.MetricContractSetChurn), &resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) RecordContractSetChurnMetric(ctx context.Context, metrics ...api.ContractSetChurnMetric) error {
	return c.c.WithContext(ctx).PUT(fmt.Sprintf("/metrics/%s", api.MetricContractSetChurn), api.ContractSetChurnMetricRequestPUT{
		Metrics: metrics,
	})
}

func (c *Client) ContractMetrics(ctx context.Context, opts api.ContractMetricsQueryOpts) ([]api.ContractMetric, error) {
	values := url.Values{}
	if opts.After != (time.Time{}) {
		values.Set("after", api.TimeRFC3339(opts.After).String())
	}
	if opts.Before != (time.Time{}) {
		values.Set("before", api.TimeRFC3339(opts.Before).String())
	}
	if opts.FCID != (types.FileContractID{}) {
		values.Set("fcid", opts.FCID.String())
	}
	if opts.Host != (types.PublicKey{}) {
		values.Set("host", opts.Host.String())
	}
	if opts.Offset != 0 {
		values.Set("offset", fmt.Sprint(opts.Offset))
	}
	if opts.Limit != 0 {
		values.Set("limit", fmt.Sprint(opts.Limit))
	}
	var resp []api.ContractMetric
	err := c.c.WithContext(ctx).GET(fmt.Sprintf("/metrics/%s?"+values.Encode(), api.MetricContract), &resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
