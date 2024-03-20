package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
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
		values.Set("contractID", opts.ContractID.String())
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

func (c *Client) ContractPruneMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractPruneMetricsQueryOpts) ([]api.ContractPruneMetric, error) {
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
	if opts.HostVersion != "" {
		values.Set("hostVersion", opts.HostVersion)
	}

	var resp []api.ContractPruneMetric
	if err := c.metric(ctx, api.MetricContractPrune, values, &resp); err != nil {
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

func (c *Client) RecordContractSetChurnMetric(ctx context.Context, metrics ...api.ContractSetChurnMetric) error {
	return c.recordMetric(ctx, api.MetricContractSetChurn, api.ContractSetChurnMetricRequestPUT{Metrics: metrics})
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
	req.SetBasicAuth("", c.c.WithContext(ctx).Password)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		err, _ := io.ReadAll(resp.Body)
		return errors.New(string(err))
	}
	return nil
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
	req.SetBasicAuth("", c.c.WithContext(ctx).Password)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer io.Copy(io.Discard, resp.Body)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		err, _ := io.ReadAll(resp.Body)
		return errors.New(string(err))
	}
	return nil
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
	req.SetBasicAuth("", c.c.WithContext(ctx).Password)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer io.Copy(io.Discard, resp.Body)
	defer resp.Body.Close()

	if resp.StatusCode != 200 && resp.StatusCode != 206 {
		err, _ := io.ReadAll(resp.Body)
		return errors.New(string(err))
	}
	return json.NewDecoder(resp.Body).Decode(&res)
}
