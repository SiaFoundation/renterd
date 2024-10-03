package prometheus

import (
	"bytes"
	"testing"
)

type encType struct {
	Test float64
}

func (e encType) PrometheusMetric() []Metric {
	return []Metric{{
		Name: "test",
		Labels: map[string]any{
			"label": 10,
		},
		Value: e.Test,
	}}
}

func TestEncode(t *testing.T) {
	v := encType{
		Test: 1.5,
	}

	var b bytes.Buffer
	e := NewEncoder(&b)
	if err := e.Append(&v); err != nil {
		t.Fatal(err)
	}

	got := string(b.Bytes())
	const expected = `test{label="10"} 1.5`
	if got != expected {
		t.Fatalf("prometheus marshaling: expected %s, got %s", expected, got)
	}
}

func TestEncodeSlice(t *testing.T) {
	v := []encType{
		{
			Test: 1.5,
		},
		{
			Test: 1.4,
		},
	}

	var b bytes.Buffer
	e := NewEncoder(&b)
	if err := e.Append(Slice(v)); err != nil {
		t.Fatal(err)
	}

	got := string(b.Bytes())
	const expected = `test{label="10"} 1.5
test{label="10"} 1.4`
	if got != expected {
		t.Fatalf("prometheus marshaling: expected %s, got %s", expected, got)
	}
}
