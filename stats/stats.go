package stats

import (
	"math"
	"sync"
	"time"

	"github.com/montanaflynn/stats"
)

const (
	statsDecayHalfTime  = 10 * time.Minute
	statsDecayThreshold = 5 * time.Minute
)

type (
	DataPoints struct {
		stats.Float64Data
		halfLife time.Duration
		size     int

		mu            sync.Mutex
		cnt           int
		p90           float64
		lastDatapoint time.Time
		lastDecay     time.Time
	}

	Float64Data = stats.Float64Data
)

func Default() *DataPoints {
	return New(statsDecayHalfTime)
}

func NoDecay() *DataPoints {
	return New(0)
}

func New(halfLife time.Duration) *DataPoints {
	return &DataPoints{
		size:        20,
		Float64Data: make([]float64, 0),
		halfLife:    halfLife,
		lastDecay:   time.Now(),
	}
}

func (a *DataPoints) Average() float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	avg, err := a.Mean()
	if err != nil {
		avg = 0
	}
	return avg
}

func (a *DataPoints) P90() float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.p90
}

func (a *DataPoints) Recompute() {
	a.mu.Lock()
	defer a.mu.Unlock()

	// apply decay
	a.tryDecay()

	// recalculate the p90
	p90, err := a.Percentile(90)
	if err != nil {
		p90 = 0
	}
	a.p90 = p90
}

func (a *DataPoints) Track(p float64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.cnt < a.size {
		a.Float64Data = append(a.Float64Data, p)
	} else {
		a.Float64Data[a.cnt%a.size] = p
	}

	a.lastDatapoint = time.Now()
	a.cnt++
}

func (a *DataPoints) tryDecay() {
	// return if decay is disabled
	if a.halfLife == 0 {
		return
	}

	// return if decay is not needed
	if time.Since(a.lastDatapoint) < statsDecayThreshold {
		return
	}

	// return if decay is not due
	decayFreq := a.halfLife / 5
	timePassed := time.Since(a.lastDecay)
	if timePassed < decayFreq {
		return
	}

	// calculate decay and apply it
	strength := float64(timePassed) / float64(a.halfLife)
	decay := math.Floor(math.Pow(0.5, strength)*100) / 100 // round down to 2 decimals
	for i := range a.Float64Data {
		a.Float64Data[i] *= decay
	}

	// update the last decay time
	a.lastDecay = time.Now()
}
