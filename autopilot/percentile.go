// Copyright (c) 2014-2020 Montana Flynn (https://montanaflynn.com)
package autopilot

import (
	"errors"
	"math"
	"sort"
)

var (
	errEmptyInput  = errors.New("input must not be empty")
	errOutOfBounds = errors.New("input is outside of range")
)

func percentile(input []float64, percent float64) (float64, error) {
	// validate input
	if len(input) == 0 {
		return math.NaN(), errEmptyInput
	}
	if percent <= 0 || percent > 100 {
		return math.NaN(), errOutOfBounds
	}

	// return early if we only have one
	if len(input) == 1 {
		return input[0], nil
	}

	// deep copy the input and sort
	input = append([]float64{}, input...)
	sort.Float64s(input)

	// multiply percent by length of input
	index := (percent / 100) * float64(len(input))

	// check if the index is a whole number, if so return that input
	if index == float64(int64(index)) {
		i := int(index)
		return input[i-1], nil
	}

	// if the index is greater than one, return the average of the index and the value prior
	if index > 1 {
		i := int(index)
		avg := (input[i-1] + input[i]) / 2
		return avg, nil
	}

	return math.NaN(), errOutOfBounds
}
