package contractor

import (
	"slices"

	"lukechampine.com/frand"
)

type (
	scoredHosts []scoredHost
)

func (hosts scoredHosts) randSelectByScore(n int) (selected []scoredHost) {
	if len(hosts) < n {
		n = len(hosts)
	} else if n < 0 {
		return nil
	}

	// deep copy to avoid modifying original slice
	candidates := slices.Clone(hosts)

	// compute initial total score
	var total float64
	for _, h := range candidates {
		total += h.score
	}

	selected = make([]scoredHost, 0, n)
	remaining := len(candidates)

	for len(selected) < n {
		// select based on weighted random
		r := frand.Float64() * total
		var sum float64
		sI := remaining - 1 // default to last if rounding issues
		for i := 0; i < remaining; i++ {
			sum += candidates[i].score
			if r < sum {
				sI = i
				break
			}
		}

		// add to selected
		selected = append(selected, candidates[sI])

		// update total score
		total -= candidates[sI].score

		// swap selected with last active element and shrink
		remaining--
		candidates[sI] = candidates[remaining]
	}

	return
}
