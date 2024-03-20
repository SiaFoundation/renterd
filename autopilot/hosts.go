package autopilot

import "lukechampine.com/frand"

type (
	scoredHosts []scoredHost
)

func (hosts scoredHosts) randSelectByScore(n int) (selected []scoredHost) {
	if len(hosts) < n {
		n = len(hosts)
	} else if n < 0 {
		return nil
	}

	used := make(map[int]struct{})
	for len(selected) < n {
		// map indices properly
		imap := make(map[int]int)

		// deep copy
		var candidates []scoredHost
		for i, h := range hosts {
			if _, used := used[i]; !used {
				candidates = append(candidates, h)
				imap[len(candidates)-1] = i
			}
		}

		// normalize
		var total float64
		for _, h := range candidates {
			total += h.score
		}
		for i := range candidates {
			candidates[i].score /= total
		}

		// select
		sI := len(candidates) - 1
		r := frand.Float64()
		var sum float64
		for i, host := range candidates {
			sum += host.score
			if r < sum {
				sI = i
				break
			}
		}

		// update
		used[imap[sI]] = struct{}{}
		selected = append(selected, hosts[imap[sI]])
	}

	return
}
