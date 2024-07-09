package worker

func shouldRecordPriceTable(err error) bool {
	// List of errors that are considered 'successful' failures. Meaning that
	// the host was reachable but we were unable to obtain a price table due to
	// reasons out of the host's control.
	if isInsufficientFunds(err) {
		return false
	}
	if isBalanceInsufficient(err) {
		return false
	}
	return true
}
