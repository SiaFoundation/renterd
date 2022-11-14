package wallet

import (
	"math/big"
	"sort"

	"go.sia.tech/siad/types"
)

// BytesPerInput is the encoded size of a SiacoinInput and corresponding
// TransactionSignature, assuming standard UnlockConditions.
const BytesPerInput = 241

// DistributeFunds is a helper function for distributing the value in a set of
// utxos among the given number of outputs, each containing the requested amount
// of siacoins. It returns the minimal set of utxos that will fund such a
// transaction, along with the resulting fee and change. Inputs with value equal
// to per are ignored. If the inputs are not sufficient to fund all outputs,
// DistributeFunds returns nil.
func DistributeFunds(utxos []SiacoinElement, outputs int, amount, feePerByte types.Currency) ([]SiacoinElement, types.Currency, types.Currency) {
	// desc sort
	sort.Slice(utxos, func(i, j int) bool {
		return utxos[i].Value.Cmp(utxos[j].Value) > 0
	})

	// estimate the fees
	const bytesPerOutput = 64 // approximate; depends on currency size
	outputFees := feePerByte.Mul64(bytesPerOutput).Mul64(uint64(outputs))
	feePerInput := feePerByte.Mul64(BytesPerInput)

	// collect utxos that cover the total amount
	var inputs []SiacoinElement
	want := amount.Mul64(uint64(outputs))
	for _, sce := range utxos {
		if sce.Value.Equals(amount) {
			continue
		}

		inputs = append(inputs, sce)
		fee := feePerInput.Mul64(uint64(len(inputs))).Add(outputFees)
		if SumElements(inputs).Cmp(want.Add(fee)) > 0 {
			break
		}
	}

	// check whether we have enough outputs to cover the total amount
	fee := feePerInput.Mul64(uint64(len(inputs))).Add(outputFees)
	if SumElements(inputs).Cmp(want.Add(fee)) < 0 {
		return nil, types.ZeroCurrency, types.ZeroCurrency
	}

	change := SumElements(inputs).Sub(want.Add(fee))
	return inputs, fee, change
}

// SumElements returns the total value of the supplied elements.
func SumElements(elements []SiacoinElement) types.Currency {
	sum := new(big.Int)
	for _, el := range elements {
		sum.Add(sum, el.Value.Big())
	}
	return types.NewCurrency(sum)
}
