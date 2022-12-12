package utils

import (
	"flag"
	"fmt"

	"go.sia.tech/siad/types"
)

// TODO: Go1.19 introduces flag.TextVar which can replace this entirely

type (
	CurrencyFlags struct {
		flags []*currencyFlags
	}

	currencyFlags struct {
		price *types.Currency
		value types.Currency
		seen  bool
	}
)

func (p *CurrencyFlags) Parse() {
	for _, flag := range p.flags {
		if !flag.seen {
			*flag.price = flag.value
		}
	}
}

func (p *CurrencyFlags) CurrencyVar(price *types.Currency, name string, value types.Currency, usage string) {
	f := &currencyFlags{
		price: price,
		value: value,
	}
	p.flags = append(p.flags, f)
	flag.Func(name, usage, f.parse)
}

func (p *currencyFlags) parse(currency string) error {
	if p.seen {
		panic("flag already parsed")
	}
	p.seen = true

	priceStr, err := types.ParseCurrency(currency)
	if err != nil {
		return fmt.Errorf("could not parse currency, err: %v", err)
	}

	_, err = fmt.Sscan(priceStr, p.price)
	if err != nil {
		return fmt.Errorf("could not read currency, err: %v", err)
	}
	return nil
}
