package main

import (
	"btcd-demo/chaincfg"
	"btcd-demo/wire"
)

var activeNetParams = &mainNetParams

type params struct {
	*chaincfg.Params
	rpcPort string
}

var mainNetParams = params{
	Params: &chaincfg.MainNetParams,
	rpcPort: "8334",
}

var regressionNetParams = params{
	rpcPort: "18334",
}

var testNet3Params = params{
	rpcPort: "18334",
}

var simNetParams = params{
	rpcPort: "18556",
}

func netName(chainParams *params) string {
	switch chainParams.Net {
	case wire.TestNet3:
		return "testnet"
	default:
		return chainParams.Name
	}
}
