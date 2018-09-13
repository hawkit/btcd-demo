package database

import "github.com/hawkit/btclog-demo"

// log is a logger that is initialized with no output filters. This
// means the package will not perform any logging by default until the
// caller request it
var log btclog.Logger

// The default amount of logging is none
func init() {
	DisableLog()
}

func DisableLog() {
	log = btclog.Disabled
}

func UseLogger(logger btclog.Logger) {
	log = logger

	for _, drv := range drivers {
		if drv.UseLogger != nil {
			drv.UseLogger(logger)
		}
	}
}
