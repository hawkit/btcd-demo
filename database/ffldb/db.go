package ffldb

import (
	"btcd-demo/database"
	"btcd-demo/wire"
)

// openDB opens the database at the provided path.  database.ErrDbDoesNotExist
// is returned if the database doesn't exist and the create flag is not set.
func openDB(dbPath string, network wire.BitcoinNet, create bool) (database.DB, error) {
	return nil, nil // todo opendb
}
