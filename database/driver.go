package database

import "fmt"

type SimpleDB struct {
}

func (db *SimpleDB) Type() string {
	return "SimpleDB"
}

func (db *SimpleDB) Close() error {
	fmt.Printf("Close db done \n")
	return nil
}

// SupportedDrivers returns a slice of strings that represent the database
// drivers that have been registered and are therefore supported.
func SupportedDrivers() []string {
	supportedDBs := make([]string, 0, 0)
	//for _, drv := range drivers {
	//	supportedDBs = append(supportedDBs, drv.DbType)
	//}
	return supportedDBs
}
