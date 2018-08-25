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