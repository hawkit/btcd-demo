package main

import (
	"btcd-demo/database"

	"github.com/hawkit/btcd-demo/chaincfg"
)

type server struct {
}

func newServer(listeners []string, db database.DB, chainParams *chaincfg.Params, interrupt <-chan struct{}) (*server, error) {
	return &server{}, nil
}

func (s *server) Stop() error {
	btcdLog.Info("Server stop done")
	return nil
}

func (s *server) Start() error {
	btcdLog.Info("Server start...")
	return nil
}

func (s *server) WaitForShutdown() error {
	btcdLog.Info("Server wait for shutdown")
	return nil
}
