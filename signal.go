package main

func interruptListener() <-chan struct{} {
	c := make(chan struct{})
	return c
}

func interruptRequested(interrupted <-chan struct{}) bool {
	select {
	case <-interrupted:
		return true
	default:
	}
	return false
}
