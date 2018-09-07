package database

type DB interface {
	Type() string

	Close() error
}
