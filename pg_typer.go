package pgsql

type PostgresTyper interface {
	PostgresType() string
}
