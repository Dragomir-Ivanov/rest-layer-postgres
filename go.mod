module github.com/Dragomir-Ivanov/rest-layer-postgres

go 1.21.1

replace github.com/rs/rest-layer => github.com/rs/rest-layer v0.2.1-0.20210930235801-19f124dac038

require (
	github.com/doug-martin/goqu/v9 v9.19.0
	github.com/huandu/go-clone/generic v1.7.2
	github.com/lib/pq v1.10.9
	github.com/pkg/errors v0.9.1
	github.com/rotisserie/eris v0.5.4
	github.com/rs/rest-layer v0.2.0
	github.com/rs/xid v1.5.0
	github.com/sanity-io/litter v1.5.5
)

require (
	github.com/evanphx/json-patch v4.1.0+incompatible // indirect
	github.com/huandu/go-clone v1.6.0 // indirect
	golang.org/x/crypto v0.0.0-20190605123033-f99c8df09eb5 // indirect
)
