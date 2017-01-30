# Go language bindings for the DAOS API

Work in progress.

This is an Go interface for
[DAOS](https://github.com/daos-stack/daos) which is also a work in progress. Building this requires a
local DAOS build and DAOS server running, so start there first.

Only basic Pool commands have been implemented so far. 

## How to Build

This is a [Go](https://golang.orghttps://golang.org/doc/install)
project, so a Go development tools are naturally required. We
recommend the most current Go release availble, curently 1.7.4.

Setup environment and build. This assumes $daospath is set as it 
was in the DAOS [README](https://github.com/daos-stack/daos/blob/master/README.md)


	export GOPATH=$HOME/go  
	export LD_LIBRARY_PATH=${daospath}/install/lib
	export PATH=${daospath}/install/bin:$PATH
	export CGO_CPPFLAGS="-I${daospath}/install/include -I${daospath}/src/include"
	export CGO_LDFLAGS=-L${daospath}/install/lib 
	go get -u github.com/rread/go-daos/cmd/dcmd

The last command will clone this repo and and its dependencies, build,
and install in the Go workspace. The `dcmd` binary will be in
`$GOPATH/bin/dcmd` and the source for the package will be
`$GOPATH/src/github.com/rread/go-daos`. You can build again using:

	go install github.com/rread/go-daos/cmd/dcmd

## dcmd utility

Also includes a simple CLI tool for interacting with DAOS. This tool
requires the orterun command that was built for DAOS to be on the
command line, and will handle running itself with orterun if is run
directly. When dcmd calls orterun itself, the default uri file is
/tmp/daos-uri or it can be customized with --uri option. See command
help for more options and command details.

This will create a single object and attempt to write and then read a
value:

	cont=$(uuidgen)
	pool=$(dcmd pool create)
	dcmd cont create --pool $pool --uuid $cont
	dcmd object hello --pool $pool --cont $cont --value "world"


