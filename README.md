# DISCONTINUATION OF PROJECT #
This project will no longer be maintained by Intel.
Intel has ceased development and contributions including, but not limited to, maintenance, bug fixes, new releases, or updates, to this project.
Intel no longer accepts patches to this project.
[![GoDoc](https://godoc.org/github.com/daos-stack/go-daos/pkg/daos?status.svg)](https://godoc.org/github.com/daos-stack/go-daos/pkg/daos)

# Go language bindings for the DAOS API

This is a Go interface for
[DAOS](https://github.com/daos-stack/daos) which is also a work in progress. Building this requires a
local DAOS build and DAOS server running, so start there first. 

## Current Status
  * Covers most of the DAOS API, even the unimplemented bits (e.g. punch, etc.)
  * [daosfs](https://github.com/daos-stack/go-daos/tree/master/pkg/daosfs) implements basic POSIX filesystem semantics
  * The [daos-fuse](https://github.com/daos-stack/go-daos/tree/master/cmd/daos-fuse) utility exposes a daosfs container via FUSE mount
  * [libdaosfs](https://github.com/daos-stack/go-daos/tree/master/pkg/daosfs/libdaosfs) exports a C-compatible API for use by things like [nfs-ganesha](https://github.com/mjmac/nfs-ganesha/tree/daosfs/src/FSAL/FSAL_DAOSFS)

## How to Build

This is a [Go](https://golang.orghttps://golang.org/doc/install)
project, so a Go development tools are naturally required. We
recommend the most current Go release available. As of April 2017, the project has been built and tested with Go 1.8.

Setup environment and build. This assumes $daospath is set as it 
was in the DAOS [README](https://github.com/daos-stack/daos/blob/master/README.md)


	export GOPATH=$HOME/go  
	export LD_LIBRARY_PATH=${daospath}/install/lib
	export PATH=${daospath}/install/bin:$PATH
	export CGO_CPPFLAGS="-I${daospath}/install/include -I${daospath}/src/include"
	export CGO_LDFLAGS=-L${daospath}/install/lib 
	go get -u github.com/daos-stack/go-daos/cmd/dcmd

The last command will clone this repo and and its dependencies, build,
and install in the Go workspace. The `dcmd` binary will be in
`$GOPATH/bin/dcmd` and the source for the package will be
`$GOPATH/src/github.com/daos-stack/go-daos`. You can build again using:

	go install github.com/daos-stack/go-daos/cmd/dcmd

## dcmd utility

Also includes a simple CLI tool for interacting with DAOS. This tool
requires the orterun command that was built for DAOS to be on the
command line, and will handle running itself with orterun if is run
directly. When dcmd calls orterun itself, the default uri file is
/tmp/daos-uri or it can be customized with --uri option. See command
help for more options and command details.

Example comands to create an container and manipulate objects:


	cont=$(uuidgen)
	export DAOS_GROUP="" # if needed
	export DAOS_POOL=$(dcmd pool create)
	dcmd cont create --name mydb
	dcmd object update --cont mydb --dkey "a" --akey "1" --value "foo"
	dcmd object update --cont mydb --dkey "a" --akey "2" --file /path/to/file
	dcmd object fetch --cont mydb --dkey "a" --akey "1"
	dcmd object fetch --cont mydb --dkey "a" --akey "2" --binary | sha1sum

By default, `fetch` will display binary data in "hexdump -C"
format. Use the --binary option to read the raw data.

N.B. `dcmd` does not include a trailing NUL in the string keys, and
assumes existing keys do not have them, either.
