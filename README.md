# Go language bindings for the DAOS API

Work in progress.

This is an Go interface for
[DAOS](https://github.com/daos-stack/daos) which is also a work in progress. Building this requires a
local DAOS build and DAOS server running, so start there first.

Only basic Pool commands have been implemented so far. 

## How to Build

This is a [Go](https://golang.orghttps://golang.org/doc/install)
project, so a Go development tools are naturally required. We
recommend the most current Go release availble (curently 1.7.4), but
1.6.x (which are avaialble from EPEL for EL 7) should work.

Setup environment and build. This assumes $daospath is set as it 
was in the DAOS [README](https://github.com/daos-stack/daos/blob/master/README.md)


	export GOPATH=$HOME/go  
	export LD_LIBRARY_PATH=${daospath}/install/lib
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
command line, though it will take care of running it for you so you
can use command lines like this (if default values work for you):

### Command help

	 NAME:
	    dcmd - DAOS-related actions

	 USAGE:
	    dcmd [global options] commadn [command options] [arguments...]

	    This command must be run using the customized orterun from DAOS project.

	 VERSION:
	    0.1

	 AUTHOR:
	    Intel® HPDD <HPDD-enterprise-lustre@intel.com>

	 COMMANDS:
	      create   Create a pool
	      info     Display info about pool
	      destroy  Destroy pools
	      help, h  Shows a list of commands or help for one command

	 GLOBAL OPTIONS:
	    --np value      Number of processes to start (default: 1)
	    --uri value     path to URI file (default: "/tmp/daos-uri")
	    --runner value  path to mpi runner to use (default: "orterun")
	    --help, -h      show help
	    --version, -v   print the version


### Example

	[vagrant@localhost go-daos]$ dcmd create 
	aec497ca-8cf5-4d65-ab41-998ad03f5cb8
	[vagrant@localhost go-daos]$ dcmd info aec497ca-8cf5-4d65-ab41-998ad03f5cb8
	Pool:     aec497ca-8cf5-4d65-ab41-998ad03f5cb8
	Mode:     0644
	Targets:  1
	Disabled: 0
	[vagrant@localhost go-daos]$ dcmd destroy aec497ca-8cf5-4d65-ab41-998ad03f5cb8

