# Go language bindings for the DAOS API

Work in progress.

Only basic Pool commands have been created. 

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

