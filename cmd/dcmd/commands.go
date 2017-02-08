package main

import (
	"github.com/daos-stack/go-daos/pkg/daos"

	"github.com/pkg/errors"
	cli "gopkg.in/urfave/cli.v1"
)

func daosCommand(cmd func(*cli.Context) error) func(*cli.Context) error {
	return func(c *cli.Context) error {
		err := daos.Init()
		if err != nil {
			return errors.Wrap(err, "daos_init failed")
		}
		defer daos.Fini()

		return cmd(c)
	}
}

var poolFlag = cli.StringFlag{
	Name:   "pool",
	Usage:  "UUID of pool to create container in.",
	EnvVar: "DAOS_POOL",
}

var groupFlag = cli.StringFlag{
	Name:   "group, g",
	Usage:  "Group name of pool servers to use.",
	EnvVar: "DAOS_GROUP",
}

var contFlag = cli.StringFlag{
	Name:  "cont",
	Usage: "UUID for the new container.",
}

var objLoFlag = cli.Uint64Flag{
	Name:  "objl",
	Usage: "Lower component of object ID",
	Value: 1,
}

var objMidFlag = cli.Uint64Flag{
	Name:  "objm",
	Usage: "Middle component of object ID",
}

var objHiFlag = cli.UintFlag{
	Name:  "objh",
	Usage: "High component of object ID",
}

var defaultOclass daos.OClassID = daos.ClassLargeRW

var objClassFlag = cli.GenericFlag{
	Name:  "objc",
	Usage: "Object I/O class",
	Value: &defaultOclass,
}
