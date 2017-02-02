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
