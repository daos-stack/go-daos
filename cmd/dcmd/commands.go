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

var (
	verboseFlag = cli.BoolFlag{
		Name:  "verbose, v",
		Usage: "Print chatty messages ",
	}

	poolFlag = cli.StringFlag{
		Name:   "pool",
		Usage:  "UUID of pool to create container in.",
		EnvVar: "DAOS_POOL",
	}

	groupFlag = cli.StringFlag{
		Name:   "group, g",
		Usage:  "Group name of pool servers to use.",
		EnvVar: "DAOS_GROUP",
	}

	contFlag = cli.StringFlag{
		Name:  "cont",
		Usage: "UUID for the new container.",
	}
	hexFlag = cli.BoolFlag{
		Name:  "hex, x",
		Usage: "Print data as hex value (useful for binary data.",
	}

	defaultObject daos.ObjectID
	objFlag       = cli.GenericFlag{
		Name:  "obj",
		Usage: "ObjectID",
		Value: &defaultObject,
	}

	objDkeyFlag = cli.StringFlag{
		Name:  "dkey",
		Usage: "The dkey to set",
	}
	objDkeybFlag = cli.StringFlag{
		Name:  "dkeyb",
		Usage: "Hex encoded dkey (for binary keys)",
	}
	objAkeyFlag = cli.StringFlag{
		Name:  "akey",
		Usage: "The akey to set",
	}
	objAkeybFlag = cli.StringFlag{
		Name:  "akeyb",
		Usage: "Hex encoded akey (for binary keys)",
	}
)
