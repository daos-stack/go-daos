package main

import (
	"github.com/rread/go-daos/pkg/daos"

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

func init() {
	poolCommands := cli.Command{
		Name:  "pool",
		Usage: "DAOS Pool related commands",
		Subcommands: []cli.Command{
			{
				Name:      "create",
				Usage:     "Create a pool",
				ArgsUsage: "",
				Action:    daosCommand(poolCreate),
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:  "group, g",
						Usage: "Group name of pool servers to use.",
					},
					cli.StringFlag{
						Name:  "uid",
						Usage: "Owner uid",
					},
					cli.StringFlag{
						Name:  "gid",
						Usage: "Owner gid",
					},
					cli.UintFlag{
						Name:  "mode",
						Value: 0644,
						Usage: "File mode",
					},
					cli.StringFlag{
						Name:  "size",
						Value: "256MiB",
						Usage: "Size of container in bytes",
					},
				},
			},
			{
				Name:      "info",
				Usage:     "Display info about pool",
				ArgsUsage: "[uuid [uuid...]]",
				Action:    daosCommand(poolInfo),
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:  "group, g",
						Usage: "Group name of pool servers to use.",
					},
				},
			},
			{
				Name:      "destroy",
				Usage:     "Destroy pools",
				ArgsUsage: "[uuid [uuid...]]",
				Action:    daosCommand(poolDestroy),
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:  "group, g",
						Usage: "Group name of pool servers to use.",
					},
					cli.BoolFlag{
						Name:  "force, f",
						Usage: "Foce destroy",
					},
				},
			},
		},
	}
	commands = append(commands, poolCommands)
}
