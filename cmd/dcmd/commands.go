package main

import cli "gopkg.in/urfave/cli.v1"

func init() {
	poolCommands := cli.Command{
		Name:  "pool",
		Usage: "DAOS Pool related commands",
		Subcommands: []cli.Command{
			{
				Name:      "create",
				Usage:     "Create a pool",
				ArgsUsage: "",
				Action:    poolCreateCommand,
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
				Action:    poolInfoCommand,
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
				Action:    poolDestroyCommand,
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
