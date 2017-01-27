package main

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/rread/go-daos/pkg/daos"
	cli "gopkg.in/urfave/cli.v1"
)

func init() {
	poolCommands := cli.Command{
		Name:  "cont",
		Usage: "DAOS Container related commands",
		Subcommands: []cli.Command{
			{
				Name:      "create",
				Usage:     "Create a container",
				ArgsUsage: "[uuid [uuid...]]",
				Action:    contCreateCommand,
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:  "pool",
						Usage: "UUID of pool to create container in.",
					},
					cli.StringFlag{
						Name:  "group, g",
						Usage: "Group name of pool servers to use.",
					},
					cli.StringFlag{
						Name:  "uuid",
						Usage: "UUID for the new container.",
					},
				},
			},
			{
				Name:      "info",
				Usage:     "Display info about container",
				ArgsUsage: "[uuid [uuid...]]",
				Action:    contInfoCommand,
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:  "pool",
						Usage: "UUID of pool to create container in.",
					},
					cli.StringFlag{
						Name:  "group, g",
						Usage: "Group name of pool servers to use.",
					},
				},
			},
			{
				Name:      "destroy",
				Usage:     "Destroy containers",
				ArgsUsage: "[uuid [uuid...]]",
				Action:    contDestroyCommand,
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:  "pool",
						Usage: "UUID of pool to create container in.",
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

func contCreateCommand(c *cli.Context) error {
	err := daos.Init()
	if err != nil {
		return errors.Wrap(err, "daos_init failed")
	}
	defer daos.Fini()

	group := c.String("group")

	poh, err := daos.PoolConnect(c.String("pool"), group, daos.PoolConnectRW)
	if err != nil {
		return errors.Wrap(err, "connect failed")
	}
	defer poh.Disconnect()

	err = poh.NewContainer(c.String("uuid"))
	return err
}

func contInfoCommand(c *cli.Context) error {
	err := daos.Init()
	if err != nil {
		return errors.Wrap(err, "daos_init failed")
	}
	defer daos.Fini()

	group := c.String("group")

	poh, err := daos.PoolConnect(c.String("pool"), group, daos.PoolConnectRW)
	if err != nil {
		return errors.Wrap(err, "connect failed")
	}
	defer poh.Disconnect()

	for _, cont := range c.Args() {
		coh, err := poh.Open(cont, daos.ContOpenRO)
		if err != nil {
			return errors.Wrap(err, "container open  failed")
		}

		defer coh.Close()

		info, err := coh.Info()
		if err != nil {
			return errors.Wrap(err, "container info failed")
		}

		fmt.Printf("Pool:     %s\n", info.UUID())
		fmt.Printf("Mode:     0%o\n", info.EpochState())
		fmt.Printf("Snapshots:  %v\n", info.Snapshots())

	}

	return nil
}
func contDestroyCommand(c *cli.Context) error {
	return nil
}
