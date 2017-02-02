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
				Action:    daosCommand(contCreate),
				Flags: []cli.Flag{
					poolFlag,
					groupFlag,
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
				Action:    daosCommand(contInfo),
				Flags: []cli.Flag{
					poolFlag,
					groupFlag,
				},
			},
			{
				Name:      "destroy",
				Usage:     "Destroy containers",
				ArgsUsage: "[uuid [uuid...]]",
				Action:    daosCommand(contDestroy),
				Flags: []cli.Flag{
					poolFlag,
					groupFlag,
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

func contCreate(c *cli.Context) error {
	poh, err := openPool(c, daos.PoolConnectRW)
	if err != nil {
		return errors.Wrap(err, "open pool")
	}
	defer poh.Disconnect()

	err = poh.NewContainer(c.String("uuid"))
	return err
}

func contInfo(c *cli.Context) error {
	poh, err := openPool(c, daos.PoolConnectRW)
	if err != nil {
		return errors.Wrap(err, "open pool")
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

func contDestroy(c *cli.Context) error {
	return nil
}
