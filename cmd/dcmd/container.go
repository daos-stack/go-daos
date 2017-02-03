package main

import (
	"fmt"

	"github.com/daos-stack/go-daos/pkg/daos"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
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
						Name:  "name",
						Usage: "Name for the new container.",
					},
					cli.StringFlag{
						Name:  "uuid",
						Usage: "UUID for the new container. (optional)",
					},
				},
			},
			{
				Name:      "info",
				Usage:     "Display info about container",
				ArgsUsage: "",
				Action:    daosCommand(contInfo),
				Flags: []cli.Flag{
					poolFlag,
					groupFlag,
					cli.StringFlag{
						Name:  "name",
						Usage: "Name for the new container.",
					},
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

	id := c.String("uuid")
	if id == "" {
		id = uuid.New()
	}
	err = poh.NewContainer(id)
	if err != nil {
		return errors.Wrap(err, "new container")
	}
	name := c.String("name")
	if name != "" {
		pm, err := OpenMeta(poh, c.String("pool"), true)
		if err != nil {
			return errors.Wrap(err, "open meta")
		}
		defer pm.Close()
		err = pm.AddContainer(name, id)
	}

	return err
}

func contInfo(c *cli.Context) error {
	poh, err := openPool(c, daos.PoolConnectRW)
	if err != nil {
		return errors.Wrap(err, "open pool")
	}
	defer poh.Disconnect()

	name := c.String("name")
	if name == "" {
		return errors.New("specifiy container name")
	}

	pm, err := OpenMeta(poh, c.String("pool"), false)
	if err != nil {
		return errors.Wrap(err, "open meta")
	}
	defer pm.Close()
	id, err := pm.LookupContainer(name)
	if err != nil {
		return errors.Wrap(err, "open mata")
	}

	fmt.Printf("UUID:      %s\n", id)

	/*
		         Not implemented yet

				coh, err := poh.Open(cont, daos.ContOpenRO)
				if err != nil {
					return errors.Wrap(err, "container open  failed")
				}

				defer coh.Close()

				info, err := coh.Info()
				if err != nil {
					return errors.Wrap(err, "container info failed")
				}

				fmt.Printf("Container:     %s\n", info.UUID())
				fmt.Printf("Mode:     0%o\n", info.EpochState())
				fmt.Printf("Snapshots:  %v\n", info.Snapshots())

	*/

	return nil
}

func contDestroy(c *cli.Context) error {
	return nil
}
