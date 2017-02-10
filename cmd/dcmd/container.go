package main

import (
	"fmt"

	"github.com/daos-stack/go-daos/pkg/ufd"
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
	uh, err := ufd.Connect(c.String("group"), c.String("pool"))
	if err != nil {
		return errors.Wrap(err, "ufd connect")
	}
	defer uh.Close()

	err = uh.NewContainer(c.String("name"), c.String("uuid"))
	if err != nil {
		return errors.Wrap(err, "new container")
	}
	return err
}

func contInfo(c *cli.Context) error {
	name := c.String("name")
	if name == "" {
		return errors.New("specifiy container name")
	}

	uh, err := ufd.Connect(c.String("group"), c.String("pool"))
	if err != nil {
		return errors.Wrap(err, "ufd connect")
	}
	defer uh.Close()

	id, err := uh.LookupContainer(name)
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
	return errors.New("not implemented")
}
