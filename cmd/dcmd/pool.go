package main

import (
	"fmt"

	"github.com/daos-stack/go-daos/pkg/daos"

	humanize "github.com/dustin/go-humanize"
	"github.com/pkg/errors"
	cli "gopkg.in/urfave/cli.v1"
)

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
					groupFlag,
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
				Name:      "init",
				Usage:     "Initialize metadata fro pool",
				ArgsUsage: "",
				Action:    daosCommand(poolInit),
				Flags: []cli.Flag{
					groupFlag,
					poolFlag,
				},
			},
			{
				Name:      "info",
				Usage:     "Display info about pool",
				ArgsUsage: "",
				Action:    daosCommand(poolInfo),
				Flags: []cli.Flag{
					groupFlag,
					poolFlag,
				},
			},
			{
				Name:      "destroy",
				Usage:     "Destroy pools",
				ArgsUsage: "[uuid [uuid...]]",
				Action:    daosCommand(poolDestroy),
				Flags: []cli.Flag{
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

func poolCreate(c *cli.Context) error {

	group := c.String("group")
	uid, err := lookupUser(c.String("uid"))
	if err != nil {
		return errors.Wrapf(err, "invalid uid: %s", c.String("uid"))
	}

	gid, err := lookupGroup(c.String("gid"))
	if err != nil {
		return errors.Wrapf(err, "invalid gid: %s", c.String("gid"))
	}
	mode := uint32(c.Uint("mode"))

	n, err := humanize.ParseBytes(c.String("size"))
	if err != nil {
		return errors.Wrap(err, "invalid size")
	}
	size := int64(n)

	uuid, err := daos.PoolCreate(mode, uid, gid, group, size)
	if err != nil {
		return errors.Wrap(err, "unable to create pool")
	}

	poh, err := openPool(c, daos.PoolConnectRW)
	if err != nil {
		return errors.Wrap(err, "connect failed")
	}
	defer poh.Disconnect()

	err = PoolMetaInit(poh, c.String("pool"))
	if err != nil {
		return errors.Wrap(err, "pool meta init")
	}

	fmt.Printf("%v\n", uuid)
	return nil
}

func poolInit(c *cli.Context) error {
	poh, err := openPool(c, daos.PoolConnectRW)
	if err != nil {
		return errors.Wrap(err, "connect failed")
	}
	defer poh.Disconnect()

	err = PoolMetaInit(poh, c.String("pool"))
	if err != nil {
		return errors.Wrap(err, "pool meta init")
	}
	return nil
}

func poolInfo(c *cli.Context) error {
	poh, err := openPool(c, daos.PoolConnectRW)
	if err != nil {
		return errors.Wrap(err, "connect failed")
	}
	defer poh.Disconnect()

	info, err := poh.Info()
	if err != nil {
		return errors.Wrap(err, "query failed")
	}
	pm, err := OpenMeta(poh, c.String("pool"), false)
	defer pm.Close()
	creator, err := pm.Creator()
	if err != nil {
		return err
	}
	created, err := pm.Created()
	if err != nil {
		return err
	}
	conttab, err := pm.ContTable()
	if err != nil {
		return err
	}
	fmt.Printf("Pool:       %s\n", info.UUID())
	fmt.Printf("Mode:       0%o\n", info.Mode())
	fmt.Printf("Targets:    %d\n", info.NumTargets())
	fmt.Printf("Disabled:   %d\n", info.NumDisabled())
	fmt.Printf("Creator:    %s\n", creator)
	fmt.Printf("Created:    %s\n", created)
	fmt.Printf("ContTable:  %s\n", conttab)

	return nil
}

func poolDestroy(c *cli.Context) error {
	group := c.String("group")
	var force int
	if c.Bool("force") {
		force = 1
	}

	for _, pool := range c.Args() {
		err := daos.PoolDestroy(pool, group, force)
		if err != nil {
			return errors.Wrap(err, "destroy failed")
		}
	}
	return nil
}
