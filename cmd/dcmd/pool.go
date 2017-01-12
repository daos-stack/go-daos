package main

import (
	"fmt"

	"github.com/rread/go-daos/pkg/daos"

	humanize "github.com/dustin/go-humanize"
	"github.com/pkg/errors"
	cli "gopkg.in/urfave/cli.v1"
)

func poolCreateCommand(c *cli.Context) error {
	err := daos.Init()
	if err != nil {
		return errors.Wrap(err, "daos_init failed")
	}
	defer daos.Fini()

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
	fmt.Printf("%v\n", uuid)
	return nil
}

func poolInfoCommand(c *cli.Context) error {
	err := daos.Init()
	if err != nil {
		return errors.Wrap(err, "daos_init failed")
	}
	defer daos.Fini()

	group := c.String("group")

	for _, pool := range c.Args() {
		poh, err := daos.PoolConnect(pool, group, 0)
		if err != nil {
			return errors.Wrap(err, "connect failed")
		}

		info, err := poh.Info()
		if err != nil {
			return errors.Wrap(err, "query failed")
		}

		fmt.Printf("Pool:     %s\n", info.UUID())
		fmt.Printf("Mode:     0%o\n", info.Mode())
		fmt.Printf("Targets:  %d\n", info.NumTargets())
		fmt.Printf("Disabled: %d\n", info.NumDisabled())

		err = poh.Disconnect()
		if err != nil {
			return errors.Wrap(err, "disconnect failed")
		}
	}

	return nil
}

func poolDestroyCommand(c *cli.Context) error {
	err := daos.Init()
	if err != nil {
		return errors.Wrap(err, "daos_init failed")
	}
	defer daos.Fini()

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
