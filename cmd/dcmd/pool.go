package main

import (
	"encoding/json"
	"fmt"
	"os/user"
	"time"

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

	fmt.Printf("%v\n", uuid)
	return nil
}

const (
	PoolMetaDkey = "metadata"
	CreatorAkey  = "creator"
	CreatedAkey  = "created"
)

func poolInit(c *cli.Context) error {
	poh, err := openPool(c, daos.PoolConnectRW)
	if err != nil {
		return errors.Wrap(err, "unable to connect to pool")
	}
	defer poh.Disconnect()
	err = poh.NewContainer(c.String("pool"))
	if err != nil {
		return errors.Wrap(err, "unable to create pool metadata container")
	}

	coh, err := poh.Open(c.String("pool"), daos.ContOpenRW)
	if err != nil {
		return errors.Wrap(err, "open container failed")
	}
	defer coh.Close()

	e, err := coh.EpochHold(0)
	if err != nil {
		return errors.Wrap(err, "epoch hold failed")
	}

	oid := daos.ObjectIDInit(0, 0, 1, daos.ClassLargeRW)

	err = coh.ObjectDeclare(oid, e, nil)
	if err != nil {
		return errors.Wrap(err, "obj declare failed")
	}

	oh, err := coh.ObjectOpen(oid, daos.EpochMax, daos.ObjOpenRW)
	if err != nil {
		return errors.Wrap(err, "open object failed")
	}
	defer oh.Close()
	user, err := user.Current()
	if err != nil {
		return errors.Wrap(err, "lookup current user")
	}

	err = oh.Put(e, PoolMetaDkey, CreatorAkey, []byte(user.Name))
	if err != nil {
		return errors.Wrap(err, "put failed")
	}

	b, err := json.Marshal(time.Now())
	if err != nil {
		return errors.Wrap(err, "put failed")
	}

	err = oh.Put(e, PoolMetaDkey, CreatedAkey, b)
	if err != nil {
		return errors.Wrap(err, "put failed")
	}
	coh.EpochCommit(e)
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

	// Fetch pool info from metadata container
	coh, err := poh.Open(c.String("pool"), daos.ContOpenRO)
	if err != nil {
		return errors.Wrap(err, "open container failed")
	}
	defer coh.Close()

	oid := daos.ObjectIDInit(0, 0, 1, daos.ClassLargeRW)

	oh, err := coh.ObjectOpen(oid, daos.EpochMax, daos.ObjOpenRO)
	if err != nil {
		return errors.Wrap(err, "open object failed")
	}
	defer oh.Close()

	creator, err := oh.Get(daos.EpochMax, PoolMetaDkey, CreatorAkey)
	if err != nil {
		return errors.Wrap(err, "get creator failed")
	}

	buf, err := oh.Get(daos.EpochMax, PoolMetaDkey, CreatedAkey)
	if err != nil {
		return errors.Wrap(err, "get created failed")
	}
	var created time.Time
	err = json.Unmarshal(buf, &created)
	if err != nil {
		return errors.Wrapf(err, "create time: %s", buf)
	}

	fmt.Printf("Pool:     %s\n", info.UUID())
	fmt.Printf("Mode:     0%o\n", info.Mode())
	fmt.Printf("Targets:  %d\n", info.NumTargets())
	fmt.Printf("Disabled: %d\n", info.NumDisabled())
	fmt.Printf("Creator:  %s\n", creator)
	fmt.Printf("Created:  %s\n", created)

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
