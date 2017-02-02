package main

import (
	"fmt"
	"log"

	"github.com/pkg/errors"
	"github.com/rread/go-daos/pkg/daos"

	cli "gopkg.in/urfave/cli.v1"
)

func init() {
	objCommands := cli.Command{
		Name:  "object",
		Usage: "DAOS Container related commands",
		Subcommands: []cli.Command{
			{
				Name:      "hello",
				Usage:     "Create an object in  a container",
				ArgsUsage: "[uuid [uuid...]]",
				Action:    daosCommand(objHello),
				Flags: []cli.Flag{
					poolFlag,
					groupFlag,
					contFlag,
					cli.StringFlag{
						Name:  "value",
						Usage: "value for object.",
					},
				},
			},
			{
				Name:      "dkeys",
				Usage:     "Create an object in  a container",
				ArgsUsage: "[uuid [uuid...]]",
				Action:    daosCommand(objDkeys),
				Flags: []cli.Flag{
					poolFlag,
					groupFlag,
					contFlag,
				},
			},
			{
				Name:      "akeys",
				Usage:     "Create an object in  a container",
				ArgsUsage: "[uuid [uuid...]]",
				Action:    daosCommand(objAkeys),
				Flags: []cli.Flag{
					poolFlag,
					groupFlag,
					contFlag,
					cli.StringFlag{
						Name:  "dkey",
						Usage: "List the akeys for this dkey",
					},
				},
			},
		},
	}
	commands = append(commands, objCommands)
}

func objHello(c *cli.Context) error {
	poh, err := openPool(c, daos.PoolConnectRW)
	if err != nil {
		return errors.Wrap(err, "connect failed")
	}
	defer poh.Disconnect()

	log.Printf("open container: %v", c.String("cont"))
	coh, err := poh.Open(c.String("cont"), daos.ContOpenRW)
	if err != nil {
		return errors.Wrap(err, "open container failed")
	}
	defer coh.Close()

	s, err := coh.EpochQuery()
	if err != nil {
		return errors.Wrap(err, "epoch query failed")
	}
	log.Printf("epoch state [%s]", s)

	e, err := coh.EpochHold(0)
	if err != nil {
		return errors.Wrap(err, "epoch hold failed")
	}

	cb := coh.EpochDiscard
	defer func() {
		cb(e)
	}()

	log.Printf("held epoch %s", e)

	oid := daos.ObjectIDInit(0, 0, 1, daos.ClassLargeRW)

	err = coh.ObjectDeclare(oid, e, nil)
	if err != nil {
		return errors.Wrap(err, "obj declare failed")
	}

	oh, err := coh.ObjectOpen(oid, e, daos.ObjOpenRW)
	if err != nil {
		return errors.Wrap(err, "open object failed")
	}
	defer oh.Close()

	val := c.String("value")

	log.Printf("put: '%s'", val)
	err = oh.Put(e, "attrs", "hello", []byte(val))
	if err != nil {
		return errors.Wrap(err, "put failed")
	}

	cb = coh.EpochCommit

	buf, err := oh.Get(e, "attrs", "hello")
	if err != nil {
		return errors.Wrap(err, "put failed failed")
	}
	log.Printf("fetched buf '%s'", buf)
	return nil
}

func openPool(c *cli.Context, flags uint) (*daos.PoolHandle, error) {
	group := c.String("group")
	pool := c.String("pool")
	if pool == "" {
		return nil, errors.New("no pool uuid provided")
	}

	poh, err := daos.PoolConnect(pool, group, flags)
	if err != nil {
		return nil, errors.Wrap(err, "connect failed")
	}
	return poh, nil
}

func objPut(c *cli.Context) error {
	poh, err := openPool(c, daos.PoolConnectRW)
	if err != nil {
		return err
	}
	defer poh.Disconnect()
	return nil

}

func objDkeys(c *cli.Context) error {
	poh, err := openPool(c, daos.PoolConnectRW)
	if err != nil {
		return err
	}
	coh, err := poh.Open(c.String("cont"), daos.ContOpenRW)
	if err != nil {
		return errors.Wrap(err, "open container failed")
	}
	defer coh.Close()

	oid := daos.ObjectIDInit(0, 0, 1, daos.ClassLargeRW)

	oh, err := coh.ObjectOpen(oid, daos.EpochMax, daos.ObjOpenRW)
	if err != nil {
		return errors.Wrap(err, "open object failed")
	}
	defer oh.Close()

	dkeys, anchor, err := oh.DistKeys(daos.EpochMax, nil)
	if err != nil {
		return err
	}
	for {
		if len(dkeys) == 0 {
			break
		}
		for i := range dkeys {
			fmt.Printf("%v\n", dkeys[i])
		}
		dkeys, anchor, err = oh.DistKeys(daos.EpochMax, anchor)
		if err != nil {
			return err
		}
	}
	return nil
}

func objAkeys(c *cli.Context) error {
	poh, err := openPool(c, daos.PoolConnectRW)
	if err != nil {
		return err
	}
	coh, err := poh.Open(c.String("cont"), daos.ContOpenRW)
	if err != nil {
		return errors.Wrap(err, "open container failed")
	}
	defer coh.Close()

	oid := daos.ObjectIDInit(0, 0, 1, daos.ClassLargeRW)

	oh, err := coh.ObjectOpen(oid, daos.EpochMax, daos.ObjOpenRW)
	if err != nil {
		return errors.Wrap(err, "open object failed")
	}
	defer oh.Close()

	dkey := c.String("dkey")

	dkeys, anchor, err := oh.AttrKeys(daos.EpochMax, []byte(dkey), nil)
	if err != nil {
		return err
	}
	for {
		if len(dkeys) == 0 {
			break
		}
		for i := range dkeys {
			fmt.Printf("%v\n", dkeys[i])
		}
		dkeys, anchor, err = oh.DistKeys(daos.EpochMax, anchor)
		if err != nil {
			return err
		}
	}
	return nil
}
