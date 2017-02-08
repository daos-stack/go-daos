package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"log"

	"github.com/daos-stack/go-daos/pkg/daos"
	"github.com/pkg/errors"

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
				Usage:     "List dkeys of an object (currently same object created by hello)",
				ArgsUsage: "[uuid [uuid...]]",
				Action:    daosCommand(objDkeys),
				Flags: []cli.Flag{
					poolFlag,
					groupFlag,
					contFlag,
					objLoFlag,
					objMidFlag,
					objHiFlag,
					objClassFlag,
				},
			},
			{
				Name:      "akeys",
				Usage:     "List akeys of an object (currently same object created by hello)",
				ArgsUsage: "[uuid [uuid...]]",
				Action:    daosCommand(objAkeys),
				Flags: []cli.Flag{
					poolFlag,
					groupFlag,
					contFlag,
					objLoFlag,
					objMidFlag,
					objHiFlag,
					objClassFlag,
					cli.StringFlag{
						Name:  "dkey",
						Usage: "List the akeys for this dkey",
					},
				},
			},
			{
				Name:      "fetch",
				Usage:     "Fetch akey of an object)",
				ArgsUsage: "[uuid [uuid...]]",
				Action:    daosCommand(objFetch),
				Flags: []cli.Flag{
					poolFlag,
					groupFlag,
					contFlag,
					objLoFlag,
					objMidFlag,
					objHiFlag,
					objClassFlag,
					cli.StringFlag{
						Name:  "dkey",
						Usage: "The dkey to fetch from",
					},
					cli.StringFlag{
						Name:  "akey",
						Usage: "The akey to lookup",
					},
					cli.IntFlag{
						Name:  "epoch, e",
						Usage: "Epoch to use to fetch data. Default is GHCE",
					},
					cli.BoolFlag{
						Name:  "hex, x",
						Usage: "Print data as hex value (useful for binary data.",
					},
					cli.BoolFlag{
						Name:  "verbose, v",
						Usage: "Print chatty messages ",
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

	pm, err := OpenMeta(poh, c.String("pool"), false)
	if err != nil {
		return errors.Wrap(err, "open meta")
	}
	defer pm.Close()

	log.Printf("open container: %v", c.String("cont"))
	coh, err := pm.OpenContainer(c.String("cont"), daos.ContOpenRW)
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

	oid := daos.ObjectIDInit(0, 0, 2, daos.ClassLargeRW)

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
	defer poh.Disconnect()

	pm, err := OpenMeta(poh, c.String("pool"), false)
	if err != nil {
		return errors.Wrap(err, "open meta")
	}
	defer pm.Close()

	coh, err := pm.OpenContainer(c.String("cont"), daos.ContOpenRW)
	if err != nil {
		return errors.Wrap(err, "open container failed")
	}
	defer coh.Close()

	oClass := c.Generic("objc").(*daos.OClassID)
	oid := daos.ObjectIDInit((uint32)(c.Uint("objh")), c.Uint64("objm"), c.Uint64("objl"), *oClass)
	log.Printf("oid: %s", oid)
	oh, err := coh.ObjectOpen(oid, daos.EpochMax, daos.ObjOpenRW)
	if err != nil {
		return errors.Wrap(err, "open object failed")
	}
	defer oh.Close()

	var anchor daos.Anchor
	for !anchor.EOF() {
		dkeys, err := oh.DistKeys(daos.EpochMax, &anchor)
		if err != nil {
			return err
		}
		for i := range dkeys {
			fmt.Printf("%s\n", dkeys[i])
		}
	}
	return nil
}

func objAkeys(c *cli.Context) error {
	poh, err := openPool(c, daos.PoolConnectRW)
	if err != nil {
		return err
	}
	defer poh.Disconnect()

	pm, err := OpenMeta(poh, c.String("pool"), false)
	if err != nil {
		return errors.Wrap(err, "open meta")
	}
	defer pm.Close()

	coh, err := pm.OpenContainer(c.String("cont"), daos.ContOpenRW)
	if err != nil {
		return errors.Wrap(err, "open container failed")
	}
	defer coh.Close()

	oClass := c.Generic("objc").(*daos.OClassID)
	oid := daos.ObjectIDInit((uint32)(c.Uint("objh")), c.Uint64("objm"), c.Uint64("objl"), *oClass)

	oh, err := coh.ObjectOpen(oid, 0, daos.ObjOpenRW)
	if err != nil {
		return errors.Wrap(err, "open object failed")
	}
	defer oh.Close()

	dkey := c.String("dkey")

	var anchor daos.Anchor

	for !anchor.EOF() {
		akeys, err := oh.AttrKeys(0, []byte(dkey), &anchor)
		if err != nil {
			return err
		}
		for i := range akeys {
			fmt.Printf("%s\n", akeys[i])
		}
	}
	return nil
}

func objFetch(c *cli.Context) error {
	verbose := c.Bool("verbose")
	poh, err := openPool(c, daos.PoolConnectRW)
	if err != nil {
		return err
	}
	defer poh.Disconnect()

	pm, err := OpenMeta(poh, c.String("pool"), false)
	if err != nil {
		return errors.Wrap(err, "open meta")
	}
	defer pm.Close()

	coh, err := pm.OpenContainer(c.String("cont"), daos.ContOpenRW)
	if err != nil {
		return errors.Wrap(err, "open container failed")
	}
	defer coh.Close()

	epoch := daos.Epoch(c.Uint("epoch"))
	if epoch == 0 {
		es, err := coh.EpochQuery()
		if err != nil {
			return errors.Wrap(err, "epoch query")
		}

		epoch = es.GHCE()
	}

	oClass := c.Generic("objc").(*daos.OClassID)
	oid := daos.ObjectIDInit((uint32)(c.Uint("objh")), c.Uint64("objm"), c.Uint64("objl"), *oClass)

	oh, err := coh.ObjectOpen(oid, epoch, daos.ObjOpenRW)
	if err != nil {
		return errors.Wrap(err, "open object failed")
	}
	defer oh.Close()

	value, err := oh.Get(epoch, c.String("dkey"), c.String("akey"))
	if err != nil {
		return errors.Wrap(err, "get key")

	}
	if verbose {
		fmt.Printf("epoch: %d\n", epoch)
		fmt.Printf("object: %s\n", oid)
		fmt.Printf("%s/%s: ", c.String("dkey"), c.String("akey"))
	}
	if c.Bool("hex") || bytes.ContainsRune(value, 0) {
		if verbose {
			fmt.Println()
		}
		fmt.Print(hex.Dump(value))
	} else {
		fmt.Printf("%s\n", value)
	}
	return nil
}
