package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
	"os"

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
					hexFlag,
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
					objDkeyFlag,
					objDkeybFlag,
					hexFlag,
				},
			},

			{
				Name:      "declare",
				Usage:     "Declare a new object)",
				ArgsUsage: "[uuid [uuid...]]",
				Action:    daosCommand(objDeclare),
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
				Name:      "update",
				Usage:     "Declare a new object)",
				ArgsUsage: "[uuid [uuid...]]",
				Action:    daosCommand(objUpdate),
				Flags: []cli.Flag{
					poolFlag,
					groupFlag,
					contFlag,
					objLoFlag,
					objMidFlag,
					objHiFlag,
					objClassFlag,
					objDkeyFlag,
					objDkeybFlag,
					objAkeyFlag,
					objAkeybFlag,
					cli.StringFlag{
						Name:  "value",
						Usage: "Value for object.",
					},
					cli.StringFlag{
						Name:  "file, f",
						Usage: "Input file.",
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
					objDkeyFlag,
					objDkeybFlag,
					objAkeyFlag,
					objAkeybFlag,
					hexFlag,
					cli.IntFlag{
						Name:  "epoch, e",
						Usage: "Epoch to use to fetch data. Default is GHCE",
					},
					cli.BoolFlag{
						Name:  "binary, b",
						Usage: "Write binary data",
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
			if c.Bool("hex") {
				fmt.Printf("%s\n", hex.EncodeToString(dkeys[i]))
			} else {
				fmt.Printf("%s\n", dkeys[i])
			}
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

	dkey := getkey(c, "dkey")

	var anchor daos.Anchor

	for !anchor.EOF() {
		akeys, err := oh.AttrKeys(0, dkey, &anchor)
		if err != nil {
			return err
		}
		for i := range akeys {
			if c.Bool("hex") {
				fmt.Printf("%s\n", hex.EncodeToString(akeys[i]))
			} else {
				fmt.Printf("%s\n", akeys[i])
			}
		}
	}
	return nil
}

func objDeclare(c *cli.Context) error {
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

	coh, err := pm.OpenContainer(c.String("cont"), daos.ContOpenRW)
	if err != nil {
		return errors.Wrap(err, "open container failed")
	}
	defer coh.Close()

	e, err := coh.EpochHold(0)
	if err != nil {
		return errors.Wrap(err, "epoch hold failed")
	}

	cb := coh.EpochDiscard
	defer func() {
		cb(e)
	}()

	oClass := c.Generic("objc").(*daos.OClassID)
	oid := daos.ObjectIDInit((uint32)(c.Uint("objh")), c.Uint64("objm"), c.Uint64("objl"), *oClass)

	err = coh.ObjectDeclare(oid, e, nil)
	if err != nil {
		return errors.Wrap(err, "obj declare failed")
	}
	fmt.Printf("Declared object %s in epoch %d\n", oid, e)

	cb = coh.EpochCommit
	return nil
}

func getkey(c *cli.Context, name string) []byte {
	k := c.String(name)
	if k == "" {
		k = c.String(name + "b")
		if k != "" {
			buf, err := hex.DecodeString(k)
			if err != nil {
				return buf[0:0]
			}
			return buf
		}
	}
	return []byte(k)
}

func objUpdate(c *cli.Context) error {
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

	coh, err := pm.OpenContainer(c.String("cont"), daos.ContOpenRW)
	if err != nil {
		return errors.Wrap(err, "open container failed")
	}
	defer coh.Close()

	val := c.String("value")
	var buf []byte
	if val == "" {
		in := c.String("file")
		if in == "" {
			return errors.New("Must specify --value  or --file")
		}
		buf, err = ioutil.ReadFile(in)
		if err != nil {
			return err
		}
	} else {
		buf = []byte(val)
	}

	epoch, err := coh.EpochHold(0)
	if err != nil {
		return errors.Wrap(err, "epoch hold failed")
	}

	cb := coh.EpochDiscard
	defer func() {
		cb(epoch)
	}()

	oClass := c.Generic("objc").(*daos.OClassID)
	oid := daos.ObjectIDInit((uint32)(c.Uint("objh")), c.Uint64("objm"), c.Uint64("objl"), *oClass)

	oh, err := coh.ObjectOpen(oid, epoch, daos.ObjOpenRW)
	if err != nil {
		return errors.Wrap(err, "open object failed")
	}
	defer oh.Close()

	dkey := getkey(c, "dkey")
	akey := getkey(c, "akey")

	err = oh.Putb(epoch, dkey, akey, buf)
	if err != nil {
		return errors.Wrap(err, "update")
	}
	fmt.Printf("%d bytes, committed epoch %d\n", len(buf), epoch)
	cb = coh.EpochCommit
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

	dkey := getkey(c, "dkey")
	akey := getkey(c, "akey")

	value, err := oh.Getb(epoch, dkey, akey)
	if err != nil {
		return errors.Wrap(err, "get key")

	}
	if verbose {
		fmt.Printf("epoch: %d\n", epoch)
		fmt.Printf("object: %s\n", oid)
		fmt.Printf("%s/%s: ", c.String("dkey"), c.String("akey"))
	}

	if c.Bool("binary") {
		os.Stdout.Write(value)
		return nil
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
