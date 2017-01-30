package main

import (
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
				Action:    objHelloCommand,
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
						Name:  "cont",
						Usage: "UUID for the new container.",
					},
					cli.StringFlag{
						Name:  "value",
						Usage: "value for object.",
					},
				},
			},
		},
	}
	commands = append(commands, objCommands)
}

func objHelloCommand(c *cli.Context) error {

	err := daos.Init()
	if err != nil {
		return errors.Wrap(err, "daos_init failed")
	}
	defer daos.Fini()

	group := c.String("group")
	log.Printf("open pool: %v", c.String("pool"))
	poh, err := daos.PoolConnect(c.String("pool"), group, daos.PoolConnectRW)
	if err != nil {
		return errors.Wrap(err, "connect failed")
	}
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

	oid := daos.ObjectIDInit(0, 0, 1, daos.ClassTinyRW)

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
		return errors.Wrap(err, "put failed failed")
	}

	cb = coh.EpochCommit

	buf, err := oh.Get(e, "attrs", "hello")
	if err != nil {
		return errors.Wrap(err, "put failed failed")
	}
	log.Printf("fetched buf '%s'", buf)
	return nil
}
