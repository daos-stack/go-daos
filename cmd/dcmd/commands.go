package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/daos-stack/go-daos/pkg/daos"

	"github.com/pkg/errors"
	cli "gopkg.in/urfave/cli.v1"
)

type objectClass struct {
	id daos.OClassID
}

func (f *objectClass) Set(value string) error {
	for _, cls := range daos.ObjectClassList() {
		if strings.ToUpper(value) == strings.ToUpper(cls.String()) {
			f.id = cls
			return nil
		}
	}

	return errors.Errorf("Unable to find DAOS Object class %q", value)
}

func (f *objectClass) Value() daos.OClassID {
	return f.id
}

func (f *objectClass) String() string {
	return f.id.String()
}

type objectClassFlag struct {
	Name   string
	Value  *objectClass
	Usage  string
	EnvVar string
	Hidden bool
}

func (f objectClassFlag) String() string {
	return cli.FlagStringer(f)
}

func (f objectClassFlag) Apply(set *flag.FlagSet) {
	if f.EnvVar != "" {
		for _, envVar := range strings.Split(f.EnvVar, ",") {
			envVar = strings.TrimSpace(envVar)
			if envVal := os.Getenv(envVar); envVal != "" {
				newVal := new(objectClass)
				err := newVal.Set(envVal)
				if err != nil {
					fmt.Fprintf(cli.ErrWriter, err.Error())
				}
				f.Value = newVal
				break
			}
		}
	}

	parts := strings.Split(f.Name, ",")
	for _, name := range parts {
		name = strings.Trim(name, " ")
		if f.Value == nil {
			f.Value = new(objectClass)
		}
		set.Var(f.Value, name, f.Usage)
	}
}

func (f objectClassFlag) GetName() string {
	return f.Name
}

func daosCommand(cmd func(*cli.Context) error) func(*cli.Context) error {
	return func(c *cli.Context) error {
		err := daos.Init()
		if err != nil {
			return errors.Wrap(err, "daos_init failed")
		}
		defer daos.Fini()

		return cmd(c)
	}
}

var poolFlag = cli.StringFlag{
	Name:   "pool",
	Usage:  "UUID of pool to create container in.",
	EnvVar: "DAOS_POOL",
}

var groupFlag = cli.StringFlag{
	Name:   "group, g",
	Usage:  "Group name of pool servers to use.",
	EnvVar: "DAOS_GROUP",
}

var contFlag = cli.StringFlag{
	Name:  "cont",
	Usage: "UUID for the new container.",
}

var objLoFlag = cli.Uint64Flag{
	Name:  "objl",
	Usage: "Lower component of object ID",
	Value: 1,
}

var objMidFlag = cli.Uint64Flag{
	Name:  "objm",
	Usage: "Middle component of object ID",
}

var objHiFlag = cli.UintFlag{
	Name:  "objh",
	Usage: "High component of object ID",
}

var objClassFlag = objectClassFlag{
	Name:  "objc",
	Usage: "Object I/O class",
	Value: &objectClass{id: daos.ClassLargeRW},
}
