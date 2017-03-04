package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/daos-stack/go-daos/pkg/daos"
	"github.com/daos-stack/go-daos/pkg/orterun"
	"github.com/intel-hpdd/logging/debug"
)

func daosMain() int {
	// Scans the arg list and sets up flags
	group := flag.String("group", "", "DAOS server group")
	dbg := flag.Bool("debug", false, "print debugging messages.")
	flag.Parse()
	if flag.NArg() < 3 {
		fmt.Fprintf(os.Stderr, "usage: %s MOUNTPOINT POOL-UUID FSNAME\n", os.Args[0])
		return 2
	}

	if *dbg {
		debug.Enable()
	}

	if err := daos.Init(); err != nil {
		fmt.Fprintf(os.Stderr, "daos.Init() failed: %s", err)
		return 1
	}
	defer daos.Fini()

	if err := mount(*group, flag.Arg(1), flag.Arg(2), flag.Arg(0)); err != nil {
		fmt.Fprintf(os.Stderr, "mount failed: %v\n", err)
		return 1
	}

	return 0
}

func main() {
	if _, ok := os.LookupEnv("PMIX_RANK"); !ok {
		orterun.Relaunch()
	}

	os.Exit(daosMain())
}
