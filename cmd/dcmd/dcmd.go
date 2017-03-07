package main

import (
	"fmt"
	"log"
	"os"

	"github.com/daos-stack/go-daos/pkg/orterun"

	cli "gopkg.in/urfave/cli.v1"
)

var commands []cli.Command

const version = "0.1"

func myMain() int {
	app := cli.NewApp()
	app.Usage = "DAOS-related actions"
	app.UsageText = `dcmd [global options] commadn [command options] [arguments...]

	 This command must be run using the customized orterun from DAOS project.`
	app.Commands = commands
	app.Version = version
	app.Authors = []cli.Author{
		{
			Name:  "Intel® HPDD",
			Email: "HPDD-enterprise-lustre@intel.com",
		},
	}
	app.Flags = []cli.Flag{
		cli.IntFlag{
			Name:  "np",
			Usage: "Number of processes to start",
			Value: 1,
		},
		cli.StringFlag{
			Name:  "uri",
			Usage: "Path to URI file for daos_server",
			Value: orterun.DefaultURI,
		},
		cli.StringFlag{
			Name:  "runner",
			Usage: "path to MPI driver cmpatible with DAOS",
			Value: orterun.DefaultRunner,
		},
		// cli.BoolFlag{
		// 	Name:  "debug",
		// 	Usage: "Display debug logging to console",
		// },
		// cli.StringFlag{
		// 	Name:  "logfile, l",
		// 	Usage: "Log tool activity to this file",
		// 	Value: "",
		// },
	}
	// app.Before = configureLogging

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return 1
	}

	return 0
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if _, ok := os.LookupEnv("PMIX_RANK"); !ok {
		orterun.Relaunch()
	}

	os.Exit(myMain())
}
