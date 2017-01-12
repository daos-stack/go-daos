package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"syscall"
)

var defaultRunner = "orterun"
var defaultURI = "/tmp/daos-uri"

func relaunch() {
	np := flag.Int("np", 1, "number of threads processes to run")
	uri := flag.String("uri", defaultURI, "path to URI file")
	runner := flag.String("runner", defaultRunner, "mpi runner")
	flag.Parse()

	prog, err := exec.LookPath(*runner)
	if err != nil {
		log.Fatalf("%s: not found", *runner)
	}
	args := []string{prog}
	if *np > 0 {
		args = append(args, "--np", fmt.Sprintf("%d", *np))
	}
	if *uri != "" {
		args = append(args, "--ompi-server", "file:"+*uri)
	}
	args = append(args, os.Args...)
	// fmt.Println("launch", args)
	err = syscall.Exec("/home/vagrant/daos//install/bin/orterun", args, os.Environ())
	if err != nil {
		panic(err)
	}
	os.Exit(0)
}
