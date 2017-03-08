package main

import (
	"fmt"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/daos-stack/go-daos/pkg/daosfs"
	"github.com/intel-hpdd/logging/debug"
)

func mount(group, pool, name, mountpoint string) error {
	dfs, err := daosfs.NewDaosFileSystem(group, pool, name)
	if err != nil {
		return err
	}
	defer dfs.Fini()

	if mountpoint == "" {
		mountpoint = "/mnt/" + name
	}
	c, err := fuse.Mount(mountpoint)
	if err != nil {
		return err
	}
	defer c.Close()

	srv := fs.New(c, &fs.Config{
		Debug: func(msg interface{}) {
			if !debug.Enabled() {
				return
			}
			debug.Output(4, fmt.Sprint(msg))
		},
	})
	filesystem := &FS{
		dfs: dfs,
	}
	root := NewDir(dfs.Root())
	filesystem.root = root
	if err := srv.Serve(filesystem); err != nil {
		return err
	}

	<-c.Ready
	if err := c.MountError; err != nil {
		return err
	}

	return nil
}
