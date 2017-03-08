package main

import (
	"bazil.org/fuse/fs"
	"github.com/daos-stack/go-daos/pkg/daosfs"
)

// FS implements the fuse Filesystem interfaces
type FS struct {
	dfs  *daosfs.DaosFileSystem
	root fs.Node
}

// Root returns the filesystem root node
func (f *FS) Root() (fs.Node, error) {
	return f.root, nil
}
