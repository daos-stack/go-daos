package main

import (
	"context"

	"bazil.org/fuse"

	"github.com/daos-stack/go-daos/pkg/daosfs"
	"github.com/intel-hpdd/logging/debug"
	"github.com/pkg/errors"
)

// Node wraps a *daosfs.Node
type Node struct {
	node *daosfs.Node
}

// Attr implements the base fs.Node interface
func (n *Node) Attr(ctx context.Context, attr *fuse.Attr) error {
	da, err := n.node.Attr()
	if err != nil {
		debug.Printf("error in Attr(): %s", err)
		return errors.Wrap(err, "Failed to get DAOS Node attributes")
	}

	attr.Inode = da.Inode
	attr.Size = da.Size
	//attr.Blocks = da.Blocks
	//attr.Atime = da.Atime
	attr.Mtime = da.Mtime
	//attr.Ctime = da.Ctime
	attr.Mode = da.Mode
	//attr.Nlink = da.Nlink
	attr.Uid = da.Uid
	attr.Gid = da.Gid
	//attr.Rdev = da.Rdev
	//attr.Blocksize = da.Blocksize

	return nil
}
