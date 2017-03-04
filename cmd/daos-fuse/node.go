package main

import (
	"context"
	"os"
	"time"

	"bazil.org/fuse"

	"github.com/daos-stack/go-daos/pkg/daosfs"
	"github.com/intel-hpdd/logging/debug"
	"github.com/pkg/errors"
)

// Node wraps a *daosfs.DaosNode
type Node struct {
	node *daosfs.DaosNode
}

// Attr implements the base fs.Node interface
func (n *Node) Attr(ctx context.Context, attr *fuse.Attr) error {
	da, err := n.node.Attr()
	if err != nil {
		debug.Printf("error in Attr(): %s", err)
		return errors.Wrap(err, "Failed to get DAOS Node attributes")
	}

	// FIXME: This is gross, shouldn't be here.
	// Maybe we gin this up when the container is created?
	if n.node.Name == "/" {
		attr.Inode = da.Inode
		attr.Mtime = time.Now()
		attr.Mode = os.ModeDir | 0755
		attr.Uid = uint32(os.Getuid())
		attr.Gid = uint32(os.Getgid())
		return nil
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
