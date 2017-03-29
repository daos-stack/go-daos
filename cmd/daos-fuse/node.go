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
	attr.Size = uint64(da.Size)
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

// Setxattr sets an extended attribute with the given name and
// value for the node.
func (n *Node) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {
	return n.node.Setxattr(req.Name, req.Xattr, req.Flags)
}

// Getxattr gets an extended attribute by the given name from the
// node.
//
// If there is no xattr by that name, returns fuse.ErrNoXattr.
func (n *Node) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	val, err := n.node.Getxattr(req.Name)
	if err != nil {
		if err == daosfs.ErrNoXattr {
			err = fuse.ErrNoXattr
		}
		return err
	}
	if req.Size > 0 && len(val) > int(req.Size) {
		debug.Printf("%s: getxattr truncating value to %d", n.node.Name, req.Size)
		val = val[:req.Size]
	}
	resp.Xattr = val
	return nil
}

// Listxattr lists the extended attributes recorded for the node.
//
// If there is no xattr by that name, returns fuse.ErrNoXattr.
func (n *Node) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	val, err := n.node.Listxattr()
	if err != nil {
		if err == daosfs.ErrNoXattr {
			err = fuse.ErrNoXattr
		}
		return err
	}
	resp.Append(val...)
	return nil
}

// Removexattr removes an extended attribute for the name.
//
// If there is no xattr by that name, returns fuse.ErrNoXattr.
func (n *Node) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {
	err := n.node.Removexattr(req.Name)
	if err == daosfs.ErrNoXattr {
		err = fuse.ErrNoXattr
	}
	return err
}
