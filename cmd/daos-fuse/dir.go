package main

import (
	"context"

	"github.com/daos-stack/go-daos/pkg/daosfs"
	"github.com/intel-hpdd/logging/debug"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

// ReadDirAll fetches the children of this directory's node and
// populates a slice of fuse.Dirent
func (n *Node) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	children, err := n.node.Children()
	if err != nil {
		return nil, err
	}

	ents := make([]fuse.Dirent, len(children))
	for i, child := range children {
		ents[i] = fuse.Dirent{
			Inode: child.Inode(),
			Name:  child.Name,
		}
		switch t := child.Type; {
		case t.IsDir():
			ents[i].Type = fuse.DT_Dir
		case t.IsRegular():
			ents[i].Type = fuse.DT_File
		}
	}

	return ents, nil
}

// Lookup takes the name of an entry in this directory and returns
// a fs.Node, if found
func (n *Node) Lookup(ctx context.Context, name string) (fs.Node, error) {
	child, err := n.node.Lookup(name)
	if err != nil {
		debug.Printf("error in lookup: %s", err)
		return nil, fuse.ENOENT
	}

	debug.Printf("child %s: %v", child.Name, child.Type())
	return NewNode(child), nil
}

// Mkdir creates a directory node
func (n *Node) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	child, err := n.node.Mkdir(&daosfs.MkdirRequest{
		Uid:  req.Header.Uid,
		Gid:  req.Header.Gid,
		Name: req.Name,
		Mode: req.Mode,
	})
	if err != nil {
		// FIXME: Translate to the correct error
		debug.Printf("Got error from DaosFileSystem: %s", err)
		return nil, fuse.EIO
	}
	return NewNode(child), nil
}

// Create creates a new file and returns a handle to it
func (n *Node) Create(ctx context.Context, req *fuse.CreateRequest, res *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	dreq := daosfs.CreateRequest{
		Uid:   req.Header.Uid,
		Gid:   req.Header.Gid,
		Flags: uint32(req.Flags),
		Name:  req.Name,
		Mode:  req.Mode,
	}

	child, err := n.node.Create(&dreq)
	if err != nil {
		return nil, nil, err
	}

	// TODO: Fill in the *fuse.CreateResponse?

	return NewNode(child), NewFileHandle(child, child.FileHandle), nil
}

// Remove implements unlink/rmdir
func (n *Node) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	if req.Dir {
		return n.node.Rmdir(req.Name)
	}
	return n.node.Unlink(req.Name)
}
