package main

import (
	"context"

	"github.com/daos-stack/go-daos/pkg/daosfs"
	"github.com/intel-hpdd/logging/debug"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

// Dir represents a directory node
type Dir struct {
	Node
}

// NewDir returns a *Dir
func NewDir(node *daosfs.Node) *Dir {
	return &Dir{
		Node: Node{
			node: node,
		},
	}
}

// ReadDirAll fetches the children of this directory's node and
// populates a slice of fuse.Dirent
func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	children, err := d.node.Children()
	if err != nil {
		return nil, err
	}

	ents := make([]fuse.Dirent, len(children))
	for i, child := range children {
		ents[i] = fuse.Dirent{
			Inode: child.Inode(),
			Name:  child.Name,
		}
	}

	return ents, nil
}

// Lookup takes the name of an entry in this directory and returns
// a fs.Node, if found
func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	child, err := d.node.Lookup(name)
	if err != nil {
		debug.Printf("error in lookup: %s", err)
		return nil, fuse.ENOENT
	}

	debug.Printf("child %s: %v", child.Name, child.Type())
	switch t := child.Type(); {
	case t.IsDir():
		return NewDir(child), nil
	case t.IsRegular():
		return NewFile(child), nil
	default:
		debug.Printf("fell through to generic Node{}")
		return &Node{node: child}, nil
	}
}

// Mkdir creates a directory node
func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	dreq := daosfs.MkdirRequest{
		Uid:  req.Header.Uid,
		Gid:  req.Header.Gid,
		Name: req.Name,
		Mode: req.Mode,
	}
	child, err := d.node.Mkdir(&dreq)
	if err != nil {
		// FIXME: Translate to the correct error
		debug.Printf("Got error from DaosFileSystem: %s", err)
		return nil, fuse.EIO
	}
	return NewDir(child), nil
}

// Create creates a new file and returns a handle to it
func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, res *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	dreq := daosfs.CreateRequest{
		Uid:   req.Header.Uid,
		Gid:   req.Header.Gid,
		Flags: uint32(req.Flags),
		Name:  req.Name,
		Mode:  req.Mode,
	}

	file, handle, err := d.node.Create(&dreq)
	if err != nil {
		return nil, nil, err
	}

	// TODO: Fill in the *fuse.CreateResponse?

	return NewFile(file), &FileHandle{handle: handle}, nil
}

// Remove implements unlink/rmdir
func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	return d.node.Remove(req.Name, req.Dir)
}
