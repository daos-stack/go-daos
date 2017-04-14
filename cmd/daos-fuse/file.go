package main

import (
	"context"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/daos-stack/go-daos/pkg/daosfs"
	"github.com/intel-hpdd/logging/debug"
)

// FileHandle represents an open file handle
type FileHandle struct {
	handle *daosfs.FileHandle
}

// File represents a file node
type File struct {
	Node
}

// NewFile returns a *File
func NewFile(node *daosfs.Node) *File {
	return &File{
		Node: Node{
			node: node,
		},
	}
}

// Open implements the FileOpener interface
func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, res *fuse.OpenResponse) (fs.Handle, error) {
	h, err := f.node.Open(uint32(req.Flags))
	return &FileHandle{handle: h}, err
}

func (fh *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, res *fuse.WriteResponse) error {
	n, err := fh.handle.Write(req.Offset, req.Data)
	res.Size = int(n)

	if err != nil {
		debug.Printf("error in Write(): %s", err)
	}
	return err
}

func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, res *fuse.ReadResponse) error {
	read, err := fh.handle.Read(req.Offset, int64(req.Size), res.Data)
	res.Data = res.Data[:read]

	if err != nil {
		debug.Printf("error in Read(): %s", err)
	}
	return err
}
