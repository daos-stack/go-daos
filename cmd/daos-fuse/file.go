package main

import (
	"context"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/daos-stack/go-daos/pkg/daosfs"
	"github.com/intel-hpdd/logging/debug"
)

// FileHandle represents an open file handle
type FileHandle struct {
	node   *daosfs.Node
	handle *daosfs.FileHandle
}

// NewFileHandle returns a new *FileHandle to wrap the supplied
// *daosfs.FileHandle
func NewFileHandle(node *daosfs.Node, fh *daosfs.FileHandle) *FileHandle {
	return &FileHandle{node: node, handle: fh}
}

// Open implements the NodeOpener interface. If the node being opened
// is a file, return a FileHandle; otherwise return the node itself.
func (n *Node) Open(ctx context.Context, req *fuse.OpenRequest, res *fuse.OpenResponse) (fs.Handle, error) {
	if req.Flags&syscall.O_DIRECTORY != 0 {
		return n, nil
	}
	err := n.node.Open(uint32(req.Flags))
	return NewFileHandle(n.node, n.node.FileHandle), err
}

func (fh *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, res *fuse.WriteResponse) error {
	if fh.handle == nil {
		debug.Print("Write() on nil filehandle")
		return syscall.EINVAL
	}

	if fh.handle.Flags&syscall.O_APPEND > 0 {
		var err error
		req.Offset, err = fh.node.GetSize()
		if err != nil {
			return err
		}
	}

	n, err := fh.handle.Write(req.Offset, req.Data)
	res.Size = int(n)

	if err != nil {
		debug.Printf("error in Write(): %s", err)
	}
	return err
}

func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, res *fuse.ReadResponse) error {
	if fh.handle == nil {
		debug.Print("Read() on nil filehandle")
		return syscall.EINVAL
	}

	read, err := fh.handle.Read(req.Offset, int64(req.Size), res.Data)
	res.Data = res.Data[:read]

	if err != nil {
		debug.Printf("error in Read(): %s", err)
	}
	return err
}

// Flush commits the handle's write transaction
func (fh *FileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	if fh.handle == nil {
		debug.Printf("Flush() on nil filehandle")
		return syscall.EINVAL
	}
	fh.handle.Commit()

	return nil
}

// Release closes the filehandle
func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	if fh.handle == nil {
		debug.Printf("Release() on nil filehandle")
		return syscall.EINVAL
	}

	if req.ReleaseFlags&fuse.ReleaseFlush != 0 {
		fh.handle.Commit()
	}

	return fh.handle.Close()
}
