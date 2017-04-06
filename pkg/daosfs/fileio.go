package daosfs

import (
	"encoding/binary"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/daos-stack/go-daos/pkg/daos"
	"github.com/intel-hpdd/logging/debug"
	"github.com/pkg/errors"
)

// FileHandle encapsulates functionality for performing file i/o
type FileHandle struct {
	node *Node

	Flags uint32
}

// NewFileHandle returns a FileHandle for file i/o
func NewFileHandle(node *Node, flags uint32) *FileHandle {
	return &FileHandle{
		node:  node,
		Flags: flags,
	}
}

func (fh *FileHandle) Write(offset int64, data []byte) (int64, error) {
	if fh.node.IsSnapshot() {
		return 0, unix.EPERM
	}
	var curSize int64
	if fh.Flags&syscall.O_APPEND > 0 {
		var err error
		curSize, err = fh.node.getSize()
		if err != nil {
			return 0, err
		}
		offset = curSize
	}

	tx, err := fh.node.fs.GetWriteTransaction()
	if err != nil {
		return 0, err
	}
	defer func() {
		tx.Complete()
	}()

	debug.Printf("Writing %d bytes @ offset %d to %s (%s)", len(data), offset, fh.node.Oid, fh.node.Name)

	oh, err := fh.node.oh()
	if err != nil {
		return 0, err
	}
	oh.Lock()
	defer oh.Unlock()

	// Write the data
	ar := daos.NewArrayRequest(&daos.BufferedRange{
		Buffer: data,
		Length: int64(len(data)),
		Offset: offset,
	})
	oa := daos.NewArray(oh.OH())
	var total int64
	if total, err = oa.Write(tx.Epoch, ar); err != nil {
		return 0, err
	}

	// Update the metadata
	var keys []*daos.KeyRequest
	keys = append(keys, daos.NewKeyRequest([]byte("Size")))
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(curSize+int64(len(data))))
	keys[0].Put(0, 1, 8, buf)

	mtime, err := time.Now().MarshalBinary()
	if err != nil {
		return 0, errors.Wrap(err, "Failed to marshal time.Now()")
	}
	keys = append(keys, daos.NewKeyRequest([]byte("Mtime")))
	keys[1].Put(0, 1, uint64(len(mtime)), mtime)

	if err := oh.Update(tx.Epoch, []byte("."), keys); err != nil {
		return 0, err
	}

	tx.Commit()
	return total, nil
}

func (fh *FileHandle) Read(offset, size int64, data []byte) (int64, error) {
	actualSize, err := fh.node.getSize()
	if err != nil {
		return 0, err
	}
	if size > actualSize {
		size = actualSize
	}

	debug.Printf("Reading %d bytes @ offset %d from %s (%s)", size, offset, fh.node.Oid, fh.node.Name)

	oh, err := fh.node.oh()
	if err != nil {
		return 0, err
	}
	oh.RLock()
	defer oh.RUnlock()

	// Read the data
	ar := daos.NewArrayRequest(&daos.BufferedRange{
		Buffer: data,
		Length: size,
		Offset: offset,
	})
	oa := daos.NewArray(oh.OH())

	return oa.Read(fh.node.Epoch(), ar)
}
