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
	node      *Node
	readEpoch *daos.Epoch
	writeTx   *WriteTransaction

	Flags uint32
}

// NewReadHandle returns a *FileHandle for file reads at the given epoch
func NewReadHandle(node *Node, epoch *daos.Epoch, flags uint32) *FileHandle {
	return &FileHandle{
		node:      node,
		readEpoch: epoch,
		Flags:     flags,
	}
}

// NewWriteHandle returns a *FileHandle for file writes with the given
// transaction.
func NewWriteHandle(node *Node, tx *WriteTransaction, flags uint32) *FileHandle {
	return &FileHandle{
		node:    node,
		writeTx: tx,
		Flags:   flags,
	}
}

// NewReadWriteHandle returns a *FileHandle for file writes with the given
// transaction and reads at that transaction's epoch.
func NewReadWriteHandle(node *Node, tx *WriteTransaction, flags uint32) *FileHandle {
	return &FileHandle{
		node:      node,
		writeTx:   tx,
		readEpoch: &tx.Epoch, // TODO: This or GHCE?
		Flags:     flags,
	}
}

// Commit commits the write transaction, if one exists
func (fh *FileHandle) Commit() {
	if fh.writeTx != nil {
		fh.writeTx.Commit()
	}
}

// Close completes the write transaction, if one exists
func (fh *FileHandle) Close() (err error) {
	if fh.writeTx != nil {
		err = fh.writeTx.Complete()
	}
	fh.writeTx = nil

	return
}

// CanRead indicates whether or not the *FileHandle is in a state for
// reading data
func (fh *FileHandle) CanRead() bool {
	return fh.readEpoch != nil
}

// CanWrite indicates whether or not the *FileHandle is in a state for
// writing data
func (fh *FileHandle) CanWrite() bool {
	return fh.writeTx != nil
}

func (fh *FileHandle) Write(offset int64, data []byte) (int64, error) {
	if !fh.CanWrite() {
		debug.Print("Write() on filehandle with no tx")
		return 0, syscall.EBADFD
	}

	if fh.node.IsSnapshot() {
		return 0, unix.EPERM
	}

	debug.Printf("Writing %d bytes @ offset %d to %s (%s) (epoch %d)", len(data), offset, fh.node.Oid, fh.node.Name, fh.writeTx.Epoch)

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
	var wrote int64
	if wrote, err = oa.Write(fh.writeTx.Epoch, ar); err != nil {
		return 0, err
	}

	// Writing 0 bytes is not an error, but we don't want to update
	// the metadata in that case.
	if wrote == 0 {
		return 0, nil
	}

	// Update the metadata
	var keys []*daos.KeyRequest
	keys = append(keys, daos.NewKeyRequest([]byte("Size")))
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(offset+wrote))
	keys[0].Put(0, 1, 8, buf)

	mtime, err := time.Now().MarshalBinary()
	if err != nil {
		return 0, errors.Wrap(err, "Failed to marshal time.Now()")
	}
	keys = append(keys, daos.NewKeyRequest([]byte("Mtime")))
	keys[1].Put(0, 1, uint64(len(mtime)), mtime)

	if err := oh.Update(fh.writeTx.Epoch, []byte("."), keys); err != nil {
		return 0, err
	}

	return wrote, nil
}

func (fh *FileHandle) Read(offset, size int64, data []byte) (int64, error) {
	if !fh.CanRead() {
		debug.Print("Read() on fh with no read epoch")
		return 0, syscall.EBADFD
	}

	actualSize, err := fh.node.getSize()
	if err != nil {
		return 0, err
	}
	if size > actualSize {
		size = actualSize
	}

	debug.Printf("Reading %d bytes @ offset %d from %s (%s) (epoch %d)", size, offset, fh.node.Oid, fh.node.Name, *fh.readEpoch)

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

	// TODO: Update Atime?
	return oa.Read(*fh.readEpoch, ar)
}
