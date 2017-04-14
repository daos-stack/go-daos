package daosfs

import (
	"encoding/binary"
	"hash/fnv"
	"os"
	"sync"
	"time"

	"github.com/daos-stack/go-daos/pkg/daos"
	"github.com/daos-stack/go-daos/pkg/ufd"
	lru "github.com/hashicorp/golang-lru"
	"github.com/intel-hpdd/logging/debug"
	"github.com/pkg/errors"
)

var (
	// RootOID is the root of the filesystem
	RootOID = daos.ObjectIDInit(0, 0, 1, daos.ClassTinyRW)
	// MetaOID is the object where misc. filesystem metadata is stashed
	MetaOID = daos.ObjectIDInit(0, 1, 1, daos.ClassTinyRW)
	// HandleCacheSize is the max number of open object handles allowed
	// in cache
	HandleCacheSize = 1024
)

// LockableObjectHandle wraps a daos.ObjectHandle with a sync.RWMutex
type LockableObjectHandle struct {
	sync.RWMutex
	daos.ObjectHandle
}

// OH returns the wrapped *daos.ObjectHandle
func (loh *LockableObjectHandle) OH() *daos.ObjectHandle {
	return &loh.ObjectHandle
}

// WriteTransaction wraps a writable epoch with some transactional
// logic that will ensure the epoch is discarded or committed when
// used in a deferred function.
type WriteTransaction struct {
	Epoch daos.Epoch
	Error error

	completed  bool
	completeFn func(daos.Epoch) error
	commitFn   func(daos.Epoch) error
}

// Complete triggers a call to the transaction's complete function.
func (wt *WriteTransaction) Complete() error {
	if wt.completed {
		return errors.New("Complete() called on completed tx")
	}

	if wt.Error = wt.completeFn(wt.Epoch); wt.Error != nil {
		debug.Printf("Error in tx.Complete() (epoch %d): %s", wt.Epoch, wt.Error)
		return errors.Wrap(wt.Error, "Error in tx.Complete()")
	}

	wt.completed = true
	return nil
}

// Commit sets the transaction's complete function to the supplied
// commit function.
func (wt *WriteTransaction) Commit() {
	wt.completeFn = wt.commitFn
}

// FileSystem provides a filesystem-like interface to DAOS
type FileSystem struct {
	Name string
	root *Node
	uh   *ufd.Handle
	ch   *daos.ContHandle
	og   *oidGenerator
	hc   *lru.TwoQueueCache
	em   *epochManager
	id   uint32
}

// NewFileSystem connects to the given pool and creates a container
// for the given filesystem name
func NewFileSystem(group, pool, container string) (*FileSystem, error) {
	debug.Printf("Connecting to %s (group: %q)", pool, group)
	uh, err := ufd.Connect(group, pool)
	if err != nil {
		return nil, errors.Wrapf(err, "Connection to %q failed", pool)
	}
	debug.Printf("Connected to %s", pool)

	fs := &FileSystem{
		Name: container,
		uh:   uh,
		root: &Node{
			modeType: os.ModeDir,
			Oid:      RootOID,
			Name:     "",
		},
	}
	fs.root.Parent = fs.root
	fs.root.fs = fs
	fs.og = newOidGenerator(fs)

	fs.hc, err = lru.New2QEvict(
		HandleCacheSize,
		// Close the object on eviction from the cache
		func(key, value interface{}) {
			oh := value.(*LockableObjectHandle)
			oh.Lock()
			oh.Close()
			oh.Unlock()
			debug.Printf("Closed handle for %s on eviction", key)
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create handle cache")
	}

	var created bool
	debug.Printf("Opening filesystem container %q", fs.Name)
	ch, err := fs.uh.OpenContainer(fs.Name, daos.ContOpenRW)
	if err != nil {
		debug.Printf("Error opening %q: %s; attempting to create it.", fs.Name, err)
		if err = fs.uh.NewContainer(fs.Name, ""); err != nil {
			return nil, errors.Wrapf(err, "Failed to create container %q", fs.Name)
		}
		created = true
		if ch, err = fs.uh.OpenContainer(fs.Name, daos.ContOpenRW); err != nil {
			return nil, errors.Wrapf(err, "Failed to open container %q after create", fs.Name)
		}
		debug.Printf("Created container for %q", fs.Name)
	}
	fs.ch = ch

	fs.em, err = newEpochManager(ch)
	if err != nil {
		return nil, err
	}

	if created {
		if _, err = fs.DeclareObject(fs.root.Oid, daos.ClassTinyRW); err != nil {
			return nil, err
		}
		now := time.Now()
		rootAttr := &Attr{
			Mode:  os.ModeDir | 0755,
			Uid:   uint32(os.Getuid()),
			Gid:   uint32(os.Getgid()),
			Atime: now,
			Mtime: now,
			Ctime: now,
		}
		err = fs.em.withCommit(func(e daos.Epoch) error {
			return fs.root.writeAttr(e, rootAttr, WriteAttrAll)
		})
	} else {
		_, err = fs.OpenObject(fs.root.Oid)
	}

	// Not sure if this is entirely correct, but should be good enough.
	fs.id = binary.LittleEndian.Uint32(fnv.New32().Sum([]byte(pool + fs.Name)))

	return fs, err
}

// GetWriteTransaction returns a transaction object which contains
// a writable epoch. It is intended to be used in a deferred function
// which will ensure that the epoch is discarded or committed when
// the enclosing function exits.
func (fs *FileSystem) GetWriteTransaction() (*WriteTransaction, error) {
	epoch, err := fs.GetWriteEpoch()
	if err != nil {
		return nil, err
	}

	return &WriteTransaction{
		Epoch:      epoch,
		completeFn: fs.DiscardEpoch,
		commitFn:   fs.ReleaseEpoch,
	}, nil
}

// GetWriteEpoch returns an epoch that hasn't been committed yet as
// far as we know at this time. We can guarantee that the epoch won't
// be committed by this process until ReleaseEpoch() is called, but we
// can't guarantee that the epoch wasn't committed by another process.
func (fs *FileSystem) GetWriteEpoch() (daos.Epoch, error) {
	return fs.em.GetWriteEpoch()
}

// GetReadEpoch always returns an epoch that is less than or equal to
// GHCE. As a result, reads at this epoch are guaranteed to be consistent,
// but may not contain the latest data.
func (fs *FileSystem) GetReadEpoch() daos.Epoch {
	return fs.em.GetReadEpoch()
}

// ReleaseEpoch attempts to commit any changes at the given epoch -- in
// addition to any changes made at earlier epochs which have been fully
// released but have not yet been committed to DAOS. If the given epoch
// is the lowest or only held epoch known by this process, then the epoch
// will be committed immediately. Otherwise, the epoch will be committed
// when all lower epochs have been committed.
func (fs *FileSystem) ReleaseEpoch(e daos.Epoch) error {
	return fs.em.Commit(e)
}

// DiscardEpoch will discard any changes made at the given epoch and
// release it. Any other holders of that epoch will get an error if
// they attempt to commit their changes.
func (fs *FileSystem) DiscardEpoch(e daos.Epoch) error {
	return fs.em.Discard(e)
}

// OpenObjectEpoch returns an open *daos.ObjectHandle for the given oid
// and epoch
func (fs *FileSystem) OpenObjectEpoch(oid *daos.ObjectID, epoch daos.Epoch) (*LockableObjectHandle, error) {
	start := time.Now()
	// TODO: Do we need to cache handles opened at different epochs? What
	// does that even mean? Does it matter if a handle is opened at
	// epoch N and then some update happens at N+2 (or N-3)?
	if oh, ok := fs.hc.Get(oid); ok {
		debug.Printf("Retrieved handle for %s from cache in %s", oid, time.Since(start))
		return oh.(*LockableObjectHandle), nil
	}

	oh, err := fs.ch.ObjectOpen(oid, epoch, daos.ObjOpenRW)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to open %s @ %s", oid, epoch)
	}
	debug.Printf("Opened handle for %s in %s", oid, time.Since(start))

	loh := &LockableObjectHandle{
		sync.RWMutex{},
		*oh,
	}

	fs.hc.Add(oid, loh)
	return loh, nil
}

// OpenObject returns an open *daos.ObjectHandle for the given oid
// and current epoch
func (fs *FileSystem) OpenObject(oid *daos.ObjectID) (*LockableObjectHandle, error) {
	return fs.OpenObjectEpoch(oid, fs.GetReadEpoch())
}

// DeclareObjectEpoch first declares an object, then opens it and returns
// an open *daos.ObjectHandle for the given oid and epoch
func (fs *FileSystem) DeclareObjectEpoch(oid *daos.ObjectID, epoch daos.Epoch, oc daos.OClassID) (*LockableObjectHandle, error) {
	// The C API accepts an optional daos_obj_attr_t value, which at
	// the moment is just an object class and a rank affinity. The
	// rank affinity member may go away eventually. The object class
	// member may eventually be used in layout calculation, but is
	// currently ignored.
	// For our API, I think we should accept an optional list of
	// object attributes and then deal with glomming them into a
	// daos_obj_attr_t in here.

	// NB: This is currently a noop in DAOS, but we should be doing it
	// to be future-proof.
	if err := fs.ch.ObjectDeclare(oid, epoch, nil); err != nil {
		return nil, err
	}

	return fs.OpenObjectEpoch(oid, epoch)
}

// DeclareObject first declares an object, then opens it and returns
// an open *daos.ObjectHandle for the given oid and current epoch
func (fs *FileSystem) DeclareObject(oid *daos.ObjectID, oc daos.OClassID) (*LockableObjectHandle, error) {
	// TODO: Does it matter which epoch we declare an object at?
	return fs.DeclareObjectEpoch(oid, fs.GetReadEpoch(), oc)
}

// CloseObject removes the object handle from cache and closes it
func (fs *FileSystem) CloseObject(oid *daos.ObjectID) {
	// handle is closed via evict callback
	fs.hc.Remove(oid)
}

// Root returns the root node
func (fs *FileSystem) Root() *Node {
	return fs.root
}

// Device returns a uint32 suitable for use as st_dev to identify
// the filesystem.
func (fs *FileSystem) Device() uint32 {
	return fs.id
}

// GetNode returns a *Node for the given ObjectID and Epoch
func (fs *FileSystem) GetNode(oid *daos.ObjectID, epoch *daos.Epoch) (*Node, error) {
	node := &Node{
		Oid:       oid,
		readEpoch: epoch,
		fs:        fs,
	}

	attr, err := node.GetAttr()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to get node attributes")
	}

	// TODO: Now, here's where things get interesting. At this point,
	// we've established that the given OID is associated with a
	// previously-saved Node, but we don't know anything about its name,
	// parent, etc.
	node.modeType = attr.Mode & os.ModeType
	node.Parent = &Node{} // just to avoid nil ptr deref errors
	node.Name = "help_i_dont_know_who_i_am"

	return node, nil
}

// Fini shuts everything down
func (fs *FileSystem) Fini() error {
	start := time.Now()
	fs.hc.Purge()
	fs.ch.Close()
	fs.uh.Close()
	debug.Printf("Shutdown took %s", time.Since(start))
	return nil
}
