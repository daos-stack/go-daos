package daosfs

import (
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

// DaosFileSystem provides a filesystem-like interface to DAOS
type DaosFileSystem struct {
	Name string
	root *Node
	uh   *ufd.Handle
	ch   *daos.ContHandle
	og   *oidGenerator
	hc   *lru.TwoQueueCache
}

// NewDaosFileSystem connects to the given pool and creates a container
// for the given filesystem name
func NewDaosFileSystem(group, pool, container string) (*DaosFileSystem, error) {
	debug.Printf("Connecting to %s (group: %q)", pool, group)
	uh, err := ufd.Connect(group, pool)
	if err != nil {
		return nil, errors.Wrapf(err, "Connection to %q failed", pool)
	}
	debug.Printf("Connected to %s", pool)

	dfs := &DaosFileSystem{
		Name: container,
		uh:   uh,
		root: &Node{
			oid:      RootOID,
			parent:   RootOID,
			modeType: os.ModeDir,
			Name:     "/",
		},
	}
	dfs.root.fs = dfs
	dfs.og = newOidGenerator(dfs)

	dfs.hc, err = lru.New2QEvict(
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
	debug.Printf("Opening filesystem container %q", dfs.Name)
	ch, err := dfs.uh.OpenContainer(dfs.Name, daos.ContOpenRW)
	if err != nil {
		debug.Printf("Error opening %q: %s; attempting to create it.", dfs.Name, err)
		if err = dfs.uh.NewContainer(dfs.Name, ""); err != nil {
			return nil, errors.Wrapf(err, "Failed to create container %q", dfs.Name)
		}
		created = true
		if ch, err = dfs.uh.OpenContainer(dfs.Name, daos.ContOpenRW); err != nil {
			return nil, errors.Wrapf(err, "Failed to open container %q after create", dfs.Name)
		}
		debug.Printf("Created container for %q", dfs.Name)
	}
	dfs.ch = ch

	epoch, err := dfs.CurrentEpoch()
	if err != nil {
		return nil, err
	}

	if created {
		if _, err = dfs.DeclareObject(dfs.root.oid, daos.ClassTinyRW); err != nil {
			return nil, err
		}
		rootAttr := &Attr{
			Mode:  os.ModeDir | 0755,
			Uid:   uint32(os.Getuid()),
			Gid:   uint32(os.Getgid()),
			Mtime: time.Now(),
		}
		err = dfs.root.writeAttr(epoch, rootAttr)
	} else {
		_, err = dfs.OpenObject(dfs.root.oid)
	}

	return dfs, err
}

// NextEpoch queries the current highest committed epoch and returns
// that + 1
func (dfs *DaosFileSystem) NextEpoch() (daos.Epoch, error) {
	cur, err := dfs.CurrentEpoch()
	return cur + 1, err
}

// CurrentEpoch queries the current highest committed epoch
func (dfs *DaosFileSystem) CurrentEpoch() (daos.Epoch, error) {
	s, err := dfs.ch.EpochQuery()
	if err != nil {
		return 0, errors.Wrap(err, "Epoch query failed")
	}

	return s.HCE(), nil
}

// OpenObjectEpoch returns an open *daos.ObjectHandle for the given oid
// and epoch
func (dfs *DaosFileSystem) OpenObjectEpoch(oid *daos.ObjectID, epoch daos.Epoch) (*LockableObjectHandle, error) {
	start := time.Now()
	// TODO: Do we need to cache handles opened at different epochs? What
	// does that even mean? Does it matter if a handle is opened at
	// epoch N and then some update happens at N+2 (or N-3)?
	if oh, ok := dfs.hc.Get(oid); ok {
		debug.Printf("Retrieved handle for %s from cache in %s", oid, time.Since(start))
		return oh.(*LockableObjectHandle), nil
	}

	oh, err := dfs.ch.ObjectOpen(oid, epoch, daos.ObjOpenRW)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to open %s @ %s", oid, epoch)
	}
	debug.Printf("Opened handle for %s in %s", oid, time.Since(start))

	loh := &LockableObjectHandle{
		sync.RWMutex{},
		*oh,
	}

	dfs.hc.Add(oid, loh)
	return loh, nil
}

// OpenObject returns an open *daos.ObjectHandle for the given oid
// and current epoch
func (dfs *DaosFileSystem) OpenObject(oid *daos.ObjectID) (*LockableObjectHandle, error) {
	epoch, err := dfs.CurrentEpoch()
	if err != nil {
		return nil, err
	}

	return dfs.OpenObjectEpoch(oid, epoch)
}

// DeclareObjectEpoch first declares an object, then opens it and returns
// an open *daos.ObjectHandle for the given oid and epoch
func (dfs *DaosFileSystem) DeclareObjectEpoch(oid *daos.ObjectID, epoch daos.Epoch, oc daos.OClassID) (*LockableObjectHandle, error) {
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
	if err := dfs.ch.ObjectDeclare(oid, epoch, nil); err != nil {
		return nil, err
	}

	return dfs.OpenObjectEpoch(oid, epoch)
}

// DeclareObject first declares an object, then opens it and returns
// an open *daos.ObjectHandle for the given oid and current epoch
func (dfs *DaosFileSystem) DeclareObject(oid *daos.ObjectID, oc daos.OClassID) (*LockableObjectHandle, error) {
	epoch, err := dfs.CurrentEpoch()
	if err != nil {
		return nil, err
	}

	return dfs.DeclareObjectEpoch(oid, epoch, oc)
}

// CloseObject removes the object handle from cache and closes it
func (dfs *DaosFileSystem) CloseObject(oid *daos.ObjectID) {
	// handle is closed via evict callback
	dfs.hc.Remove(oid)
}

// Root returns the root node
func (dfs *DaosFileSystem) Root() *Node {
	return dfs.root
}

// Fini shuts everything down
func (dfs *DaosFileSystem) Fini() error {
	start := time.Now()
	dfs.hc.Purge()
	dfs.ch.Close()
	dfs.uh.Close()
	debug.Printf("Shutdown took %s", time.Since(start))
	return nil
}
