package daosfs

import (
	"encoding/binary"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/daos-stack/go-daos/pkg/daos"
	"github.com/intel-hpdd/logging/debug"
	"github.com/pkg/errors"
)

// Attr represents a Node's file attributes
type Attr struct {
	Inode uint64 // fuse only handles a uint64
	Size  uint64
	Mtime time.Time
	Mode  os.FileMode
	Uid   uint32 // nolint
	Gid   uint32 // nolint
	// TODO: Implement other fields as necessary
}

// TODO: Some bigly refactoring needed here. Sad!

// MkdirRequest contains the information needed to complete a mkdir() request
type MkdirRequest struct {
	Uid  uint32 // nolint
	Gid  uint32 // nolint
	Mode os.FileMode
	Name string
}

// CreateRequest contains the information needed to complete a create() request
type CreateRequest struct {
	Uid   uint32 // nolint
	Gid   uint32 // nolint
	Flags uint32
	Mode  os.FileMode
	Name  string
}

// Node represents a file or directory stored in DAOS
type Node struct {
	fs       *DaosFileSystem
	oid      *daos.ObjectID
	parent   *daos.ObjectID
	modeType os.FileMode

	Name string
}

// DirEntry holds information about the child of a directory Node
type DirEntry struct {
	Name string
	Oid  *daos.ObjectID
	Type os.FileMode
}

// Inode returns lowest 64 bits of Oid
func (entry *DirEntry) Inode() uint64 {
	return entry.Oid.Lo()
}

// Not really happy about this, but we need to be able to prevent a cached
// open object handle from being purged/closed while it's in use.
func (n *Node) oh() (*LockableObjectHandle, error) {
	return n.fs.OpenObject(n.oid)
}

func (n *Node) withWriteHandle(fn func(oh *LockableObjectHandle) error) error {
	oh, err := n.oh()
	if err != nil {
		return err
	}
	oh.Lock()
	defer oh.Unlock()

	return fn(oh)
}

func (n *Node) withReadHandle(fn func(oh *LockableObjectHandle) (interface{}, error)) (interface{}, error) {
	oh, err := n.oh()
	if err != nil {
		return nil, err
	}
	oh.RLock()
	defer oh.RUnlock()

	return fn(oh)
}

func (n *Node) getSize() (uint64, error) {
	size, err := n.withReadHandle(func(oh *LockableObjectHandle) (interface{}, error) {
		val, err := oh.Get(daos.EpochMax, ".", "Size")
		if err != nil {
			return 0, errors.Wrapf(err, "Failed to fetch ./Size on %s", n.oid)
		}

		return binary.LittleEndian.Uint64(val), nil
	})

	return size.(uint64), err
}

func (n *Node) currentEpoch() daos.Epoch {
	return daos.EpochMax
}

func (n *Node) fetchEntry(name string) (*DirEntry, error) {
	epoch := n.currentEpoch()
	kvi, err := n.withReadHandle(func(oh *LockableObjectHandle) (interface{}, error) {
		kv, err := oh.GetKeys(epoch, name, []string{"OID", "ModeType"})
		return kv, errors.Wrap(err, "GetKeys failed")
	})
	if err != nil {
		return nil, err
	}
	kv, ok := kvi.(map[string][]byte)
	if !ok {
		return nil, errors.Errorf("Failed to type-assert %v!?", kvi)
	}

	var dentry DirEntry
	dentry.Name = name

	dentry.Oid = &daos.ObjectID{}
	if rawOID, ok := kv["OID"]; ok {
		if err := dentry.Oid.UnmarshalBinary(rawOID); err != nil {
			return nil, errors.Wrapf(err, "Failed to unmarshal %v", rawOID)
		}
	} else {
		return nil, errors.Errorf("Failed to fetch OID attr for %s", name)
	}

	if val, ok := kv["ModeType"]; ok {
		dentry.Type = os.FileMode(binary.LittleEndian.Uint32(val))
	} else {
		return nil, errors.Errorf("Failed to fetch ModeType attr for %s", name)
	}

	return &dentry, nil
}

func (n *Node) writeEntry(epoch daos.Epoch, name string, dentry *DirEntry) error {
	kv := make(map[string][]byte)

	buf, err := dentry.Oid.MarshalBinary()
	if err != nil {
		return errors.Wrapf(err, "Can't marshal %s", dentry.Oid)
	}
	kv["OID"] = buf

	buf = make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(dentry.Type))
	kv["ModeType"] = buf

	return n.withWriteHandle(func(oh *LockableObjectHandle) error {
		return errors.Wrapf(oh.PutKeys(epoch, name, kv),
			"Failed to store attrs to %s", name)
	})
}

func (n *Node) writeAttr(epoch daos.Epoch, attr *Attr) error {
	kv := make(map[string][]byte)

	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(attr.Mode))
	kv["Mode"] = buf

	buf = make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, attr.Uid)
	kv["Uid"] = buf

	buf = make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, attr.Gid)
	kv["Gid"] = buf

	buf = make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, attr.Size)
	kv["Size"] = buf

	mtime, err := attr.Mtime.MarshalBinary()
	if err != nil {
		return err
	}
	kv["Mtime"] = mtime

	return n.withWriteHandle(func(oh *LockableObjectHandle) error {
		return errors.Wrapf(oh.PutKeys(epoch, ".", kv),
			"Failed to update child attrs")
	})
}

// Attr retrieves the latest attributes for a node
func (n *Node) Attr() (*Attr, error) {
	da := &Attr{
		Inode: n.Inode(),
	}

	dkey := "."
	kvi, err := n.withReadHandle(func(oh *LockableObjectHandle) (interface{}, error) {
		kv, err := oh.GetKeys(n.currentEpoch(), dkey, []string{"Size", "Mtime", "Mode", "Uid", "Gid"})
		return kv, errors.Wrapf(err, "get attrs for node %s", dkey)
	})
	if err != nil {
		return nil, err
	}
	kv, ok := kvi.(map[string][]byte)
	if !ok {
		return nil, errors.Errorf("Failed to type-assert %v?!", kvi)
	}

	for key := range kv {
		val := kv[key]
		switch key {
		case "Size":
			da.Size = binary.LittleEndian.Uint64(val)
		case "Mtime":
			if err := da.Mtime.UnmarshalBinary(val); err != nil {
				return nil, errors.Wrap(err, "Unable to decode Mtime")
			}
		case "Mode":
			da.Mode = os.FileMode(binary.LittleEndian.Uint32(val))
		case "Uid":
			da.Uid = binary.LittleEndian.Uint32(val)
		case "Gid":
			da.Gid = binary.LittleEndian.Uint32(val)
		}
	}

	return da, nil
}

func (n *Node) String() string {
	return fmt.Sprintf("oid: %s, parent: %s, name: %s", n.oid, n.parent, n.Name)
}

// Type returns the node type (ModeDir, ModeSymlink, etc)
func (n *Node) Type() os.FileMode {
	return n.modeType
}

// Inode returns the node's Inode representation
func (n *Node) Inode() uint64 {
	// Fuse only supports uint64 Inodes, so just use the bottom part
	// of the OID
	return n.oid.Lo()
}

// Children returns a slice of *DirEntry
func (n *Node) Children() ([]*DirEntry, error) {
	var children []*DirEntry

	debug.Printf("getting children of %s (%s)", n.oid, n.Name)
	var anchor daos.Anchor
	for !anchor.EOF() {
		ki, err := n.withReadHandle(func(oh *LockableObjectHandle) (interface{}, error) {
			keys, err := oh.DistKeys(daos.EpochMax, &anchor)
			return keys, errors.Wrapf(err, "Failed to fetch dkeys for %s", n.oid)
		})
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to fetch dkeys for %s", n.oid)
		}
		keys := ki.([][]byte)
		debug.Printf("fetched %d keys for %s @ epoch %d", len(keys), n.oid, daos.EpochMax)

		chunk := make([]*DirEntry, 0, len(keys))
		for i := range keys {
			// Skip the attributes dkey
			if string(keys[i]) == "." {
				continue
			}
			entry, err := n.fetchEntry(string(keys[i]))
			if err != nil {
				return nil, errors.Wrapf(err, "Failed to fetch entry for %s", keys[i])
			}

			chunk = append(chunk, entry)
			debug.Printf("child: %s", chunk[len(chunk)-1])
		}

		if len(chunk) > 0 {
			children = append(children, chunk...)
		}
	}

	debug.Printf("Found %d children of %s (%s)", len(children), n.oid, n.Name)
	return children, nil
}

// Lookup attempts to find the object associated with the name and
// returns a *daos.Node if found
func (n *Node) Lookup(name string) (*Node, error) {
	debug.Printf("looking up %s in %s", name, n.oid)

	entry, err := n.fetchEntry(name)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to fetch entry for %s", name)
	}
	debug.Printf("%s: entry %#v", name, entry)
	child := Node{
		oid:      entry.Oid,
		parent:   n.oid,
		fs:       n.fs,
		modeType: entry.Type,
		Name:     name,
	}

	return &child, nil
}

func (n *Node) createChild(uid, gid uint32, mode os.FileMode, name string) (*Node, error) {
	if child, _ := n.Lookup(name); child != nil {
		debug.Printf("In Mkdir(): %s already exists!", name)
		return nil, syscall.EEXIST
	}
	debug.Printf("Creating %s/%s", n.Name, name)

	epoch, err := n.fs.ch.EpochHold(0)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to hold epoch")
	}
	tx := n.fs.ch.EpochDiscard
	defer func() {
		tx(epoch)
	}()

	// TODO: We may need to find a better default for non-directory
	// objects. In fact, we may need to evaluate what the best class
	// is for directories, given that there may be implications for
	// directories with lots of entries (i.e. lots of dkeys).
	objClass := daos.ClassLargeRW
	if mode.IsDir() {
		objClass = daos.ClassTinyRW
	}
	nextOID, err := n.fs.og.Next(objClass)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to get next OID in Mkdir")
	}

	err = n.writeEntry(epoch, name, &DirEntry{name, nextOID, mode & os.ModeType})
	if err != nil {
		return nil, errors.Wrap(err, "Failed to write entry")
	}

	child := &Node{
		oid:    nextOID,
		parent: n.oid,
		fs:     n.fs,
		Name:   name,
	}
	if _, err = n.fs.DeclareObjectEpoch(child.oid, epoch, objClass); err != nil {
		return nil, err
	}
	debug.Printf("Created new child object %s", child)

	attr := &Attr{
		Mode:  mode,
		Uid:   uid,
		Gid:   gid,
		Mtime: time.Now(),
	}

	err = child.writeAttr(epoch, attr)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to write attributes")
	}

	debug.Printf("Successfully created %s/%s", n.Name, child.Name)
	tx = n.fs.ch.EpochCommit

	return child, nil
}

// Mkdir attempts to create a new child Node
func (n *Node) Mkdir(req *MkdirRequest) (*Node, error) {
	return n.createChild(req.Uid, req.Gid, req.Mode, req.Name)
}

// Create attempts to create a new child node and returns a fileoh to it
func (n *Node) Create(req *CreateRequest) (*Node, *FileHandle, error) {
	child, err := n.createChild(req.Uid, req.Gid, req.Mode, req.Name)

	return child, &FileHandle{node: child, Flags: req.Flags}, err
}

// Open returns a fileoh
func (n *Node) Open(flags uint32) (*FileHandle, error) {
	return NewFileHandle(n, flags), nil
}

func (n *Node) destroyChild(child *Node) error {
	epoch, err := n.fs.ch.EpochHold(0)
	if err != nil {
		return errors.Wrap(err, "Unable to hold epoch")
	}
	tx := n.fs.ch.EpochDiscard
	defer func() {
		tx(epoch)
	}()

	err = child.withWriteHandle(func(oh *LockableObjectHandle) error {
		return errors.Wrapf(oh.Punch(epoch),
			"Object punch failed on %s", child.oid)
	})
	if err != nil {
		return err
	}

	tx = n.fs.ch.EpochCommit

	// FIXME: This is broken. Need a fix from DAOS so that we can
	// actually delete a dkey.
	return nil
}

// Remove punches the object and ... ?
func (n *Node) Remove(name string, dir bool) error {
	child, err := n.Lookup(name)
	if err != nil {
		return err
	}

	switch child.Type() {
	case os.ModeDir:
		if !dir {
			return syscall.EISDIR
		}
		children, err := child.Children()
		if err != nil {
			return err
		}
		if len(children) != 0 {
			return syscall.ENOTEMPTY
		}
	default:
		if dir {
			return syscall.ENOTDIR
		}
	}

	return n.destroyChild(child)
}
