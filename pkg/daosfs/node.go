package daosfs

import (
	"encoding/binary"
	"encoding/json"
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
	Uid   uint32
	Gid   uint32
	// TODO: Implement other fields as necessary
}

// TODO: Some bigly refactoring needed here. Sad!

// MkdirRequest contains the information needed to complete a mkdir() request
type MkdirRequest struct {
	Uid  uint32
	Gid  uint32
	Name string
	Mode os.FileMode
}

// CreateRequest contains the information needed to complete a create() request
type CreateRequest struct {
	Uid   uint32
	Gid   uint32
	Flags uint32
	Name  string
	Mode  os.FileMode
}

// Node represents a file or directory stored in DAOS
type Node struct {
	fs       *DaosFileSystem
	oh       *daos.ObjectHandle
	oid      *daos.ObjectID
	parent   *daos.ObjectID
	modeType os.FileMode

	Name string
}

type DirEntry struct {
	oid      daos.ObjectID
	modeType os.FileMode
}

func (n *Node) openObjectLatest() error {
	return n.openObject(daos.EpochMax)
}

func (n *Node) openObject(e daos.Epoch) (err error) {
	// Don't open again if it's already open
	if n.oh != nil && !daos.HandleIsInvalid(n.oh) {
		return
	}

	debug.Printf("Opening oid %q @ %d", n.oid, e)
	n.oh, err = n.fs.ch.ObjectOpen(n.oid, e, daos.ObjOpenRW)
	if err != nil {
		err = errors.Wrapf(err, "Failed to open node oid %s", n.oid)
	}
	return
}

func (n *Node) closeObject() error {
	if n.oh == nil || daos.HandleIsInvalid(n.oh) {
		return nil
	}

	debug.Printf("Closing oid %q", n.oid)
	return n.oh.Close()
}

func (n *Node) getSize() (uint64, error) {
	if err := n.openObjectLatest(); err != nil {
		return 0, err
	}
	defer n.closeObject()

	val, err := n.oh.Get(daos.EpochMax, ".", "Size")
	if err != nil {
		return 0, errors.Wrapf(err, "Failed to fetch ./Size on %s", n.oid)
	}

	return binary.LittleEndian.Uint64(val), nil
}

func (n *Node) currentEpoch() daos.Epoch {
	return daos.EpochMax
}

func (n *Node) fetchEntry(name string) (*DirEntry, error) {
	epoch := n.currentEpoch()
	kv, err := n.oh.GetKeys(epoch, name, []string{"OID", "ModeType"})
	if err != nil {
		return nil, errors.Wrap(err, "GetKeys failed")
	}

	var dentry DirEntry

	if rawOID, ok := kv["OID"]; ok {
		// FIXME: Figure out how to bypass marshaling
		//oid := daos.ObjectID(rawOID)
		if err := json.Unmarshal(rawOID, &dentry.oid); err != nil {
			return nil, errors.Wrapf(err, "Failed to unmarshal %q", rawOID)
		}
	} else {
		return nil, errors.Errorf("Failed to fetch OID attr for %s", name)
	}

	if val, ok := kv["ModeType"]; ok {
		dentry.modeType = os.FileMode(binary.LittleEndian.Uint32(val))
	} else {
		return nil, errors.Errorf("Failed to fetch ModeType attr for %s", name)
	}

	return &dentry, nil
}

func (n *Node) writeEntry(epoch daos.Epoch, name string, dentry *DirEntry) error {
	kv := make(map[string][]byte)

	// FIXME: Don't marshal
	buf, err := json.Marshal(&dentry.oid)
	if err != nil {
		return errors.Wrapf(err, "Can't marshal %s", dentry.oid)
	}
	kv["OID"] = buf

	buf = make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(dentry.modeType))
	kv["ModeType"] = buf

	if err := n.oh.PutKeys(epoch, name, kv); err != nil {
		return errors.Wrapf(err, "Failed to store attrs to %s", name)
	}

	return nil
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

	if err := n.oh.PutKeys(epoch, ".", kv); err != nil {
		return errors.Wrap(err, "Failed to update child attrs")
	}
	return nil
}

// Attr retrieves the latest attributes for a node
func (n *Node) Attr() (*Attr, error) {
	if err := n.openObjectLatest(); err != nil {
		return nil, err
	}
	defer n.closeObject()

	da := &Attr{
		Inode: n.Inode(),
	}

	dkey := "."
	kv, err := n.oh.GetKeys(n.currentEpoch(), dkey, []string{"Size", "Mtime", "Mode", "Uid", "Gid"})
	if err != nil {
		return nil, errors.Wrapf(err, "get attrs for node %s", dkey)
	}

	for key := range kv {
		val := kv[key]
		switch string(key) {
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

// Children returns a slice of child *Nodes
func (n *Node) Children() ([]*Node, error) {
	var children []*Node

	debug.Printf("getting children of %s (%s)", n.oid, n.Name)
	if err := n.openObjectLatest(); err != nil {
		return nil, err
	}
	defer n.closeObject()

	var anchor daos.Anchor
	for !anchor.EOF() {
		keys, err := n.oh.DistKeys(daos.EpochMax, &anchor)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to fetch dkeys for %s", n.oid)
		}
		debug.Printf("fetched %d keys for %s @ epoch %d", len(keys), n.oid, daos.EpochMax)

		chunk := make([]*Node, 0, len(keys))
		for i := range keys {
			// Skip the attributes dkey
			if string(keys[i]) == "." {
				continue
			}
			entry, err := n.fetchEntry(string(keys[i]))
			if err != nil {
				return nil, errors.Wrapf(err, "Failed to fetch entry for %s", keys[i])
			}

			chunk = append(chunk, &Node{
				oid:      &entry.oid,
				parent:   n.oid,
				fs:       n.fs,
				modeType: entry.modeType,
				Name:     string(keys[i]),
			})
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
	if err := n.openObjectLatest(); err != nil {
		return nil, err
	}
	defer n.closeObject()

	entry, err := n.fetchEntry(name)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to fetch entry for %s", name)
	}
	debug.Printf("%s: entry %#v", name, entry)
	return &Node{
		oid:      &entry.oid,
		parent:   n.oid,
		fs:       n.fs,
		modeType: entry.modeType,
		Name:     name,
	}, nil
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

	if err := n.openObject(epoch); err != nil {
		return nil, err
	}
	defer n.closeObject()

	nextOID, err := n.fs.og.Next(daos.ClassTinyRW)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to get next OID in Mkdir")
	}

	err = n.writeEntry(epoch, name, &DirEntry{*nextOID, os.FileMode(mode & os.ModeType)})
	if err != nil {
		return nil, errors.Wrap(err, "Failed to write entry")
	}

	child := &Node{
		oid:    nextOID,
		parent: n.oid,
		fs:     n.fs,
		Name:   name,
	}
	if err := child.openObject(epoch); err != nil {
		return nil, err
	}
	defer child.closeObject()
	debug.Printf("Created new child object %s", child)

	attr := &Attr{
		Mode:  os.FileMode(mode),
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

// Create attempts to create a new child node and returns a filehandle to it
func (n *Node) Create(req *CreateRequest) (*Node, *FileHandle, error) {
	child, err := n.createChild(req.Uid, req.Gid, req.Mode, req.Name)

	return child, &FileHandle{node: child, Flags: req.Flags}, err
}

// Open returns a filehandle
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

	if err := n.openObject(epoch); err != nil {
		return err
	}
	defer n.closeObject()
	if err := child.openObject(epoch); err != nil {
		return err
	}
	defer child.closeObject()

	// Apparently Punch() is not implemented yet...
	if err := child.oh.Punch(epoch); err != nil {
		return errors.Wrapf(err, "Object punch failed on %s", child.oid)
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
