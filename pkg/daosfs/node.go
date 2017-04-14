package daosfs

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/daos-stack/go-daos/pkg/daos"
	"github.com/intel-hpdd/logging/debug"
	"github.com/pkg/errors"
)

const (
	// WriteAttrMode indicates that Mode was set
	WriteAttrMode = 1 << iota
	// WriteAttrUID indicates that UID was set
	WriteAttrUID
	// WriteAttrGID indicates that GID was set
	WriteAttrGID
	// WriteAttrMtime indicates that Mtime was set
	WriteAttrMtime
	// WriteAttrAtime indicates that Atime was set
	WriteAttrAtime
	// WriteAttrSize indicates that Size was set
	WriteAttrSize
	// WriteAttrCtime indicates that Ctime was set
	WriteAttrCtime
	// WriteAttrAll indicates that all known fields were set
	WriteAttrAll = 0xFF
)

// Attr represents a Node's file attributes
type Attr struct {
	Device  uint32
	Inode   uint64 // fuse only handles a uint64
	Mode    os.FileMode
	Nlink   uint64
	Uid     uint32 // nolint
	Gid     uint32 // nolint
	Rdev    uint64
	Size    int64
	Blksize int64
	Blocks  int64
	Atime   time.Time
	Mtime   time.Time
	Ctime   time.Time
}

// CMode converts the os.FileMode value into a C.mode_t suitable for
// use as st_mode in a C.struct_stat.
func (a *Attr) CMode() uint32 {
	out := uint32(a.Mode) & 0777
	switch {
	default:
		out |= syscall.S_IFREG
	case a.Mode&os.ModeDir != 0:
		out |= syscall.S_IFDIR
	case a.Mode&os.ModeDevice != 0:
		if a.Mode&os.ModeCharDevice != 0 {
			out |= syscall.S_IFCHR
		} else {
			out |= syscall.S_IFBLK
		}
	case a.Mode&os.ModeNamedPipe != 0:
		out |= syscall.S_IFIFO
	case a.Mode&os.ModeSymlink != 0:
		out |= syscall.S_IFLNK
	case a.Mode&os.ModeSocket != 0:
		out |= syscall.S_IFSOCK
	}
	if a.Mode&os.ModeSetuid != 0 {
		out |= syscall.S_ISUID
	}
	if a.Mode&os.ModeSetgid != 0 {
		out |= syscall.S_ISGID
	}

	return out
}

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
	fs        *FileSystem
	modeType  os.FileMode
	readEpoch *daos.Epoch

	FileHandle *FileHandle
	Parent     *Node
	Oid        *daos.ObjectID
	Name       string
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
	return n.fs.OpenObject(n.Oid)
}

func (n *Node) withWriteHandle(fn func(oh *LockableObjectHandle) error) error {
	if n.IsSnapshot() {
		return unix.EPERM
	}
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

// GetSize returns the current Size attribute
// FIXME: It would probably be better to make a more flexible version of
// GetAttr() to specify desired attributes by mask or whatever.
func (n *Node) GetSize() (int64, error) {
	size, err := n.withReadHandle(func(oh *LockableObjectHandle) (interface{}, error) {
		val, err := oh.Get(n.Epoch(), ".", "Size")
		if err != nil {
			return 0, errors.Wrapf(err, "Failed to fetch ./Size on %s", n.Oid)
		}

		return binary.LittleEndian.Uint64(val), nil
	})

	val := size.(uint64)
	if int64(val) < 0 {
		return 0, errors.Errorf("%d overflows int64", val)
	}

	return int64(val), err
}

// IsSnapshot is true if the Node refers to snapshot version of a object
func (n *Node) IsSnapshot() bool {
	return n.readEpoch != nil
}

// Epoch returns the node's read epoch; either a snapshot or HCE.
func (n *Node) Epoch() daos.Epoch {
	if n.readEpoch != nil {
		return *n.readEpoch
	}
	return n.fs.GetReadEpoch()
}

func (n *Node) fetchEntry(name string) (*DirEntry, error) {
	epoch := n.Epoch()
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

func (n *Node) writeAttr(epoch daos.Epoch, attr *Attr, mask uint32) error {
	kv := make(map[string][]byte)

	if mask&WriteAttrMode != 0 {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(attr.Mode))
		kv["Mode"] = buf
	}

	if mask&WriteAttrUID != 0 {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, attr.Uid)
		kv["Uid"] = buf
	}

	if mask&WriteAttrGID != 0 {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, attr.Gid)
		kv["Gid"] = buf
	}

	if mask&WriteAttrSize != 0 {
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(attr.Size))
		kv["Size"] = buf
	}

	if mask&WriteAttrAtime != 0 {
		atime, err := attr.Atime.MarshalBinary()
		if err != nil {
			return err
		}
		kv["Atime"] = atime
	}

	if mask&WriteAttrMtime != 0 {
		mtime, err := attr.Mtime.MarshalBinary()
		if err != nil {
			return err
		}
		kv["Mtime"] = mtime
	}

	if mask&WriteAttrCtime != 0 {
		ctime, err := attr.Ctime.MarshalBinary()
		if err != nil {
			return err
		}
		kv["Ctime"] = ctime
	}

	return n.withWriteHandle(func(oh *LockableObjectHandle) error {
		return errors.Wrapf(oh.PutKeys(epoch, ".", kv),
			"Failed to update child attrs")
	})
}

// SetAttr sets attributes for a node
func (n *Node) SetAttr(attr *Attr, mask uint32) error {
	tx, err := n.fs.GetWriteTransaction()
	if err != nil {
		return err
	}
	defer func() {
		tx.Complete()
	}()

	err = n.writeAttr(tx.Epoch, attr, mask)
	if err != nil {
		return errors.Wrap(err, "Failed to write attributes")
	}

	tx.Commit()
	return nil
}

// GetAttr retrieves the latest attributes for a node
func (n *Node) GetAttr() (*Attr, error) {
	da := &Attr{
		Inode:  n.Inode(),
		Device: n.fs.Device(),
	}

	dkey := "."
	kvi, err := n.withReadHandle(func(oh *LockableObjectHandle) (interface{}, error) {
		kv, err := oh.GetKeys(n.Epoch(), dkey, []string{"Size", "Atime", "Mtime", "Ctime", "Mode", "Uid", "Gid"})
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
			size := binary.LittleEndian.Uint64(val)
			if int64(size) < 0 {
				return nil, errors.Errorf("%d overflows int64", size)
			}
			da.Size = int64(size)
		case "Atime":
			if err := da.Atime.UnmarshalBinary(val); err != nil {
				return nil, errors.Wrap(err, "Unable to decode Atime")
			}
		case "Mtime":
			if err := da.Mtime.UnmarshalBinary(val); err != nil {
				return nil, errors.Wrap(err, "Unable to decode Mtime")
			}
		case "Ctime":
			if err := da.Ctime.UnmarshalBinary(val); err != nil {
				return nil, errors.Wrap(err, "Unable to decode Ctime")
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

// Getxattr returns the value of the node's extended attribute for the
// given name
func (n *Node) Getxattr(name string) ([]byte, error) {
	debug.Printf("getxattr %s[%s]", n.Name, name)
	switch name {
	case "user.snapshot_epoch":
		if n.readEpoch != nil {
			s := fmt.Sprintf("%d", *n.readEpoch)
			debug.Printf("epoch %s %d %s", n.Name, *n.readEpoch, s)
			return []byte(s), nil
		}
	}
	return nil, ErrNoXattr
}

// Listxattr returns a slice of extended attribute names for the node
func (n *Node) Listxattr() ([]string, error) {
	var attrs []string
	if n.readEpoch != nil {
		debug.Printf("listxattr %s", n.Name)
		attrs = append(attrs, "user.snapshot_epoch")
	}
	return attrs, nil
}

// Setxattr sets a node's extended attribute to the given value
func (n *Node) Setxattr(name string, value []byte, flags uint32) error {
	debug.Printf("setxattr %s[%s] =  %s", n.Name, name, value)
	switch name {
	case "user.snapshot_epoch":
		e, err := strconv.ParseUint(string(value), 0, 64)
		if err != nil {
			return errors.Wrap(err, "parse number")
		}
		n.readEpoch = (*daos.Epoch)(&e)
	default:
		return unix.ENOSYS
	}
	return nil
}

// Removexattr removes a node's extended attribute for the given name
func (n *Node) Removexattr(name string) error {
	debug.Printf("remove xattr %s[%s])", n.Name, name)

	switch name {
	case "user.snapshot_epoch":
		n.readEpoch = nil
	default:
		return ErrNoXattr
	}
	return nil
}

func (n *Node) String() string {
	return fmt.Sprintf("oid: %s, parent: %s, name: %s", n.Oid, n.Parent.Oid, n.Name)
}

// Type returns the node type (ModeDir, ModeSymlink, etc)
func (n *Node) Type() os.FileMode {
	return n.modeType
}

// Inode returns the node's Inode representation
func (n *Node) Inode() uint64 {
	// Fuse only supports uint64 Inodes, so just use the bottom part
	// of the OID
	return n.Oid.Lo()
}

// Children returns a slice of *DirEntry
func (n *Node) Children() ([]*DirEntry, error) {
	var children []*DirEntry

	if !n.Type().IsDir() {
		return nil, unix.ENOTDIR
	}

	epoch := n.Epoch()
	debug.Printf("getting children of %s (%s)", n.Oid, n.Name)
	var anchor daos.Anchor
	for !anchor.EOF() {
		ki, err := n.withReadHandle(func(oh *LockableObjectHandle) (interface{}, error) {
			keys, err := oh.DistKeys(epoch, &anchor)
			return keys, errors.Wrapf(err, "Failed to fetch dkeys for %s", n.Oid)
		})
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to fetch dkeys for %s", n.Oid)
		}
		keys := ki.([][]byte)
		debug.Printf("fetched %d keys for %s @ epoch %d", len(keys), n.Oid, epoch)

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

	debug.Printf("Found %d children of %s (%s)", len(children), n.Oid, n.Name)
	return children, nil
}

// IsRoot indicates whether or not the node is the root node of the filesystem
func (n *Node) IsRoot() bool {
	return n.Oid == n.Parent.Oid
}

// Lookup attempts to find the object associated with the name and
// returns a *daos.Node if found
func (n *Node) Lookup(name string) (*Node, error) {
	debug.Printf("looking up %s under %q (%s)", name, n.Name, n.Oid)
	if !n.Type().IsDir() {
		return nil, syscall.ENOTDIR
	}

	name = filepath.Clean(name)
	if n.IsRoot() {
		if name == ".." || name == "/" {
			return n, nil
		}
	}

	dir, file := filepath.Split(name)
	if dir == "" || dir == "/" || dir == "." {
		entry, err := n.fetchEntry(file)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to fetch entry for %s", file)
		}
		debug.Printf("%s: entry %#v", file, entry)
		child := Node{
			Oid:       entry.Oid,
			Parent:    n,
			fs:        n.fs,
			modeType:  entry.Type,
			readEpoch: n.readEpoch,
			Name:      file,
		}

		return &child, nil
	}

	if dir == ".." {
		return n.Parent.Lookup(file)
	}

	// First, walk down the list of subdirectories to find the parent,
	// then lookup the desired node under that parent.
	// TODO: Maybe a path->Node LRU cache is necessary for better
	// performance here.
	node := n
	var err error
	for _, subdir := range strings.Split(filepath.Clean(dir), "/") {
		if subdir == "" {
			continue
		}
		if node, err = node.Lookup(subdir); err != nil {
			return nil, err
		}
	}
	return node.Lookup(file)
}

func (n *Node) createChild(uid, gid uint32, mode os.FileMode, name string) (*Node, error) {
	if !n.Type().IsDir() {
		return nil, syscall.ENOTDIR
	}

	if child, _ := n.Lookup(name); child != nil {
		debug.Printf("In Mkdir(): %s already exists!", name)
		return nil, unix.EEXIST
	}
	debug.Printf("Creating %s/%s", n.Name, name)

	tx, err := n.fs.GetWriteTransaction()
	if err != nil {
		return nil, err
	}
	defer func() {
		tx.Complete()
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

	err = n.writeEntry(tx.Epoch, name, &DirEntry{name, nextOID, mode & os.ModeType})
	if err != nil {
		return nil, errors.Wrap(err, "Failed to write entry")
	}

	child := &Node{
		Oid:      nextOID,
		Parent:   n,
		fs:       n.fs,
		modeType: mode & os.ModeType,
		Name:     name,
	}
	if _, err = n.fs.DeclareObjectEpoch(child.Oid, tx.Epoch, objClass); err != nil {
		return nil, err
	}
	debug.Printf("Created new child object %s", child)

	now := time.Now()
	attr := &Attr{
		Mode:  mode,
		Uid:   uid,
		Gid:   gid,
		Atime: now,
		Mtime: now,
		Ctime: now,
	}

	err = child.writeAttr(tx.Epoch, attr, WriteAttrAll)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to write attributes")
	}

	debug.Printf("Successfully created %s/%s", n.Name, child.Name)
	tx.Commit()

	return child, nil
}

// Mkdir attempts to create a new child directory
func (n *Node) Mkdir(req *MkdirRequest) (*Node, error) {
	return n.createChild(req.Uid, req.Gid, req.Mode, req.Name)
}

// Create attempts to create a new child file
func (n *Node) Create(req *CreateRequest) (*Node, error) {
	debug.Printf("Create(%v)", req)
	child, err := n.createChild(req.Uid, req.Gid, req.Mode, req.Name)
	if err != nil {
		return nil, err
	}
	return child, child.Open(req.Flags)
}

// Open creates a filehandle if the node is a regular file
func (n *Node) Open(flags uint32) error {
	debug.Printf("Open(0x%x) (%b)", flags, flags)
	if n.Type().IsDir() {
		if flags&syscall.O_DIRECTORY == 0 {
			return syscall.EISDIR
		}
		return nil
	}

	if n.FileHandle != nil {
		if n.FileHandle.Flags == flags {
			return nil
		}
		debug.Printf("Reopen? cur 0x%x -> new 0x%x", n.FileHandle.Flags, flags)
	}

	// FIXME: All filehandles should be able to read, so get rid of
	// read/readwrite handles. The only distinction is whether or
	// not the thing can write because it has a tx.
	if flags&(syscall.O_RDWR|syscall.O_WRONLY|syscall.O_APPEND) != 0 {
		if n.IsSnapshot() {
			return errors.New("Can't write to snapshot")
		}
		if n.FileHandle != nil && n.FileHandle.CanWrite() {
			// Don't get another write tx.
			return nil
		}
		tx, err := n.fs.GetWriteTransaction()
		if err != nil {
			return err
		}
		if flags&syscall.O_WRONLY != 0 {
			n.FileHandle = NewWriteHandle(n, tx, flags)
			return nil
		}
		n.FileHandle = NewReadWriteHandle(n, tx, flags)
		return nil
	}

	readEpoch := n.Epoch()
	n.FileHandle = NewReadHandle(n, &readEpoch, flags)

	return nil
}

// IsOpen indicates whether or not the node has been opened as a file
func (n *Node) IsOpen() bool {
	return n.FileHandle != nil
}

// Close closes the node's filehandle
func (n *Node) Close() (err error) {
	if n.FileHandle == nil {
		// TODO: Should this actually be an error?
		return errors.New("Close() on nil filehandle")
	}

	if err = n.FileHandle.Close(); err == nil {
		n.FileHandle = nil
	}
	return
}

func (n *Node) destroyChild(child *Node) error {
	tx, err := n.fs.GetWriteTransaction()
	if err != nil {
		return err
	}
	defer func() {
		tx.Complete()
	}()

	err = child.withWriteHandle(func(oh *LockableObjectHandle) error {
		return errors.Wrapf(oh.Punch(tx.Epoch),
			"Object punch failed on %s", child.Oid)
	})
	if err != nil {
		return err
	}

	tx.Commit()

	// FIXME: This is broken. Need a fix from DAOS so that we can
	// actually delete a dkey.
	return nil
}

// Unlink removes a file
func (n *Node) Unlink(name string) error {
	child, err := n.Lookup(name)
	if err != nil {
		return err
	}

	if child.Type().IsDir() {
		return unix.EISDIR
	}

	return n.destroyChild(child)
}

// Rmdir removes a directory
func (n *Node) Rmdir(name string) error {
	child, err := n.Lookup(name)
	if err != nil {
		return err
	}

	if !child.Type().IsDir() {
		return unix.ENOTDIR
	}

	children, err := child.Children()
	if err != nil {
		return err
	}
	if len(children) != 0 {
		return unix.ENOTEMPTY
	}

	return n.destroyChild(child)
}
