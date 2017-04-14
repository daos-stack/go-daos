package main

// #cgo LDFLAGS:  -ldaos  -lcrt_util -lcrt -ldaos_common -ldaos_tier -ldaos_array -luuid
// #include <stdlib.h>
// #include <sys/stat.h>
// #include <daosfs_types.h>
//
// /* FIXME: Dumb hack to avoid unused-function warning when building ganesha */
// #ifndef GANESHA_VERSION
// static bool invoke_readdir_cb(daosfs_readdir_cb cb, char *name, void *arg, uint64_t offset) {
//     return cb(name, arg, offset);
// }
// #endif
import "C"
import (
	"encoding/binary"
	"hash/fnv"
	"os"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/daos-stack/go-daos/pkg/daos"
	"github.com/daos-stack/go-daos/pkg/daosfs"
	"github.com/intel-hpdd/logging/debug"
)

// This map is kind of a kludge, but it's necessary to avoid
// GC problems. The rules of cgo (1) specify that pointers passed
// between Go and C may not contain pointers. Basically we just keep
// refs to Go-allocated things as map values so that the GC doesn't
// mark them for collection.
//
// 1. https://golang.org/cmd/cgo/#hdr-Passing_pointers
type refMap struct {
	sync.RWMutex
	h2r  map[uint64]interface{}
	refs map[uint64]int
}

var refs = &refMap{
	h2r:  map[uint64]interface{}{},
	refs: map[uint64]int{},
}

func (rm *refMap) Get(hash uint64) (interface{}, bool) {
	rm.Lock()
	defer rm.Unlock()

	ref, found := rm.h2r[hash]
	if found {
		rm.refs[hash]++
	}
	return ref, found
}

func (rm *refMap) Set(ref interface{}) uint64 {
	rm.Lock()
	defer rm.Unlock()

	hash := fnv.New64()
	switch o := ref.(type) {
	case *daosfs.Node:
		hash.Write([]byte("daosfs-node"))
		inode := make([]byte, 8)
		binary.LittleEndian.PutUint64(inode, o.Inode())
		hash.Write(inode)
	case *daosfs.FileSystem:
		hash.Write([]byte("daosfs-filesystem"))
		hash.Write([]byte(o.Name))
	default:
		panic("Unknown interface type given to refMap.Set()")
	}

	h := hash.Sum64()
	if _, found := rm.h2r[h]; !found {
		rm.h2r[h] = ref
	}
	rm.refs[h]++

	return h
}

func (rm *refMap) Delete(hash uint64) {
	rm.Lock()
	defer rm.Unlock()

	if _, found := rm.h2r[hash]; found {
		rm.refs[hash]--
	} else {
		return
	}

	if rm.refs[hash] <= 0 {
		debug.Printf("Deleting ref %v from ref map", rm.h2r[hash])
		delete(rm.refs, hash)
		delete(rm.h2r, hash)
	}
}

func getFs(cFs *C.struct_daosfs_fs_handle) (*daosfs.FileSystem, error) {
	if cFs == nil {
		debug.Print("Got nil fs handle")
		return nil, syscall.EINVAL
	}

	hash := uint64(cFs.fs_ptr)
	ref, found := refs.Get(hash)
	if !found {
		debug.Printf("No fs ref found for %d", hash)
		return nil, syscall.ENOENT
	}

	fs, ok := ref.(*daosfs.FileSystem)
	if !ok {
		debug.Printf("%d does not refer to a *daosfs.FileSystem", hash)
	}

	return fs, nil
}

func getNode(cNh *C.struct_daosfs_node_handle) (*daosfs.Node, error) {
	if cNh == nil {
		debug.Print("Got nil node handle")
		return nil, syscall.EINVAL
	}

	hash := uint64(cNh.node_ptr)
	ref, found := refs.Get(hash)
	if !found {
		debug.Printf("No node ref found for %d", hash)
		return nil, syscall.ENOENT
	}

	node, ok := ref.(*daosfs.Node)
	if !ok {
		debug.Printf("%d does not refer to a *daosfs.Node", hash)
	}

	return node, nil
}

func err2rc(err error) C.int {
	if err == nil {
		return 0
	}

	switch err := err.(type) {
	case syscall.Errno:
		return -C.int(err)
	default:
		debug.Printf("Unknown error: %s", err)
		return -C.int(syscall.EFAULT)
	}
}

func st2fm(cMode C.__mode_t) (mode os.FileMode) {
	// TODO: See if there is a more portable way of doing this.
	// This is lifted from os/stat_linux.go
	mode = os.FileMode(uint32(cMode) & 0777)
	switch uint32(cMode) & syscall.S_IFMT {
	case syscall.S_IFBLK:
		mode |= os.ModeDevice
	case syscall.S_IFCHR:
		mode |= os.ModeDevice | os.ModeCharDevice
	case syscall.S_IFDIR:
		mode |= os.ModeDir
	case syscall.S_IFIFO:
		mode |= os.ModeNamedPipe
	case syscall.S_IFLNK:
		mode |= os.ModeSymlink
	case syscall.S_IFREG:
		// nothing to do
	case syscall.S_IFSOCK:
		mode |= os.ModeSocket
	}
	if uint32(cMode)&syscall.S_ISGID != 0 {
		mode |= os.ModeSetgid
	}
	if uint32(cMode)&syscall.S_ISUID != 0 {
		mode |= os.ModeSetuid
	}
	if uint32(cMode)&syscall.S_ISVTX != 0 {
		mode |= os.ModeSticky
	}
	return
}

func main() {}

// LibDaosFileSystemInit initializes the DAOS libraries. It should only be
// called once per invocation of main(). The supplied opaque library handle
// is initialized here and freed in LibDaosFileSystemFini().
//export LibDaosFileSystemInit
func LibDaosFileSystemInit(dfs *C.daosfs_t) C.int {
	if *dfs != nil {
		debug.Print("Got non-nil pointer in OpenDaosFileSystem()")
		return -C.int(syscall.EEXIST)
	}

	if err := daos.Init(); err != nil {
		debug.Printf("daos.Init() failed: %s", err)
		return -C.int(syscall.EFAULT)
	}

	// TODO: Maybe use a uuid here?
	*dfs = C.daosfs_t(C.CString("oh hai i'm alive"))
	return 0
}

// LibDaosFileSystemFini finalizes the connection to DAOS. Any open
// fsHandles will be closed before calling daos_fini().
//export LibDaosFileSystemFini
func LibDaosFileSystemFini(dfs C.daosfs_t) {
	if dfs == nil {
		debug.Print("Got nil pointer in LibDaosFileSystemFini()")
		return
	}

	toClose := make([]uint64, 0, len(refs.h2r))
	refs.RLock()
	for hash, ref := range refs.h2r {
		if fs, ok := ref.(*daosfs.FileSystem); ok {
			debug.Printf("Closing fs %q (%d) in Fini()", fs.Name, hash)
			toClose = append(toClose, hash)
		}
	}
	refs.RUnlock()

	for _, hash := range toClose {
		fsh := &C.struct_daosfs_fs_handle{fs_ptr: (C.daosfs_ptr_t)(hash)}
		if rc := CloseDaosFileSystem(fsh); rc != 0 {
			debug.Printf("Got rc %d while closing filesystem", rc)
			return
		}
	}

	daos.Fini()
	C.free(unsafe.Pointer(dfs))
}

// EnableDaosFileSystemDebug enables debug output.
//export EnableDaosFileSystemDebug
func EnableDaosFileSystemDebug() {
	debug.Enable()
}

// DisableDaosFileSystemDebug disables debug output.
//export DisableDaosFileSystemDebug
func DisableDaosFileSystemDebug() {
	debug.Disable()
}

// OpenDaosFileSystem creates a connection to the specified DAOS pool and
// container. The container can be specified as either a uuid string or a
// "friendly" name which is resolved to a container uuid. If the specified
// container does not exist, one will be created. The supplied opaque
// fs handle is initialized here and freed in CloseDaosFileSystem().
//export OpenDaosFileSystem
func OpenDaosFileSystem(cGroup, cPool, cContainer *C.char, cFs **C.struct_daosfs_fs_handle) C.int {
	// Already opened?
	if *cFs != nil {
		debug.Print("Got non-nil pointer in OpenDaosFileSystem()")
		return -C.int(syscall.EEXIST)
	}

	group := C.GoString(cGroup)
	pool := C.GoString(cPool)
	container := C.GoString(cContainer)
	fs, err := daosfs.NewFileSystem(group, pool, container)
	if err != nil {
		debug.Printf("Failed to create DAOS connection with server group %q, pool %q, container %q: %s", group, pool, container, err)
		return -C.int(syscall.EINVAL)
	}

	*cFs = (*C.struct_daosfs_fs_handle)(C.malloc(C.sizeof_struct_daosfs_fs_handle))
	(*cFs).fs_ptr = (C.daosfs_ptr_t)(refs.Set(fs))
	(*cFs).root_ptr = (C.daosfs_ptr_t)(refs.Set(fs.Root()))
	debug.Printf("Opened filesystem %q: %v", fs.Name, (*cFs).fs_ptr)

	return 0
}

// CloseDaosFileSystem closes the connection to DAOS for the supplied
// fs handle. Any open object handles will be closed first, as well as
// the filesystem's container handle and pool handle. Note that this
// function does not call daos_fini() -- that is handled in
// LibDaosFileSystemFini().
//export CloseDaosFileSystem
func CloseDaosFileSystem(cFs *C.struct_daosfs_fs_handle) C.int {
	fs, err := getFs(cFs)
	if err != nil {
		return err2rc(err)
	}
	debug.Printf("Closing filesystem %q: %v", fs.Name, cFs.fs_ptr)

	if err := fs.Fini(); err != nil {
		debug.Printf("Error in Fini(): %s", err)
		return -C.int(syscall.EFAULT)
	}

	refs.Delete(uint64(cFs.root_ptr))
	refs.Delete(uint64(cFs.fs_ptr))
	C.free(unsafe.Pointer(cFs))
	cFs = nil

	return 0
}

// DaosFileSystemTruncate truncates the file to the specifed size
//export DaosFileSystemTruncate
func DaosFileSystemTruncate(cNh *C.struct_daosfs_node_handle, cSize C.size_t) C.int {
	_, err := getNode(cNh)
	if err != nil {
		return err2rc(err)
	}

	// TODO: Implement this -- needs support in DAOS I think?
	return -C.int(syscall.ENOTSUP)
}

// DaosFileSystemSetAttr updates the node's attributes based on the
// values supplied in the given *C.struct_stat
//export DaosFileSystemSetAttr
func DaosFileSystemSetAttr(cNh *C.struct_daosfs_node_handle, st *C.struct_stat, cMask C.uint32_t) C.int {
	node, err := getNode(cNh)
	if err != nil {
		return err2rc(err)
	}

	mask := uint32(cMask)
	na := daosfs.Attr{}

	if mask&daosfs.WriteAttrMode != 0 {
		na.Mode = st2fm(st.st_mode)
	}
	if mask&daosfs.WriteAttrUID != 0 {
		na.Uid = uint32(st.st_uid)
	}
	if mask&daosfs.WriteAttrGID != 0 {
		na.Gid = uint32(st.st_gid)
	}
	if mask&daosfs.WriteAttrSize != 0 {
		na.Size = int64(st.st_size)
	}
	if mask&daosfs.WriteAttrAtime != 0 {
		na.Atime = time.Unix(int64(st.st_atim.tv_sec),
			int64(st.st_atim.tv_nsec))
	}
	if mask&daosfs.WriteAttrMtime != 0 {
		na.Mtime = time.Unix(int64(st.st_mtim.tv_sec),
			int64(st.st_mtim.tv_nsec))
	}
	if mask&daosfs.WriteAttrCtime != 0 {
		na.Ctime = time.Unix(int64(st.st_ctim.tv_sec),
			int64(st.st_ctim.tv_nsec))
	}

	return err2rc(node.SetAttr(&na, mask))
}

// DaosFileSystemGetAttr fills in a *C.struct_stat with the attributes
// of the supplied *daosfs.Node.
//export DaosFileSystemGetAttr
func DaosFileSystemGetAttr(cNh *C.struct_daosfs_node_handle, st *C.struct_stat) C.int {
	node, err := getNode(cNh)
	if err != nil {
		return err2rc(err)
	}

	na, err := node.GetAttr()
	if err != nil {
		debug.Printf("Error in node.Attr(): %s", err)
		return -C.int(syscall.EFAULT)
	}

	debug.Printf("Before stat(): %v", st)
	st.st_dev = C.__dev_t(na.Device)
	st.st_ino = C.__ino_t(na.Inode)
	st.st_mode = C.__mode_t(na.CMode())
	st.st_nlink = C.__nlink_t(na.Nlink)
	st.st_uid = C.__uid_t(na.Uid)
	st.st_gid = C.__gid_t(na.Uid)
	st.st_rdev = C.__dev_t(na.Rdev)
	st.st_size = C.__off_t(na.Size)
	st.st_blksize = C.__blksize_t(na.Blksize)
	st.st_blocks = C.__blkcnt_t(na.Blocks)
	st.st_atim = C.struct_timespec{
		tv_sec:  C.__time_t(na.Atime.Unix()),
		tv_nsec: C.__syscall_slong_t(na.Atime.UnixNano()),
	}
	st.st_mtim = C.struct_timespec{
		tv_sec:  C.__time_t(na.Mtime.Unix()),
		tv_nsec: C.__syscall_slong_t(na.Mtime.UnixNano()),
	}
	st.st_ctim = C.struct_timespec{
		tv_sec:  C.__time_t(na.Ctime.Unix()),
		tv_nsec: C.__syscall_slong_t(na.Ctime.UnixNano()),
	}
	debug.Printf("After stat(): %v", st)

	return 0
}

// DaosFileSystemGetNodeHandle creates a *C.struct_daosfs_node_handle to wrap
// a *daos.Node.
//export DaosFileSystemGetNodeHandle
func DaosFileSystemGetNodeHandle(np C.daosfs_ptr_t, cNh **C.struct_daosfs_node_handle) C.int {
	ref, found := refs.Get(uint64(np))
	if !found {
		debug.Printf("Unable to find node for %p", np)
		return -C.int(syscall.ENOENT)
	}
	node, ok := ref.(*daosfs.Node)
	if !ok {
		debug.Printf("%p does not refer to a *daosfs.Node", uint64(np))
		return -C.int(syscall.EINVAL)
	}

	*cNh = (*C.struct_daosfs_node_handle)(C.malloc(C.sizeof_struct_daosfs_node_handle))
	(*cNh).node_ptr = np

	// Create the key used for wire handles sent to/from client.
	C.memcpy(unsafe.Pointer(&(*cNh).key.oid),
		unsafe.Pointer(node.Oid.Pointer()),
		C.sizeof_daos_obj_id_t)
	ne := node.Epoch()
	C.memcpy(unsafe.Pointer(&(*cNh).key.epoch),
		unsafe.Pointer(ne.Pointer()),
		C.sizeof_daos_epoch_t)
	debug.Printf("key oid: %s->%v, key epoch: %d->%v", node.Oid, (*cNh).key.oid, ne, (*cNh).key.epoch)

	return 0
}

// DaosFileSystemFreeNodeHandle deallocates memory for the node handle.
//export DaosFileSystemFreeNodeHandle
func DaosFileSystemFreeNodeHandle(cNh *C.struct_daosfs_node_handle) {
	if cNh == nil {
		debug.Print("Got nil pointer in DaosFileSystemFreeNodeHandle()")
		return
	}
	refs.Delete(uint64(cNh.node_ptr))

	C.free(unsafe.Pointer(cNh))
}

// DaosFileSystemOpen opens the supplied node handle
//export DaosFileSystemOpen
func DaosFileSystemOpen(cNh *C.struct_daosfs_node_handle, flags C.int) C.int {
	node, err := getNode(cNh)
	if err != nil {
		return err2rc(err)
	}

	debug.Print("DaosFileSystemOpen()")
	return err2rc(node.Open(uint32(flags)))
}

// DaosFileSystemClose closes the supplied node handle
//export DaosFileSystemClose
func DaosFileSystemClose(cNh *C.struct_daosfs_node_handle) C.int {
	node, err := getNode(cNh)
	if err != nil {
		return err2rc(err)
	}

	debug.Print("DaosFileSystemClose()")
	return err2rc(node.Close())
}

// DaosFileSystemRead reads from a node handle
//export DaosFileSystemRead
func DaosFileSystemRead(cNh *C.struct_daosfs_node_handle, cOffset C.uint64_t, cBufSize C.size_t, cReadAmount *C.size_t, cBuffer unsafe.Pointer) C.int {
	node, err := getNode(cNh)
	if err != nil {
		return err2rc(err)
	}

	/* TODO: Add support for NFSv4 and stateful ops. For now,
	 * these are all stateless.
	if !node.IsOpen() {
		debug.Print("Read() on closed node")
		return -C.int(syscall.EBADFD)
	}
	*/

	if err := node.Open(syscall.O_RDONLY); err != nil {
		return err2rc(err)
	}
	defer node.Close()

	bufSize := int64(cBufSize)
	data := (*[1 << 30]byte)(unsafe.Pointer(cBuffer))[:bufSize:bufSize]
	read, err := node.FileHandle.Read(int64(cOffset), bufSize, data)
	*cReadAmount = C.size_t(read)

	return err2rc(err)
}

// DaosFileSystemWrite writes to a node handle
//export DaosFileSystemWrite
func DaosFileSystemWrite(cNh *C.struct_daosfs_node_handle, cOffset C.uint64_t, cBufSize C.size_t, cWroteAmount *C.size_t, cBuffer unsafe.Pointer) C.int {
	node, err := getNode(cNh)
	if err != nil {
		return err2rc(err)
	}

	if !node.IsOpen() {
		debug.Print("Write() on closed node")
		return -C.int(syscall.EBADFD)
	}

	/*if err := node.Open(syscall.O_WRONLY); err != nil {
		return err2rc(err)
	}
	defer node.Close()*/

	bufSize := int64(cBufSize)
	data := (*[1 << 30]byte)(unsafe.Pointer(cBuffer))[:bufSize:bufSize]
	wrote, err := node.FileHandle.Write(int64(cOffset), data)
	*cWroteAmount = C.size_t(wrote)
	if err == nil {
		node.FileHandle.Commit()
	}

	return err2rc(err)
}

// DaosFileSystemCommit signals to the node's file handle that it should commit
//export DaosFileSystemCommit
func DaosFileSystemCommit(cNh *C.struct_daosfs_node_handle, cOffset C.off_t, cLength C.size_t) C.int {
	node, err := getNode(cNh)
	if err != nil {
		return err2rc(err)
	}

	if !node.IsOpen() {
		debug.Print("Write() on closed node")
		return -C.int(syscall.EBADFD)
	}

	debug.Printf("DaosFileSystemCommit(%d %d)", cOffset, cLength)
	node.FileHandle.Commit()
	// FIXME: This isn't quite right. The FileHandle stuff needs to be
	// fleshed out a bit more to support committing the epoch while
	// keeping the FH open. I think this means getting a new tx after
	// completing the current one.
	return err2rc(node.FileHandle.Close())
}

// DaosFileSystemCreate creates a regular file
//export DaosFileSystemCreate
func DaosFileSystemCreate(cNh *C.struct_daosfs_node_handle, cName *C.char, st *C.struct_stat, flags C.int, out **C.struct_daosfs_node_handle) C.int {
	parent, err := getNode(cNh)
	if err != nil {
		return err2rc(err)
	}

	dreq := daosfs.CreateRequest{
		Uid:   uint32(st.st_uid),
		Gid:   uint32(st.st_gid),
		Flags: uint32(flags),
		Name:  C.GoString(cName),
		Mode:  st2fm(st.st_mode),
	}

	child, err := parent.Create(&dreq)
	if err != nil {
		debug.Printf("Error in DaosFileSystemCreate(): %s", err)
		return err2rc(err)
	}

	ptr := (C.daosfs_ptr_t)(refs.Set(child))
	if rc := DaosFileSystemGetNodeHandle(ptr, out); rc != 0 {
		return rc
	}

	return DaosFileSystemGetAttr(*out, st)
}

// DaosFileSystemMkdir creates a directory
//export DaosFileSystemMkdir
func DaosFileSystemMkdir(cNh *C.struct_daosfs_node_handle, cName *C.char, st *C.struct_stat, out **C.struct_daosfs_node_handle) C.int {
	parent, err := getNode(cNh)
	if err != nil {
		return err2rc(err)
	}

	dreq := daosfs.MkdirRequest{
		Uid:  uint32(st.st_uid),
		Gid:  uint32(st.st_gid),
		Name: C.GoString(cName),
		Mode: os.FileMode(st.st_mode),
	}
	child, err := parent.Mkdir(&dreq)
	if err != nil {
		debug.Printf("Error in DaosFileSystemMkdir(): %s", err)
		return err2rc(err)
	}

	ptr := (C.daosfs_ptr_t)(refs.Set(child))
	if rc := DaosFileSystemGetNodeHandle(ptr, out); rc != 0 {
		return rc
	}

	return DaosFileSystemGetAttr(*out, st)
}

// DaosFileSystemLookupPath creates a node handle for node at the given
// path, if found.
//export DaosFileSystemLookupPath
func DaosFileSystemLookupPath(cNh *C.struct_daosfs_node_handle, cPath *C.char, out **C.struct_daosfs_node_handle) C.int {
	parent, err := getNode(cNh)
	if err != nil {
		return err2rc(err)
	}

	path := C.GoString(cPath)
	child, err := parent.Lookup(path)
	if err != nil {
		return -C.int(syscall.ENOENT)
	}

	ptr := (C.daosfs_ptr_t)(refs.Set(child))
	return DaosFileSystemGetNodeHandle(ptr, out)
}

// DaosFileSystemLookupHandle creates a node handle for a node specified
// by the given hash key (NFS-style lookup).
//export DaosFileSystemLookupHandle
func DaosFileSystemLookupHandle(cFs *C.struct_daosfs_fs_handle, cNk *C.struct_daosfs_node_key, out **C.struct_daosfs_node_handle) C.int {
	if cNk == nil {
		debug.Print("Got nil nodekey in DaosFileSystemLookupHandle()")
		return -C.int(syscall.EINVAL)
	}

	fs, err := getFs(cFs)
	if err != nil {
		return err2rc(err)
	}

	var oid daos.ObjectID
	C.memcpy(unsafe.Pointer(&oid), unsafe.Pointer(&cNk.oid),
		C.sizeof_daos_obj_id_t)
	epoch := (daos.Epoch)(cNk.epoch)
	debug.Printf("nk oid: %v, nk epoch: %v", oid, epoch)
	node, err := fs.GetNode(&oid, &epoch)
	if err != nil {
		return -C.int(syscall.ENOENT)
	}

	ptr := (C.daosfs_ptr_t)(refs.Set(node))
	return DaosFileSystemGetNodeHandle(ptr, out)
}

// DaosFileSystemStatFs gets attributes of the filesystem.
// FIXME: For the moment, they're completely bogus.
//export DaosFileSystemStatFs
func DaosFileSystemStatFs(cFs *C.struct_daosfs_fs_handle, cVst *C.struct_daosfs_statvfs) C.int {
	// FIXME: I'm not really sure what we can actually provide here,
	// given the way DAOS works. For the time being, just gin up some
	// happy numbers.
	const UINT64MAX = 1<<63 - 1

	cVst.f_bsize = 1024 * 1024 // 1M
	cVst.f_frsize = 1024
	cVst.f_blocks = 0
	cVst.f_bfree = UINT64MAX
	cVst.f_bavail = UINT64MAX
	cVst.f_files = 0
	cVst.f_ffree = UINT64MAX
	cVst.f_favail = UINT64MAX
	// cVst.f_fsid = ? // get the container uuid? nothing uses this now...
	cVst.f_flag = 0
	cVst.f_namemax = 4096

	return 0
}

// DaosFileSystemUnlink removes the named entry from a directory.
//export DaosFileSystemUnlink
func DaosFileSystemUnlink(cNh *C.struct_daosfs_node_handle, cPath *C.char) C.int {
	parent, err := getNode(cNh)
	if err != nil {
		return err2rc(err)
	}

	path := C.GoString(cPath)
	child, err := parent.Lookup(path)
	if err != nil {
		return -C.int(syscall.ENOENT)
	}

	switch child.Type() {
	case os.ModeDir:
		err = parent.Rmdir(path)
	default:
		err = parent.Unlink(path)
	}

	return err2rc(err)
}

// DaosFileSystemReadDir reads the contents of a directory and passes the
// found dirent info to the supplied callback.
//export DaosFileSystemReadDir
func DaosFileSystemReadDir(cNh *C.struct_daosfs_node_handle, cOffset *C.uint64_t, rcb C.daosfs_readdir_cb, rcbArg unsafe.Pointer, cEOF *C.bool) C.int {
	parent, err := getNode(cNh)
	if err != nil {
		return err2rc(err)
	}

	children, err := parent.Children()
	if err != nil {
		debug.Printf("Unable to get children of %s (%s): %s", parent.Name, parent.Oid, err)
		return -C.int(syscall.EFAULT)
	}
	for i, child := range children {
		cName := C.CString(child.Name)
		defer C.free(unsafe.Pointer(cName))
		C.invoke_readdir_cb(rcb, cName, rcbArg, C.uint64_t(i+3))
	}
	*cEOF = C.bool(true)

	return 0
}
