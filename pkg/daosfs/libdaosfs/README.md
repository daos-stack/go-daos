# libdaosfs - A C-Compatible API for daosfs

NB: This is still very much a work in progress. The goal is to provide a C API to be consumed by things like nfs-ganesha (c.f. [FSAL_DAOSFS](https://github.com/mjmac/nfs-ganesha/tree/daosfs/src/FSAL/FSAL_DAOSFS)).

## Build
Rough build instructions:

* Build Prerequisites:
  * [DAOS](https://github.com/daos-stack/daos)
  * Go Environment >= 1.8

* Build:
  * make libdaosfs.so

## Usage
By itself, this shared library is not especially useful. Examining its symbols, we can see that it's exporting a bunch of symbols for use in C applications:

    00000000001952e0 T DaosFileSystemClose
    0000000000195430 T DaosFileSystemCommit
    0000000000195490 T DaosFileSystemCreate
    0000000000195250 T DaosFileSystemFreeNodeHandle
    00000000001951b0 T DaosFileSystemGetAttr
    0000000000195200 T DaosFileSystemGetNodeHandle
    00000000001955e0 T DaosFileSystemLookupHandle
    0000000000195580 T DaosFileSystemLookupPath
    0000000000195510 T DaosFileSystemMkdir
    0000000000195290 T DaosFileSystemOpen
    0000000000195330 T DaosFileSystemRead
    00000000001956e0 T DaosFileSystemReadDir
    0000000000195150 T DaosFileSystemSetAttr
    0000000000195640 T DaosFileSystemStatFs
    0000000000195100 T DaosFileSystemTruncate
    0000000000195690 T DaosFileSystemUnlink
    00000000001953b0 T DaosFileSystemWrite

For more details about the API itself, please see ~~the generated godoc~~ (er, well, [the source code](https://github.com/daos-stack/go-daos/blob/master/pkg/daosfs/libdaosfs/libdaosfs.go), because [reasons](https://github.com/golang/gddo/issues/397#issuecomment-294901095)).

These function names should be pretty comprehensible to anyone who's done any filesystem development. The idea behind daosfs is to provide a POSIX-like filesystem interface for storage which exploits the low latency and high throughput possibilities provided by persistent memory and DAOS.

A very basic (useless, even!) use of the library might look something like the following:

	#include <stdio.h>
	#include <unistd.h>
	#include <sys/stat.h>
	#include "daosfs_types.h"
	#include "libdaosfs.h"
	
	int main(int argc, char *argv[]) {
	        int rc;
	        char *group = NULL;
	        char *pool;
	        char *fsname;
	        daosfs_t dfs;
	        struct daosfs_fs_handle *fs = NULL;
	        struct daosfs_node_handle *nh, *new = NULL;
	
	        if (argc != 3) {
	                printf("Usage: %s <pool_uuid> <container>\n", argv[0]);
	                exit(1);
	        }
	        pool = argv[1];
	        fsname = argv[2];
	
	        /* This function enables debug output to stderr from within
	         * the libdaosfs/daosfs/daos packages. Better to optionally 
	         * enable it with a -debug flag, probably.
	         */
	        EnableDaosFileSystemDebug();
	
	        /* This function initializes the underlying DAOS libraries.
	         * It only needs to be called once per program invocation. The
	         * supplied daosfs_t parameter is an opaque token which
	         * is initialized here and freed in LibDaosFileSystemFini().
	         */
	        rc = LibDaosFileSystemInit(&dfs);
	        if (rc != 0) {
	                printf("daos_init() failed: %d\n", rc);
	                exit(1);
	        }
	
	        /* This function opens a DAOS container with the specified
	         * string name or uuid in the specified pool. The pool must
	         * exist already, but the container will be created 
	         * automatically if necessary. The supplied
	         * daosfs_fs_handle will be initialized here and is freed in
	         * CloseDaosFileSystem().
	         */
	        rc = OpenDaosFileSystem(group, pool, fsname, &fs);
	
	        /* This function shows the construction of a
	         * daosfs_node_handle struct to wrap a *daosfs.Node
	         * instance. Normally this function is called internally
	         * by other functions, but in this case it's used to get
	         * the filesystem's root handle.
	         */
	        rc = DaosFileSystemGetNodeHandle(fs->root_ptr, &nh);
	
	        /* This function shows the process of looking up a path under
	         * the filesystem root. If the path resolves to a directory
	         * or file, the supplied node handle struct is filled in
	         * with its details. If not, an appropriate error code is
	         * returned.
	         */
	        rc = DaosFileSystemLookupPath(nh, "/foo/bar/baz", &new);
	
	        /* These functions free the memory allocated for the node
	         * handles.
	         */
	        DaosFileSystemFreeNodeHandle(new);
	        DaosFileSystemFreeNodeHandle(nh);
	
	        /* This function explicitly closes the open filesystem. It
	         * is an error to shut down DAOS with open handles, so it's
	         * important to close the filesystem before shutting down. This
	         * function takes care of closing all open object handles in
	         * the filesystem's container and then closing the container
	         * and pool handles.
	         */
	        rc = CloseDaosFileSystem(fs);
	
	        /* This function shuts down the DAOS subsystem. In theory,
	         * it will use the internal reference map to shut down
	         * any filesystems which haven't already been explicitly
	         * shut down, but it would be better to do that explicitly
	         * with CloseDaosFileSystem().
	         */
	        LibDaosFileSystemFini(dfs);
	
	        return rc;
	}
	
For a more complete usage example, see the prototype nfs-ganesha FSAL referenced earlier.

## Implementation Notes
(Or, how the sausage was made)

It turns out that exporting Go to C is kinda tricky. Using [C from Go](https://golang.org/cmd/cgo/#hdr-Go_references_to_C) is pretty straightforward (joyful, even?), but the reverse isn't quite as painless. I had a hard time finding anything other than trivial examples of this, so I thought I'd write about my own experiences in this section.

### Reference Handles
I'm not entirely certain that this was the correct solution to the problem, but it seems to be working fine. The problem with mixing C and Go is that one has to be very careful about where memory is allocated and how those allocations are used. The [cgo documentation](https://golang.org/cmd/cgo/#hdr-Passing_pointers) has exhaustive detail, but the short story is that it's not safe to hand a Go pointer back to a caller in C-land. This makes intuitive sense -- Go is garbage collected, and the Go gc has no idea what's happening out there in C-land. The workaround I came up with was to store a map of hash keys to Go references. The refs are stored with refcounts in a global structure and C gets a uint64 hash key to hang on to. When the C stuff is finished with a reference, calling the appropriate Free/Close function will decrement the reference's count and eventually delete it.

### -EGIANTPACKAGE
(yeah, yeah) ... One of the implementation nits that I encountered was that the [c-shared buildmode](https://golang.org/cmd/go/#hdr-Description_of_build_modes) requires all of the exported functions to be in a main package, and more annoyingly, all of the exported functions for a specific library must be in the same .go file. The result is that libdaosfs.go is nearly 1KLOC, even though it's mostly just wrapping the [daosfs package](https://godoc.org/github.com/daos-stack/go-daos/pkg/daosfs). Maybe a bit more refactoring could be done to cut that down a little, but I can see this getting out of hand for a larger project.

### return codes? bleah.
I hadn't fully appreciated how spoiled I've become by Go's rich error handling capabilities. Libraries in particular are not supposed to emit errors or warnings to stdout/stderr, so it feels like a big step backward to have to boil down "something strange happened in DAOS and it made this thing unhappy which also made this other thing unhappy" to -EFAULT.