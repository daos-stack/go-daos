# daos-fuse -- Create and manage a POSIX-like filesystem in DAOS

Given a mountpoint, a DAOS pool UUID, and a container name, this utility will create and/or open the specified container for use as a filesystem backing store.

## Prerequisites

* The fuse userspace tools (`yum install -y fuse`)
* A running daos_server instance
* A DAOS pool uuid (e.g. as returned by `dcmd pool create`)

## Example usage
    $ daos-fuse $HOME/daos-fuse 23605ee8-3172-4f0f-9305-38001a9332fd test &
    $ ls -l $HOME/daos-fuse
	 total 0
	 drwxrwxr-x. 1 vagrant vagrant  0 Mar 12 04:47 bar
	 -rw-rw-r--. 1 vagrant vagrant 29 Mar 12 04:47 baz
	 drwxrwxr-x. 1 vagrant vagrant  0 Mar 12 04:47 foo
	 -rw-rw-r--. 1 vagrant vagrant 58 Mar 12 04:47 qux
	 $ fusermount -u $HOME/daos-fuse
	 
Note that the correct way to shut down the filesystem is with the fusermount utility; this will ensure a clean shutdown of the fuse filesystem server.

The -debug flag will enable copious amounts of debug output, which may be useful for development.