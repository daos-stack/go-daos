#ifndef DAOSFS_TYPES_H
#define DAOSFS_TYPES_H

#include <daos.h>

typedef void* daosfs_t;
typedef unsigned long long daosfs_ptr_t; // GoUint64
typedef bool (*daosfs_readdir_cb)(char *name, void *arg, uint64_t offset);

struct daosfs_node_key {
	daos_obj_id_t oid;
	daos_epoch_t epoch;
};

struct daosfs_node_handle {
	daosfs_ptr_t	node_ptr;
	struct daosfs_node_key key;
};

struct daosfs_fs_handle {
	daosfs_ptr_t fs_ptr;
	daosfs_ptr_t root_ptr;
};

struct daosfs_statvfs {
	uint64_t f_bsize;
	uint64_t f_frsize;
	uint64_t f_blocks;
	uint64_t f_bfree;
	uint64_t f_bavail;
	uint64_t f_files;
	uint64_t f_ffree;
	uint64_t f_favail;
	uint64_t f_fsid[2];
	uint64_t f_flag;
	uint64_t f_namemax;
};

/* setattr mask bits */
#define DAOSFS_SETATTR_MODE   1
#define DAOSFS_SETATTR_UID    2
#define DAOSFS_SETATTR_GID    4
#define DAOSFS_SETATTR_MTIME  8
#define DAOSFS_SETATTR_ATIME 16
#define DAOSFS_SETATTR_SIZE  32
#define DAOSFS_SETATTR_CTIME 64

#endif /* DAOSFS_TYPES_H */
