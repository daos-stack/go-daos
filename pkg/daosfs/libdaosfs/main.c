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

	EnableDaosFileSystemDebug();

	rc = LibDaosFileSystemInit(&dfs);
	if (rc != 0) {
		printf("daos_init() failed: %d\n", rc);
		exit(1);
	}

	rc = OpenDaosFileSystem(group, pool, fsname, &fs);
	printf("Got rc %d from OpenDaosFileSystem\n", rc);

	rc = DaosFileSystemGetNodeHandle(fs->root_ptr, &nh);
	printf("Got rc %d from DaosFileSystemGetNodeHandle\n", rc);
	printf("nh epoch: %d\n", nh->key->epoch);

	rc = DaosFileSystemLookupPath(nh, "/butt", &new);
	printf("Got rc %d from DaosFileSystemLookupPath\n", rc);

	DaosFileSystemFreeNodeHandle(new);
	DaosFileSystemFreeNodeHandle(nh);

	rc = CloseDaosFileSystem(fs);
	printf("Got rc %d from CloseDaosFileSystem\n", rc);

	LibDaosFileSystemFini(dfs);

	return rc;
}
