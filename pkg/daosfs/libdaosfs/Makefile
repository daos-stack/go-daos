run: test
	orterun -np 1 --ompi-server file:/tmp/daos-uri ./test $(POOL_UUID) libdaosfs

test: main.c libdaosfs.so
	gcc -o test main.c $(CFLAGS) $(LDFLAGS) -ldaos  -lcrt_util -lcrt -ldaos_common -ldaos_tier -ldaos_array -luuid -L. -ldaosfs

libdaosfs.so: libdaosfs
	ln -sf $< $@

libdaosfs: *.go ../*.go daosfs_types.h
	go build -buildmode=c-shared *.go
