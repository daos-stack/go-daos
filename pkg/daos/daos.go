package daos

//
// #cgo LDFLAGS:  -ldaos  -lcrt_util -lcrt -ldaos_common -ldaos_tier -luuid -lpmem
// #include <stdlib.h>
// #include <daos.h>
// #include <daos/common.h>
//
//
//daos_rank_t*  _alloc_ranks(uint32_t n) {
//        return (daos_rank_t*) calloc(n, sizeof(daos_rank_t));
//}
import "C"

import (
	"unsafe"

	"github.com/pkg/errors"
)

// Init initializes the DAOS connection
func Init() error {
	rc, err := C.daos_init()
	if rc != 0 {
		return errors.Wrapf(err, "init failed %d, ", rc)
	}
	return nil
}

// Fini shutsdown DAOS connection
func Fini() {
	C.daos_fini()
}

// Returns an failure if rc != 0. If err is already set
// then it is wrapped, otherwise it is ignored.
func rc2err(label string, rc C.int, err error) error {
	if rc != 0 {
		if err != nil {
			return errors.Wrapf(err, "%s failed: %d", label, rc)
		}
		return errors.Errorf("%s: %d", label, rc)
	}
	return nil
}

func uuid2str(uuid []C.uchar) string {
	var buf [37]C.char
	C.uuid_unparse_lower((*C.uchar)(unsafe.Pointer(&uuid[0])), (*C.char)(unsafe.Pointer(&buf[0])))
	return C.GoString((*C.char)(unsafe.Pointer(&buf[0])))
}

// Analog to uuid2str but returns *C.uchar for convenience.
func str2uuid(s string) (*C.uchar, error) {
	var uuid [16]C.uchar
	cstr := C.CString(s)
	defer C.free(unsafe.Pointer(cstr))
	rc, err := C.uuid_parse(cstr, (*C.uchar)(unsafe.Pointer(&uuid[0])))
	if rc != 0 {
		return nil, rc2err("uuid_parse", rc, err)
	}
	return (*C.uchar)(unsafe.Pointer(&uuid[0])), nil
}

/*
int
daos_pool_create(unsigned int mode, unsigned int uid, unsigned int gid,
		 const char *grp, const daos_rank_list_t *tgts, const char *dev,
		 daos_size_t size, daos_rank_list_t *svc, uuid_t uuid,
		 daos_event_t *ev)
*/

// PoolCreate creates a new pool of specfied size.
func PoolCreate(mode uint32, uid uint32, gid uint32, group string, size int64) (string, error) {
	var cGroup *C.char
	if group != "" {
		cGroup = C.CString(group)
		defer C.free(unsafe.Pointer(cGroup))
	}

	cDev := C.CString("pmem")
	defer C.free(unsafe.Pointer(cDev))

	nranks := C.uint32_t(13)
	svc := &C.daos_rank_list_t{}
	ranks := C._alloc_ranks(nranks)
	defer C.free(unsafe.Pointer(ranks))

	svc.rl_nr.num = nranks
	svc.rl_nr.num_out = 0
	svc.rl_ranks = ranks

	var u C.uuid_t
	var uuid [unsafe.Sizeof(u)]C.uchar

	rc, err := C.daos_pool_create(C.uint(mode),
		C.uint(uid),
		C.uint(gid),
		cGroup,
		nil, /* tgts */
		cDev,
		C.daos_size_t(size),
		svc,
		(*C.uchar)(unsafe.Pointer(&uuid[0])),
		nil /* ev */)

	if err = rc2err("daos_pool_create", rc, err); err != nil {
		return "", err
	}
	return uuid2str(uuid[:]), nil
}

/*
int
daos_pool_destroy(const uuid_t uuid, const char *grp, int force,
		  daos_event_t *ev)
*/

// PoolDestroy deletes the passed pool uuid.
func PoolDestroy(pool string, group string, force int) error {
	var cGroup *C.char
	if group != "" {
		cGroup = C.CString(group)
		defer C.free(unsafe.Pointer(cGroup))
	}
	uuid, err := str2uuid(pool)
	if err != nil {
		return errors.Wrapf(err, "unable to parse %s", pool)
	}
	rc, err := C.daos_pool_destroy(uuid, cGroup, C.int(force), nil)
	return rc2err("daos_pool_destroy", rc, err)
}

/*
int
daos_pool_connect(const uuid_t uuid, const char *grp,
		  const daos_rank_list_t *svc, unsigned int flags,
		  daos_handle_t *poh, daos_pool_info_t *info, daos_event_t *ev);
*/

type PoolHandle C.daos_handle_t
type PoolInfo C.daos_pool_info_t

func (info *PoolInfo) UUID() string {
	if info == nil {
		return ""
	}
	return uuid2str(info.pi_uuid[:])
}

func (info *PoolInfo) NumTargets() int {
	if info == nil {
		return 0
	}
	return int(info.pi_ntargets)
}

func (info *PoolInfo) NumDisabled() int {
	if info == nil {
		return 0
	}
	return int(info.pi_ndisabled)
}

func (info *PoolInfo) Mode() uint32 {
	if info == nil {
		return 0
	}
	return uint32(info.pi_mode)
}

func (info *PoolInfo) Space() uint64 {
	if info == nil {
		return 0
	}
	// pi_space not implemented yet
	//	return uint64(info.pi_space)
	return 0
}

// PoolConnect flags
const (
	PoolConnectRO = C.DAOS_PC_RO
	PoolConnectRW = C.DAOS_PC_RW
	PoolConnectEX = C.DAOS_PC_EX
)

// PoolConnect returns the pool handle for given pool.
func PoolConnect(pool string, group string, flags uint) (*PoolHandle, error) {
	var cGroup *C.char
	if group != "" {
		cGroup = C.CString(group)
		defer C.free(unsafe.Pointer(cGroup))
	}
	uuid, err := str2uuid(pool)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to parse %s", pool)
	}
	var poh PoolHandle
	rc, err := C.daos_pool_connect(uuid, cGroup, nil, C.uint(flags), (*C.daos_handle_t)(&poh),
		nil, nil)
	if err = rc2err("daos_pool_connect", rc, err); err != nil {
		return nil, err
	}
	return &poh, nil
}

/*
int
daos_pool_disconnect(daos_handle_t poh, daos_event_t *ev);
*/

// PoolDisconnect closes the pool handle.
func (poh *PoolHandle) Disconnect() error {
	rc, err := C.daos_pool_disconnect((C.daos_handle_t)(*poh), nil)
	return rc2err("daos_pool_disconnect", rc, err)
}

/*
int
daos_pool_query(daos_handle_t poh, daos_rank_list_t *tgts,
		daos_pool_info_t *info, daos_event_t *ev);
*/
// Info returns current pool info
func (poh *PoolHandle) Info() (*PoolInfo, error) {
	var info PoolInfo
	rc, err := C.daos_pool_query((C.daos_handle_t)(*poh), nil, (*C.daos_pool_info_t)(&info), nil)

	if err = rc2err("daos_pool_query", rc, err); err != nil {
		return nil, err
	}
	return &info, nil
}
