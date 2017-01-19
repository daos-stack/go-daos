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

type (
	// Rank is a target index
	Rank C.daos_rank_t

	// PoolHandle is an open conection to a Pool
	PoolHandle C.daos_handle_t

	// PoolInfo is current state of a pool
	PoolInfo C.daos_pool_info_t

	// ContHandle refers to an open container.
	ContHandle C.daos_handle_t

	// ContInfo is current status of a container.
	ContInfo C.daos_cont_info_t

	// Epoch identifiers (uint64)
	Epoch C.daos_epoch_t

	// EpochState is current epoch status.
	EpochState C.daos_epoch_state_t
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
	rc, err := C.daos_pool_connect(uuid, cGroup, nil, C.uint(flags), poh.Pointer(),
		nil, nil)
	if err = rc2err("daos_pool_connect", rc, err); err != nil {
		return nil, err
	}
	return &poh, nil
}

// H returns C-typed handle
func (poh *PoolHandle) H() C.daos_handle_t {
	return (C.daos_handle_t)(*poh)
}

// Pointer returns C-typed handle pointer
func (poh *PoolHandle) Pointer() *C.daos_handle_t {
	return (*C.daos_handle_t)(poh)
}

// Disconnect closes the pool handle.
func (poh *PoolHandle) Disconnect() error {
	rc, err := C.daos_pool_disconnect(poh.H(), nil)
	return rc2err("daos_pool_disconnect", rc, err)
}

// Info returns current pool info
func (poh *PoolHandle) Info() (*PoolInfo, error) {
	var info PoolInfo
	rc, err := C.daos_pool_query(poh.H(), nil, (*C.daos_pool_info_t)(&info), nil)

	if err = rc2err("daos_pool_query", rc, err); err != nil {
		return nil, err
	}
	return &info, nil
}

// Exclude ranks from pool.
//
// https://github.com/golang/go/wiki/cgo
// Convert C array into Go slice
//     slice := (*[1 << 30]C.YourType)(unsafe.Pointer(theCArray))[:length:length]
//
func (poh *PoolHandle) Exclude(targets []Rank) error {
	var tgts C.daos_rank_list_t

	tgts.rl_nr.num = C.uint32_t(len(targets))
	tgts.rl_ranks = C._alloc_ranks(tgts.rl_nr.num)
	defer C.free(unsafe.Pointer(tgts.rl_ranks))

	ranks := (*[1 << 30]C.daos_rank_t)(unsafe.Pointer(tgts.rl_ranks))[:len(targets):len(targets)]
	for i, r := range targets {
		ranks[i] = C.daos_rank_t(r)
	}
	rc, err := C.daos_pool_exclude(poh.H(), &tgts, nil)
	return rc2err("daos_pool_exclude", rc, err)
}

// NewContainer creates a container identified by the UUID
func (poh *PoolHandle) NewContainer(uuid string) error {
	cuuid, err := str2uuid(uuid)
	if err != nil {
		return errors.Wrapf(err, "unable to parse %s", uuid)
	}

	rc, err := C.daos_cont_create(poh.H(), cuuid, nil)
	return rc2err("daos_cont_create", rc, err)
}

// Open a the container identified by the UUID
func (poh *PoolHandle) Open(uuid string, flags int) (*ContHandle, error) {
	cuuid, err := str2uuid(uuid)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to parse %s", uuid)
	}

	var coh ContHandle
	rc, err := C.daos_cont_open(poh.H(), cuuid, C.uint(flags), (*C.daos_handle_t)(&coh), nil, nil)
	if err := rc2err("daos_cont_open", rc, err); err != nil {
		return nil, err
	}
	return &coh, nil
}

// Destroy the container identified by the UUID
func (coh *ContHandle) Destroy(uuid string, force bool) error {
	cuuid, err := str2uuid(uuid)
	if err != nil {
		return errors.Wrapf(err, "unable to parse %s", uuid)
	}

	var cforce C.int
	if force {
		cforce = 1
	}

	rc, err := C.daos_cont_destroy(coh.H(), cuuid, cforce, nil)
	if err := rc2err("daos_cont_destroy", rc, err); err != nil {
		return err
	}
	return nil
}

// H returns C-typed handle
func (coh *ContHandle) H() C.daos_handle_t {
	return C.daos_handle_t(*coh)
}

// Native returns native C-type epoch
func (e Epoch) Native() C.daos_epoch_t {
	return C.daos_epoch_t(e)
}

// Pointer turns C-typed EpochState pointer
func (s *EpochState) Pointer() *C.daos_epoch_state_t {
	return (*C.daos_epoch_state_t)(s)
}

// Close the container handle
func (coh *ContHandle) CLose() error {
	rc, err := C.daos_cont_close(coh.H(), nil)
	return rc2err("daos_cont_close", rc, err)
}

// Info returns current pool info
func (coh *ContHandle) Info() (*ContInfo, error) {
	var info ContInfo
	rc, err := C.daos_cont_query(coh.H(), (*C.daos_cont_info_t)(&info), nil)

	if err = rc2err("daos_cont_query", rc, err); err != nil {
		return nil, err
	}
	return &info, nil
}

// Attributes returns slide of attribute names
func (coh *ContHandle) Attributes() ([]string, error) {
	var size C.size_t
	rc, err := C.daos_cont_attr_list(coh.H(), nil, &size, nil)
	if err = rc2err("daos_cont_attr_list", rc, err); err != nil {
		return nil, err
	}

	if size == 0 {
		return nil, nil
	}

	buf := make([]byte, size)
	rc, err = C.daos_cont_attr_list(coh.H(), (*C.char)(unsafe.Pointer(&buf[0])), &size, nil)
	if err = rc2err("daos_cont_attr_list", rc, err); err != nil {
		return nil, err
	}

	var attrs []string
	var s int
	for i, b := range buf {
		if b == 0 {
			attrs = append(attrs, string(buf[s:i]))
			s = i + 1
		}
	}
	return attrs, nil
}

// AttributeGet returns values for given set of attributes.
func (coh *ContHandle) AttributeGet(names []string) ([]byte, error) {
	return nil, errors.New("Not Implemented")
}

// AttributeSet set named attributes to the provided values.
func (coh *ContHandle) AttributeSet(names []string, values []byte) error {
	return errors.New("Not Implemented")
}

// HCE returns Highest Committed Epoch
func (s *EpochState) HCE() Epoch {
	return Epoch(s.es_hce)
}

// LRE returns Lowest Referenced Epoch
func (s *EpochState) LRE() Epoch {
	return Epoch(s.es_lre)
}

// LHE returns Lowest Held Epoch
func (s *EpochState) LHE() Epoch {
	return Epoch(s.es_lhe)
}

// GHCE Global Highest Committted Epoch
func (s *EpochState) GHCE() Epoch {
	return Epoch(s.es_ghce)
}

// GLRE Global Lowest Referenced Epoch
func (s *EpochState) GLRE() Epoch {
	return Epoch(s.es_glre)
}

// GHPCE Global Highest Partially Committted Epoch
func (s *EpochState) GHPCE() Epoch {
	return Epoch(s.es_ghpce)
}

// EpochFlush completes the epoch and returns the epoch state.
func (coh *ContHandle) EpochFlush(e Epoch) (*EpochState, error) {
	var s EpochState
	rc, err := C.daos_epoch_flush(coh.H(), e.Native(), s.Pointer(), nil)
	if err = rc2err("daos_epoch_flush", rc, err); err != nil {
		return nil, err
	}
	return &s, nil

}

// EpochDiscard discards the epoch and returns current epoch state.
func (coh *ContHandle) EpochDiscard(e Epoch) (*EpochState, error) {
	var s EpochState
	rc, err := C.daos_epoch_discard(coh.H(), e.Native(), s.Pointer(), nil)
	if err = rc2err("daos_epoch_discard", rc, err); err != nil {
		return nil, err
	}
	return &s, nil

}

// EpochQuery returns current epoch state.
func (coh *ContHandle) EpochQuery() (*EpochState, error) {
	var s EpochState
	rc, err := C.daos_epoch_query(coh.H(), s.Pointer(), nil)
	if err = rc2err("daos_epoch_query", rc, err); err != nil {
		return nil, err
	}
	return &s, nil

}

// EpochHold propose a new lowest held epoch on this container handle.
func (coh *ContHandle) EpochHold(e Epoch) (*EpochState, error) {
	var s EpochState
	rc, err := C.daos_epoch_discard(coh.H(), e.Native(), s.Pointer(), nil)
	if err = rc2err("daos_epoch_hold", rc, err); err != nil {
		return nil, err
	}
	return &s, nil

}

// EpochSlip increases the lowest referenced epoch of the container handle.
func (coh *ContHandle) EpochSlip(e Epoch) (*EpochState, error) {
	var s EpochState
	rc, err := C.daos_epoch_discard(coh.H(), e.Native(), s.Pointer(), nil)
	if err = rc2err("daos_epoch_slip", rc, err); err != nil {
		return nil, err
	}
	return &s, nil

}

// EpochCommit commits an epoch for the container handle.
func (coh *ContHandle) EpochCommit(e Epoch) (*EpochState, error) {
	var s EpochState
	rc, err := C.daos_epoch_discard(coh.H(), e.Native(), s.Pointer(), nil)
	if err = rc2err("daos_epoch_commit", rc, err); err != nil {
		return nil, err
	}
	return &s, nil

}

// EpochWait waits an epoch to be committed.
func (coh *ContHandle) EpochWait(e Epoch) (*EpochState, error) {
	var s EpochState
	rc, err := C.daos_epoch_discard(coh.H(), e.Native(), s.Pointer(), nil)
	if err = rc2err("daos_epoch_wait", rc, err); err != nil {
		return nil, err
	}
	return &s, nil

}
