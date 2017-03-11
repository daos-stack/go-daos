package daos

//
// #cgo LDFLAGS:  -ldaos  -lcrt_util -lcrt -ldaos_common -ldaos_tier -luuid
// #include <stdlib.h>
// #include <daos.h>
// #include <daos/common.h>
//
import "C"

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"unsafe"

	"github.com/pborman/uuid"
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

// Handle is an interface for DAOS handle types
type Handle interface {
	Pointer() *C.daos_handle_t
	H() C.daos_handle_t
	Zero()
}

// HandleIsInvalid returns a boolean indicating whether or not the
// handle is invalid.
func HandleIsInvalid(h Handle) bool {
	return bool(C.daos_handle_is_inval(h.H()))
}

// Returns an failure if rc != 0. If err is already set
// then it is wrapped, otherwise it is ignored.
func rc2err(label string, rc C.int, err error) error {
	if rc != 0 {
		if rc < 0 {
			rc = -rc
		}
		e := Error(rc)
		return errors.Errorf("%s: %s", label, e)
	}
	return nil
}

// uuid2str converts a buffer that may or may not have come from C land
// so we must allocate Go memory here just in case.
func uuid2str(cuuid []C.uchar) string {
	buf := C.GoBytes(unsafe.Pointer(&cuuid[0]), C.int(len(cuuid)))
	id := uuid.UUID([]byte(buf))
	return id.String()
}

// Analog to uuid2str but returns *C.uchar for convenience.
// Returns uuid in Go memory
func str2uuid(s string) (*C.uchar, error) {
	id := uuid.Parse(s)
	if id == nil {
		return nil, errors.New("Unable to parse UUID")
	}
	return (*C.uchar)(unsafe.Pointer(&id[0])), nil
}

// allocRecx allocates an array of  ranks in C memory
// return value must be released with C.free
func allocRanks(nr C.uint32_t) *C.daos_rank_t {
	return (*C.daos_rank_t)(C.calloc(C.size_t(nr), C.size_t(unsafe.Sizeof(C.daos_rank_t(0)))))
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
	ranks := allocRanks(nranks)
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

// Zero resets the handle to the invalid state
func (poh *PoolHandle) Zero() {
	poh.Pointer().cookie = 0
}

// Disconnect closes the pool handle.
func (poh *PoolHandle) Disconnect() error {
	rc, err := C.daos_pool_disconnect(poh.H(), nil)
	poh.Zero()
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
	tgts.rl_ranks = allocRanks(tgts.rl_nr.num)
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

const (
	ContOpenRO     = C.DAOS_COO_RO
	ContOpenRW     = C.DAOS_COO_RW
	ContOpenNoSlip = C.DAOS_COO_NOSLIP
)

// H returns C-typed handle
func (coh *ContHandle) H() C.daos_handle_t {
	return C.daos_handle_t(*coh)
}

// Pointer returns pointer C-typed container handle
func (coh *ContHandle) Pointer() *C.daos_handle_t {
	return (*C.daos_handle_t)(coh)
}

// Zero resets the handle to the invalid state
func (coh *ContHandle) Zero() {
	coh.Pointer().cookie = 0
}

// Open a the container identified by the UUID
func (poh *PoolHandle) Open(uuid string, flags int) (*ContHandle, error) {
	cuuid, err := str2uuid(uuid)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to parse %s", uuid)
	}

	var coh ContHandle
	rc, err := C.daos_cont_open(poh.H(), cuuid, C.uint(flags), coh.Pointer(), nil, nil)
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

	coh.Zero()
	return nil
}

// Native returns native C-type epoch
func (e Epoch) Native() C.daos_epoch_t {
	return C.daos_epoch_t(e)
}

// Native returns native C-type epoch
func (e *Epoch) Pointer() *C.daos_epoch_t {
	return (*C.daos_epoch_t)(e)
}

func (e Epoch) String() string {
	return fmt.Sprintf("0x%x", uint64(e))
}

// Pointer turns C-typed EpochState pointer
func (s *EpochState) Pointer() *C.daos_epoch_state_t {
	return (*C.daos_epoch_state_t)(s)
}

// Close the container handle
func (coh *ContHandle) Close() error {
	rc, err := C.daos_cont_close(coh.H(), nil)
	return rc2err("daos_cont_close", rc, err)
}

// Info returns current container info
func (coh *ContHandle) Info() (*ContInfo, error) {
	var info ContInfo
	rc, err := C.daos_cont_query(coh.H(), (*C.daos_cont_info_t)(&info), nil)

	if err = rc2err("daos_cont_query", rc, err); err != nil {
		return nil, err
	}
	return &info, nil
}

func (info *ContInfo) UUID() string {
	if info == nil {
		return ""
	}
	return uuid2str(info.ci_uuid[:])
}

func (info *ContInfo) EpochState() *EpochState {
	if info == nil {
		return nil
	}
	es := EpochState(info.ci_epoch_state)
	return &es
}

func (info *ContInfo) Snapshots() []Epoch {
	if info == nil {
		return nil
	}

	return nil
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

func (s *EpochState) String() string {
	return fmt.Sprintf("HCE %s, LRE %s, LHE %s, GHCE %s, GLRE %s, GHPCE %s",
		s.HCE(), s.LRE(), s.LHE(), s.GHCE(), s.GLRE(), s.GHPCE())
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
	var es EpochState
	rc, err := C.daos_epoch_flush(coh.H(), e.Native(), es.Pointer(), nil)
	if err = rc2err("daos_epoch_flush", rc, err); err != nil {
		return nil, err
	}
	return &es, nil

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
func (coh *ContHandle) EpochHold(e Epoch) (Epoch, error) {
	var s EpochState
	rc, err := C.daos_epoch_hold(coh.H(), e.Pointer(), s.Pointer(), nil)
	if err = rc2err("daos_epoch_hold", rc, err); err != nil {
		return 0, err
	}
	return e, nil

}

// EpochSlip increases the lowest referenced epoch of the container handle.
func (coh *ContHandle) EpochSlip(e Epoch) (*EpochState, error) {
	var s EpochState
	rc, err := C.daos_epoch_slip(coh.H(), e.Native(), s.Pointer(), nil)
	if err = rc2err("daos_epoch_slip", rc, err); err != nil {
		return nil, err
	}
	return &s, nil

}

// EpochCommit commits an epoch for the container handle.
func (coh *ContHandle) EpochCommit(e Epoch) (*EpochState, error) {
	var s EpochState
	rc, err := C.daos_epoch_commit(coh.H(), e.Native(), s.Pointer(), nil)
	if err = rc2err("daos_epoch_commit", rc, err); err != nil {
		return nil, err
	}
	return &s, nil

}

// EpochWait waits an epoch to be committed.
func (coh *ContHandle) EpochWait(e Epoch) (*EpochState, error) {
	var s EpochState
	rc, err := C.daos_epoch_wait(coh.H(), e.Native(), s.Pointer(), nil)
	if err = rc2err("daos_epoch_wait", rc, err); err != nil {
		return nil, err
	}
	return &s, nil

}

type Hash C.daos_hash_out_t

type OClassID C.daos_oclass_id_t
type ObjectID C.daos_obj_id_t

func (o ObjectID) String() string {
	return fmt.Sprintf("0x%x.0x%x.0x%x", o.hi, o.mid, o.lo)
}

func (o *ObjectID) Native() C.daos_obj_id_t {
	return C.daos_obj_id_t(*o)
}

func (o *ObjectID) Pointer() *C.daos_obj_id_t {
	return (*C.daos_obj_id_t)(o)
}

func (o *ObjectID) Hi() uint32 {
	// top half of .hi is reserved
	return uint32(o.hi)
}

func (o *ObjectID) Mid() uint64 {
	return uint64(o.mid)
}

func (o *ObjectID) Lo() uint64 {
	return uint64(o.lo)
}

func (o *ObjectID) Class() OClassID {
	c := C.daos_obj_id2class(o.Native())
	return OClassID(c)
}

func (o *ObjectID) MarshalJSON() ([]byte, error) {
	return []byte(`"` + o.String() + `"`), nil
}

func (o *ObjectID) UnmarshalJSON(b []byte) error {
	if b[0] == '"' {
		b = b[1 : len(b)-1]
	}
	oid, err := ParseOID(string(b))
	if err != nil {
		return err
	}
	*o = *oid
	return nil
}

func (o *ObjectID) Set(value string) error {
	oid, err := ParseOID(value)
	if err != nil {
		return err
	}
	*o = *oid
	return nil
}

func ParseOID(s string) (*ObjectID, error) {
	var oc OClassID
	var hi uint32
	var mid, lo uint64

	i := strings.Index(s, ":")
	if i >= 0 {
		c := s[0:i]
		err := oc.Set(c)
		if err != nil {
			return nil, errors.Wrap(err, "invalid class")
		}
		s = s[i+1 : len(s)]
	}
	parts := strings.Split(s, ".")
	if len(parts) == 3 {
		tmp, err := strconv.ParseUint(parts[0], 0, 64)
		if err != nil {
			return nil, errors.Wrap(err, "hi")
		}
		oc = OClassID(tmp >> 32)
		hi = uint32(tmp & 0x00000000ffffffff)
		parts = parts[1:len(parts)]
	}
	if len(parts) == 2 {
		tmp, err := strconv.ParseUint(parts[0], 0, 64)
		if err != nil {
			return nil, errors.Wrap(err, "mid")
		}
		mid = tmp
		parts = parts[1:len(parts)]
	}
	if len(parts) == 1 {
		tmp, err := strconv.ParseUint(parts[0], 0, 64)
		if err != nil {
			return nil, errors.Wrap(err, "lo")
		}
		lo = tmp
	}

	if oc == 0 {
		oc = ClassLargeRW
	}
	return ObjectIDInit(hi, mid, lo, oc), nil
}

func (c OClassID) Native() C.daos_oclass_id_t {
	return C.daos_oclass_id_t(c)
}

func (c *OClassID) Set(value string) error {
	for _, cls := range ObjectClassList() {
		if strings.ToUpper(value) == strings.ToUpper(cls.String()) {
			*c = cls
			return nil
		}
	}

	return errors.Errorf("Unable to find DAOS Object class %q", value)
}

func (c OClassID) String() string {
	switch c {
	case ClassTinyRW:
		return "TinyRW"
	case ClassSmallRW:
		return "SmallRW"
	case ClassLargeRW:
		return "LargeRW"
	case ClassRepl2RW:
		return "Repl2RW"
	case ClassReplMaxRW:
		return "ReplMaxRW"
	default:
		return "Unknown"
	}
}

const (
	startOfObjectClasses = ClassUnknown
	ClassUnknown         = OClassID(C.DAOS_OC_UNKNOWN)
	ClassTinyRW          = OClassID(C.DAOS_OC_TINY_RW)
	ClassSmallRW         = OClassID(C.DAOS_OC_SMALL_RW)
	ClassLargeRW         = OClassID(C.DAOS_OC_LARGE_RW)
	ClassRepl2RW         = OClassID(C.DAOS_OC_REPL_2_RW)
	ClassReplMaxRW       = OClassID(C.DAOS_OC_REPL_MAX_RW)
	endOfObjectClasses   = ClassReplMaxRW
)

func ObjectClassList() []OClassID {
	var classes []OClassID
	for cls := startOfObjectClasses; cls <= endOfObjectClasses; cls++ {
		classes = append(classes, cls)
	}
	return classes
}

// GenerateOID returns a random OID
// The ID is derived from a Random  UUID, so should
// reasonably unique.
func GenerateOID(oc OClassID) *ObjectID {
	id := uuid.NewRandom()
	buf := bytes.NewReader([]byte(id))
	var lomed struct{ Med, Lo uint64 }
	binary.Read(buf, binary.BigEndian, &lomed)
	return ObjectIDInit(0, lomed.Med, lomed.Lo, oc)
}

// ObjectIDInit initializes an ObjectID
func ObjectIDInit(hi uint32, mid, lo uint64, class OClassID) *ObjectID {
	var oid ObjectID
	oid.hi = C.uint64_t(hi)
	oid.mid = C.uint64_t(mid)
	oid.lo = C.uint64_t(lo)

	C.daos_obj_id_generate(oid.Pointer(), class.Native())
	return &oid
}

func (oa *ObjectAttribute) Pointer() *C.daos_obj_attr_t {
	return (*C.daos_obj_attr_t)(oa)
}

func (oh *ObjectHandle) H() C.daos_handle_t {
	return C.daos_handle_t(*oh)
}

func (oh *ObjectHandle) Pointer() *C.daos_handle_t {
	return (*C.daos_handle_t)(oh)
}

// Zero resets the handle to the invalid state
func (oh *ObjectHandle) Zero() {
	oh.Pointer().cookie = 0
}

func (coh *ContHandle) ObjectDeclare(oid *ObjectID, e Epoch, oa *ObjectAttribute) error {
	rc, err := C.daos_obj_declare(coh.H(), oid.Native(), e.Native(), oa.Pointer(), nil)
	return rc2err("daos_obj_declare", rc, err)
}

type ObjectOpenFlag uint

const (
	ObjOpenRO     = ObjectOpenFlag(C.DAOS_OO_RO)
	ObjOpenRW     = ObjectOpenFlag(C.DAOS_OO_RW)
	ObjOpenExcl   = ObjectOpenFlag(C.DAOS_OO_EXCL)
	ObjOpenIORand = ObjectOpenFlag(C.DAOS_OO_IO_RAND)
	ObjOpenIOSeq  = ObjectOpenFlag(C.DAOS_OO_IO_SEQ)
)

func (coh *ContHandle) ObjectOpen(oid *ObjectID, e Epoch, mode ObjectOpenFlag) (*ObjectHandle, error) {
	var oh ObjectHandle
	rc, err := C.daos_obj_open(coh.H(), oid.Native(), e.Native(), C.uint(mode), oh.Pointer(), nil)
	if err := rc2err("daos_obj_open", rc, err); err != nil {
		return nil, err
	}
	return &oh, nil
}

func (oh *ObjectHandle) Close() error {
	if HandleIsInvalid(oh) {
		return nil
	}

	rc, err := C.daos_obj_close(oh.H(), nil)
	oh.Zero()
	return rc2err("daos_obj_close", rc, err)
}

func (oh *ObjectHandle) Punch(e Epoch) error {
	rc, err := C.daos_obj_punch(oh.H(), e.Native(), nil)
	return rc2err("daos_obj_punch", rc, err)
}

func (oh *ObjectHandle) Query(e Epoch) (*ObjectAttribute, error) {
	var oa ObjectAttribute
	rc, err := C.daos_obj_query(oh.H(), e.Native(), oa.Pointer(), nil, nil)
	if err := rc2err("daos_obj_punch", rc, err); err != nil {
		return nil, err
	}
	return &oa, nil
}

type (
	// ObjectAttribute is an attribute
	ObjectAttribute C.daos_obj_attr_t

	// ObjectHandle refers to an open object
	ObjectHandle C.daos_handle_t
)

// ByteToAttrKey creates a C buffer with copy of string and returns a go IoVec.
// Allocates memory in C, return value must be released with Free()
func ByteToAttrKey(buf []byte) *AttrKey {
	var iov IoVec
	return (*AttrKey)(copyToIov(&iov, buf))
}

// ByteToDistKey creates a C buffer with copy of string and returns a go IoVec.
// Allocates memory in C, return value must be released with Free()
func ByteToDistKey(buf []byte) *DistKey {
	var iov IoVec
	return (*DistKey)(copyToIov(&iov, buf))
}

func copyToIov(iov *IoVec, value []byte) *IoVec {
	n := C.size_t(len(value))
	iov.iov_len = C.daos_size_t(n)
	iov.iov_buf_len = C.daos_size_t(n)
	iov.iov_buf = C.CBytes(value)
	return iov
}

func (k *IoVec) Free() {
	if k != nil && k.iov_buf != nil {
		C.free(k.iov_buf)
		k.iov_buf = nil
	}
}

func (iov *IoVec) Pointer() *C.daos_iov_t {
	return (*C.daos_iov_t)(iov)

}

func (dk *DistKey) Free() {
	(*IoVec)(dk).Free()
}

func (dk *DistKey) Pointer() *C.daos_dkey_t {
	return (*C.daos_dkey_t)(dk)
}

func (ak *AttrKey) Free() {
	(*IoVec)(ak).Free()
}

func (ak *AttrKey) Pointer() *C.daos_akey_t {
	return (*C.daos_akey_t)(ak)
}

func (ak *AttrKey) Native() C.daos_akey_t {
	return C.daos_akey_t(*ak)
}

// Put sets the first record of a-key to value, with record size is len(value).
func (oh *ObjectHandle) Put(e Epoch, dkey string, akey string, value []byte) error {
	return oh.Putb(e, []byte(dkey), []byte(akey), value)
}

// Putb sets the first record of a-key to value, with record size is len(value).
func (oh *ObjectHandle) Putb(e Epoch, dkey []byte, akey []byte, value []byte) error {
	kr := NewKeyRequest(akey)
	kr.Put(0, 1, uint64(len(value)), value)

	return oh.Update(e, dkey, []*KeyRequest{kr})
}

func (oh *ObjectHandle) PutKeys(e Epoch, dkey string, akeys map[string][]byte) error {
	var request []*KeyRequest

	for k := range akeys {
		kr := NewKeyRequest([]byte(k))
		kr.Put(0, 1, uint64(len(akeys[k])), akeys[k])
		request = append(request, kr)

	}

	return oh.Update(e, []byte(dkey), request)
}

const (
	RecAny   = C.DAOS_REC_ANY
	EpochMax = Epoch(0xffffffffffffffff)
)

// Get returns first record for a-key.
func (oh *ObjectHandle) Get(e Epoch, dkey string, akey string) ([]byte, error) {
	return oh.Getb(e, []byte(dkey), []byte(akey))
}

// Getb returns first record for a-key.
func (oh *ObjectHandle) Getb(e Epoch, dkey []byte, akey []byte) ([]byte, error) {
	kr := NewKeyRequest(akey)
	kr.Get(0, 1, RecAny)

	err := oh.Fetch(e, dkey, []*KeyRequest{kr})
	if err != nil {
		return nil, err
	}

	if len(kr.Buffers) > 0 {
		return kr.Buffers[0], nil
	}

	return nil, nil
}

// GetKeys returns first record for each a-key available in the specified epoch.
func (oh *ObjectHandle) GetKeys(e Epoch, dkey string, akeys []string) (map[string][]byte, error) {
	kv := make(map[string][]byte)
	var request []*KeyRequest

	for k := range akeys {
		kr := NewKeyRequest([]byte(akeys[k]))
		kr.Get(0, 1, RecAny)
		request = append(request, kr)

	}

	err := oh.Inspect(e, []byte(dkey), request)
	if err != nil {
		return nil, err
	}

	var fetch []*KeyRequest

	// Only fetch akeys that have a size
	for _, req := range request {
		for _, extent := range req.Extents {
			if extent.RecSize != RecAny {
				fetch = append(fetch, req)
			}
		}
	}

	// If no a-keys are available, return empty result.
	if len(fetch) == 0 {
		return kv, nil
	}

	err = oh.Fetch(e, []byte(dkey), fetch)
	if err != nil {
		return nil, err
	}

	for _, kr := range request {
		if len(kr.Buffers) > 0 {
			kv[string(kr.Attr)] = kr.Buffers[0]
		}
	}

	return kv, nil
}
