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

const (
	// RecAny signals that the record needs to be prefetched in order
	// to determine its size
	RecAny = C.DAOS_REC_ANY
	// EpochMax is the highest possible epoch -- usually used in read
	// operations to get the latest data regardless of what's committed
	// NB: Can't use C.DAOS_EPOCH_MAX because it's interpreted as -1
	EpochMax = Epoch(1<<64 - 1)
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
	return rc2err("daos_init", rc, err)
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

// UUID returns the Pool's UUID as a string
func (info *PoolInfo) UUID() string {
	if info == nil {
		return ""
	}
	return uuid2str(info.pi_uuid[:])
}

// NumTargets returns the number of targets in the Pool
func (info *PoolInfo) NumTargets() int {
	if info == nil {
		return 0
	}
	return int(info.pi_ntargets)
}

// NumDisabled returns the number of disabled targets in the Pool
func (info *PoolInfo) NumDisabled() int {
	if info == nil {
		return 0
	}
	return int(info.pi_ndisabled)
}

// Mode returns the Pool's operating mode
func (info *PoolInfo) Mode() uint32 {
	if info == nil {
		return 0
	}
	return uint32(info.pi_mode)
}

// Space returns the amount of space allocated for the Pool
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
	// ContOpenRO indicates that the container should be opened Read-Only
	ContOpenRO = C.DAOS_COO_RO
	// ContOpenRW indicates that the container should be opened Read/Write
	ContOpenRW = C.DAOS_COO_RW
	// ContOpenNoSlip indicates that the container should not allow
	// slip operations to succeed
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

// Pointer returns a pointer to the native C-type epoch
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

// UUID returns the container's UUID as a string
func (info *ContInfo) UUID() string {
	if info == nil {
		return ""
	}
	return uuid2str(info.ci_uuid[:])
}

// EpochState returns a pointer to the container's current EpochState
func (info *ContInfo) EpochState() *EpochState {
	if info == nil {
		return nil
	}
	es := EpochState(info.ci_epoch_state)
	return &es
}

// Snapshots returns a slice of Epochs representing stable snapshots
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

// Hash wraps a C.daos_hash_out_t
type Hash C.daos_hash_out_t

// OClassID wraps a C.daos_oclass_id_t
type OClassID C.daos_oclass_id_t

// ObjectID wraps a C.daos_obj_id_t
type ObjectID C.daos_obj_id_t

func (o ObjectID) String() string {
	return fmt.Sprintf("0x%x.0x%x.0x%x", o.hi, o.mid, o.lo)
}

// Native returns a C.daos_obj_id_t
func (o *ObjectID) Native() C.daos_obj_id_t {
	return C.daos_obj_id_t(*o)
}

// Pointer returns a pointer to a C.daos_obj_id_t
func (o *ObjectID) Pointer() *C.daos_obj_id_t {
	return (*C.daos_obj_id_t)(o)
}

// Hi returns the top part of the ObjectID
func (o *ObjectID) Hi() uint32 {
	// top half of .hi is reserved
	return uint32(o.hi)
}

// Mid returns the middle part of the ObjectID
func (o *ObjectID) Mid() uint64 {
	return uint64(o.mid)
}

// Lo returns the lower part of the ObjectID
func (o *ObjectID) Lo() uint64 {
	return uint64(o.lo)
}

// Class returns the OClassID for the ObjectID
func (o *ObjectID) Class() OClassID {
	c := C.daos_obj_id2class(o.Native())
	return OClassID(c)
}

const oidBinaryVersion byte = 1

// MarshalBinary implements the encoding.BinaryMarshaler interface
func (o *ObjectID) MarshalBinary() ([]byte, error) {
	return []byte{
		oidBinaryVersion,
		byte(o.hi >> 56),
		byte(o.hi >> 48),
		byte(o.hi >> 40),
		byte(o.hi >> 32),
		byte(o.hi >> 24),
		byte(o.hi >> 16),
		byte(o.hi >> 8),
		byte(o.hi),
		byte(o.mid >> 56),
		byte(o.mid >> 48),
		byte(o.mid >> 40),
		byte(o.mid >> 32),
		byte(o.mid >> 24),
		byte(o.mid >> 16),
		byte(o.mid >> 8),
		byte(o.mid),
		byte(o.lo >> 56),
		byte(o.lo >> 48),
		byte(o.lo >> 40),
		byte(o.lo >> 32),
		byte(o.lo >> 24),
		byte(o.lo >> 16),
		byte(o.lo >> 8),
		byte(o.lo),
	}, nil
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface
func (o *ObjectID) UnmarshalBinary(data []byte) error {
	buf := data
	if len(buf) == 0 {
		return errors.Errorf("ObjectID.UnmarshalBinary: no data")
	}

	if buf[0] != oidBinaryVersion {
		return errors.Errorf("ObjectID.UnmarshalBinary: unsupported version")
	}

	if len(buf) != /*version*/ 1+ /*hi+mid+lo*/ 24 {
		return errors.Errorf("ObjectID.UnmarshalBinary: invalid length")
	}

	buf = buf[1:]
	o.hi = C.uint64_t(buf[7]) | C.uint64_t(buf[6])<<8 | C.uint64_t(buf[5])<<16 |
		C.uint64_t(buf[4])<<24 | C.uint64_t(buf[3])<<32 | C.uint64_t(buf[2])<<40 |
		C.uint64_t(buf[1])<<48 | C.uint64_t(buf[0])<<56
	buf = buf[8:]
	o.mid = C.uint64_t(buf[7]) | C.uint64_t(buf[6])<<8 | C.uint64_t(buf[5])<<16 |
		C.uint64_t(buf[4])<<24 | C.uint64_t(buf[3])<<32 | C.uint64_t(buf[2])<<40 |
		C.uint64_t(buf[1])<<48 | C.uint64_t(buf[0])<<56
	buf = buf[8:]
	o.lo = C.uint64_t(buf[7]) | C.uint64_t(buf[6])<<8 | C.uint64_t(buf[5])<<16 |
		C.uint64_t(buf[4])<<24 | C.uint64_t(buf[3])<<32 | C.uint64_t(buf[2])<<40 |
		C.uint64_t(buf[1])<<48 | C.uint64_t(buf[0])<<56

	return nil
}

// MarshalJSON marshals the ObjectID to a JSON-formatted []byte
func (o *ObjectID) MarshalJSON() ([]byte, error) {
	return []byte(`"` + o.String() + `"`), nil
}

// UnmarshalJSON unmarshals the ObjectID from a JSON-formatted []byte
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

// Set attempts to parse the supplied string as an ObjectID and replaces
// the pointer if successful
func (o *ObjectID) Set(value string) error {
	oid, err := ParseOID(value)
	if err != nil {
		return err
	}
	*o = *oid
	return nil
}

// ParseOID attempts to parse the supplied string as an ObjectID
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

// Native returns a C.daos_oclass_id_t
func (c OClassID) Native() C.daos_oclass_id_t {
	return C.daos_oclass_id_t(c)
}

// Set attempts to parse the supplied string as an OClassID
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
	// ClassUnknown is an unknown Object Class
	ClassUnknown = OClassID(C.DAOS_OC_UNKNOWN)
	// ClassTinyRW is tiny i/o
	ClassTinyRW = OClassID(C.DAOS_OC_TINY_RW)
	// ClassSmallRW is mall i/o
	ClassSmallRW = OClassID(C.DAOS_OC_SMALL_RW)
	// ClassLargeRW is large i/o
	ClassLargeRW = OClassID(C.DAOS_OC_LARGE_RW)
	// ClassRepl2RW is 2-way replicated i/o
	ClassRepl2RW = OClassID(C.DAOS_OC_REPL_2_RW)
	// ClassReplMaxRW is fully-replicated i/o
	ClassReplMaxRW     = OClassID(C.DAOS_OC_REPL_MAX_RW)
	endOfObjectClasses = ClassReplMaxRW
)

// ObjectClassList returns a slice of known ObjectClasses
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

// Pointer returns a *C.daos_obj_attr_t
func (oa *ObjectAttribute) Pointer() *C.daos_obj_attr_t {
	return (*C.daos_obj_attr_t)(oa)
}

// H returns a C.daos_daos_handle_t
func (oh *ObjectHandle) H() C.daos_handle_t {
	return C.daos_handle_t(*oh)
}

// Pointer returns a *C.daos_daos_handle_t
func (oh *ObjectHandle) Pointer() *C.daos_handle_t {
	return (*C.daos_handle_t)(oh)
}

// Zero resets the handle to the invalid state
func (oh *ObjectHandle) Zero() {
	oh.Pointer().cookie = 0
}

// ObjectDeclare declares the object with the specified attributes
func (coh *ContHandle) ObjectDeclare(oid *ObjectID, e Epoch, oa *ObjectAttribute) error {
	rc, err := C.daos_obj_declare(coh.H(), oid.Native(), e.Native(), oa.Pointer(), nil)
	return rc2err("daos_obj_declare", rc, err)
}

// ObjectOpenFlag specifies the object open flags
type ObjectOpenFlag uint

const (
	// ObjOpenRO indicates that the object should be opened Read Only
	ObjOpenRO = ObjectOpenFlag(C.DAOS_OO_RO)
	// ObjOpenRW indicates that the object should be opened Read/Write
	ObjOpenRW = ObjectOpenFlag(C.DAOS_OO_RW)
	// ObjOpenExcl indicates that the object should be opened for exclusive i/o
	ObjOpenExcl = ObjectOpenFlag(C.DAOS_OO_EXCL)
	// ObjOpenIORand indicates that the object will be used for random i/o
	ObjOpenIORand = ObjectOpenFlag(C.DAOS_OO_IO_RAND)
	// ObjOpenIOSeq indicates that the object will be used for sequential i/o
	ObjOpenIOSeq = ObjectOpenFlag(C.DAOS_OO_IO_SEQ)
)

// ObjectOpen opens the specified object with the supplied flags
func (coh *ContHandle) ObjectOpen(oid *ObjectID, e Epoch, mode ObjectOpenFlag) (*ObjectHandle, error) {
	var oh ObjectHandle
	rc, err := C.daos_obj_open(coh.H(), oid.Native(), e.Native(), C.uint(mode), oh.Pointer(), nil)
	if err := rc2err("daos_obj_open", rc, err); err != nil {
		return nil, err
	}
	return &oh, nil
}

// Close closes the object
func (oh *ObjectHandle) Close() error {
	if HandleIsInvalid(oh) {
		return nil
	}

	rc, err := C.daos_obj_close(oh.H(), nil)
	oh.Zero()
	return rc2err("daos_obj_close", rc, err)
}

// Punch performs a punch operation on the object
func (oh *ObjectHandle) Punch(e Epoch) error {
	rc, err := C.daos_obj_punch(oh.H(), e.Native(), nil)
	return rc2err("daos_obj_punch", rc, err)
}

// Query queries the object's attributes
func (oh *ObjectHandle) Query(e Epoch) (*ObjectAttribute, error) {
	var oa ObjectAttribute
	rc, err := C.daos_obj_query(oh.H(), e.Native(), oa.Pointer(), nil, nil)
	if err := rc2err("daos_obj_punch", rc, err); err != nil {
		return nil, err
	}
	return &oa, nil
}

type (
	// ObjectAttribute is an object attribute
	ObjectAttribute C.daos_obj_attr_t

	// ObjectHandle refers to an open object
	ObjectHandle C.daos_handle_t
)

// Put sets the first record of a-key to value, with record size is len(value).
func (oh *ObjectHandle) Put(e Epoch, dkey string, akey string, value []byte) error {
	return oh.Putb(e, []byte(dkey), []byte(akey), value)
}

// Putb sets the first record of a-key to value, with record size is len(value).
func (oh *ObjectHandle) Putb(e Epoch, dkey []byte, akey []byte, value []byte) error {
	ir := NewIoRequest(dkey, NewSingleRecordRequest(akey, value))
	defer ir.Free()

	return oh.Update(e, ir)
}

// PutKeys stores a map of akey->val on the specified dkey
func (oh *ObjectHandle) PutKeys(e Epoch, dkey string, akeys map[string][]byte) error {
	ir := NewIoRequest([]byte(dkey))
	defer ir.Free()

	for k := range akeys {
		ir.AddRecordRequest(NewSingleRecordRequest([]byte(k), akeys[k]))
	}

	return oh.Update(e, ir)
}

// Get returns first record for a-key.
func (oh *ObjectHandle) Get(e Epoch, dkey string, akey string) ([]byte, error) {
	return oh.Getb(e, []byte(dkey), []byte(akey))
}

// Getb returns first record for a-key.
func (oh *ObjectHandle) Getb(e Epoch, dkey []byte, akey []byte) ([]byte, error) {
	ir := NewIoRequest(dkey, NewSingleRecordRequest(akey))
	defer ir.Free()

	if err := oh.Fetch(e, ir); err != nil {
		return nil, err
	}

	if ir.Records[0].Size() == 0 {
		return nil, errors.Errorf("Got 0-length record for %s/%s", dkey, akey)
	}

	return ir.Records[0].Buffers()[0], nil
}

// GetKeys returns first record for each a-key available in the specified epoch.
func (oh *ObjectHandle) GetKeys(e Epoch, dkey string, akeys []string) (map[string][]byte, error) {
	ir := NewIoRequest([]byte(dkey))
	defer ir.Free()

	for _, k := range akeys {
		ir.AddRecordRequest(NewSingleRecordRequest([]byte(k)))
	}

	if err := oh.Fetch(e, ir); err != nil {
		return nil, err
	}

	kv := make(map[string][]byte)
	for _, rr := range ir.Records {
		if rr.Size() == 0 {
			continue
		}
		kv[rr.StringKey()] = rr.Buffers()[0]
	}

	return kv, nil
}
