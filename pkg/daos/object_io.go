package daos

// #include <stdlib.h>
// #include <daos.h>
// #include <daos/common.h>
import "C"

import (
	"fmt"
	"unsafe"

	"github.com/pkg/errors"

	"github.com/intel-hpdd/logging/debug"
)

type (
	// RequestBuffers is a slice of byte slices
	RequestBuffers [][]byte

	// SGList wraps a C.daos_sg_list_t
	SGList struct {
		complete bool
		buffers  RequestBuffers
		sgl      C.daos_sg_list_t
	}

	// IoVector wraps a C.daos_iov_t
	IoVector C.daos_iov_t

	// SingleRecordRequest represents an IOD for a single record
	// fetch/update on an akey
	SingleRecordRequest struct {
		// Key is the attribute key of the record
		Key []byte
		// Buffer is Go-allocated memory for reading or writing
		// the record
		Buffer []byte

		// Hang on to pointers of stuff we've allocated in
		// C-land so that we can free it later
		akey   *IoVector
		iod    *C.daos_iod_t
		sgl    *C.daos_sg_list_t
		iovBuf unsafe.Pointer
	}

	// Extent is a range of contiguous records of the same size inside
	// an array
	Extent struct {
		// Index of the first record within the extent
		Index uint64
		// Count is the number of contiguous records in the extent
		Count uint64
	}

	// ArrayRecordRequest represents an IOD for a multi-record
	// fetch/update on an akey
	ArrayRecordRequest struct {
		// Key is the attribute key of the record
		Key []byte
		// Extents is a slice of extents, where each extent
		// defines the index of the first record in the extent
		// and the number of records to access
		Extents []Extent
		// Epochs is an optional slice of epochs which if supplied
		// will override the epoch given to daos_obj_(fetch|update)
		Epochs []Epoch
		// Buffers is a slice of Go-allocated memory for reading or
		// writing the records
		Buffers RequestBuffers
	}

	// RecordRequest defines an interface which is implemented by
	// either ArrayRecordRequest or SingleRecordRequest
	RecordRequest interface {
		Name() *C.daos_key_t
		StringKey() string
		Buffers() RequestBuffers
		Size() uint64
		SetSize(uint64)
		Free()
		Complete() error
		PrepareIOD(*C.daos_iod_t)
		PrepareSGL(*C.daos_sg_list_t)
	}

	// IoRequest encapsulates a set of record requests which are co-located
	// on the same distribution key
	IoRequest struct {
		DistKey []byte
		Records []RecordRequest

		// Hang on to this so we can free it later
		dkey *IoVector
	}
)

// NewIoVector returns a new *IoVector for the given []byte
func NewIoVector(buf []byte) *IoVector {
	return (*IoVector)(&C.daos_iov_t{
		iov_buf:     C.CBytes(buf),
		iov_buf_len: C.daos_size_t(len(buf)),
		iov_len:     C.daos_size_t(len(buf)),
	})
}

// Free releases the allocated C memory for the IoVector
func (iov *IoVector) Free() {
	if iov != nil && iov.iov_buf != nil {
		C.free(iov.iov_buf)
	}
}

func (iov *IoVector) String() string {
	if iov == nil {
		return ""
	}
	return C.GoStringN((*C.char)(iov.iov_buf), C.int(iov.iov_len))
}

// NewSGList creates a DAOS Scatter-Gather list set up to do I/O based on
// the supplied slice of buffers
func NewSGList(bufs RequestBuffers) *SGList {
	nr := C.int(len(bufs))
	iovPtr := (*C.daos_iov_t)(C.malloc(C.size_t(C.sizeof_daos_iov_t * nr)))
	C.memset(unsafe.Pointer(iovPtr), 0, C.size_t(C.sizeof_daos_iov_t*nr))

	iovs := (*[1 << 30]C.daos_iov_t)(unsafe.Pointer(iovPtr))[:nr:nr]
	for i := range iovs {
		iovs[i].iov_len = C.daos_size_t(len(bufs[i]))
		iovs[i].iov_buf_len = C.daos_size_t(cap(bufs[i]))
		iovs[i].iov_buf = C.CBytes(bufs[i])
	}

	sgl := &SGList{
		buffers: bufs,
		sgl: C.daos_sg_list_t{
			sg_nr: C.daos_nr_t{
				num: C.uint32_t(nr),
			},
			sg_iovs: iovPtr,
		},
	}

	return sgl
}

// Complete does any required housekeeping after a fetch
func (sgl *SGList) Complete() error {
	if !sgl.complete {
		for i, iov := range sgl.iovs() {
			len := int(iov.iov_len)
			if len > cap(sgl.buffers[i]) {
				return errors.Errorf("Can't copy IOV with length %d into buffer with cap %d", len, cap(sgl.buffers[i]))
			}
			copy(sgl.buffers[i][:len], C.GoBytes(iov.iov_buf, C.int(iov.iov_len)))
		}
		sgl.complete = true
	}

	return nil
}

// Free releases all C-allocated memory used by the SGL
func (sgl *SGList) Free() {
	for _, iov := range sgl.iovs() {
		if iov.iov_buf != nil {
			C.free(unsafe.Pointer(iov.iov_buf))
		}
	}
	if sgl.sgl.sg_iovs != nil {
		C.free(unsafe.Pointer(sgl.sgl.sg_iovs))
	}
	sgl.sgl.sg_iovs = nil
}

func (sgl *SGList) iovs() []C.daos_iov_t {
	if sgl.sgl.sg_iovs == nil {
		return nil
	}
	nr := len(sgl.buffers)
	return (*[1 << 30]C.daos_iov_t)(unsafe.Pointer(sgl.sgl.sg_iovs))[:nr:nr]
}

// Pointer returns *C.daos_sg_list_t
func (sgl *SGList) Pointer() *C.daos_sg_list_t {
	cSgl := sgl.sgl
	return (*C.daos_sg_list_t)(&cSgl)
}

// NewSingleRecordRequest creates a new *SingleRecordRequest
func NewSingleRecordRequest(name []byte, buffers ...[]byte) *SingleRecordRequest {
	// IFF we've been given one buffer, i.e. for an Update or for a Fetch
	// when we know in advance how much we're fetching, use it. Otherwise,
	// set a placeholder buffer which will be replaced with a proper-sized
	// buffer.
	data := []byte{}
	if len(buffers) == 1 {
		data = buffers[0]
	}
	return &SingleRecordRequest{
		Key:    name,
		Buffer: data,
	}
}

// Free releases all allocated C memory used by the request
func (srr *SingleRecordRequest) Free() {
	if srr.sgl != nil {
		sglWrap := &SGList{
			sgl:     *srr.sgl,
			buffers: [][]byte{srr.Buffer},
		}
		sglWrap.Free()
	}
	if srr.akey != nil {
		srr.akey.Free()
	}
}

// StringKey returns the request's attribute name as a string
func (srr *SingleRecordRequest) StringKey() string {
	return (*IoVector)(srr.Name()).String()
}

// Name returns the request's attribute name as a *C.daos_key_t
func (srr *SingleRecordRequest) Name() *C.daos_key_t {
	if srr.akey == nil {
		srr.akey = NewIoVector(srr.Key)
	}
	return (*C.daos_key_t)(srr.akey)
}

// Buffers returns the request's single buffer wrapped in a slice
// in order to implement the RecordRequest interface
func (srr *SingleRecordRequest) Buffers() RequestBuffers {
	return [][]byte{srr.Buffer}
}

// Size returns the length of the request's buffer
func (srr *SingleRecordRequest) Size() uint64 {
	return uint64(len(srr.Buffer))
}

// SetSize adjusts the request's buffer to match the supplied size
func (srr *SingleRecordRequest) SetSize(newSize uint64) {
	// Replace a placeholder. NB: This could also truncate if
	// newSize == 0.
	if srr.Size() == 0 {
		srr.Buffer = make([]byte, newSize)
		return
	}

	// Shrink the buffer.
	if newSize < srr.Size() {
		srr.Buffer = srr.Buffer[:newSize]
		return
	}

	// Grow the buffer, copying anything in the existing buffer
	// into the new one.
	newBuf := make([]byte, newSize)
	copy(newBuf, srr.Buffer)
	srr.Buffer = newBuf
}

// PrepareIOD sets up the supplied *C.daos_iod_t to reflect this request's
// layout
func (srr *SingleRecordRequest) PrepareIOD(iod *C.daos_iod_t) {
	name := srr.Name()
	iod.iod_type = C.DAOS_IOD_SINGLE
	iod.iod_name = *name
	iod.iod_size = C.daos_size_t(srr.Size())
	iod.iod_nr = C.uint(1)
	srr.iod = iod
}

// PrepareSGL sets up the supplied *C.daos_sg_list_t to do I/O
func (srr *SingleRecordRequest) PrepareSGL(sgl *C.daos_sg_list_t) {
	iov := (*C.daos_iov_t)(C.malloc(C.sizeof_daos_iov_t))
	iov.iov_buf = C.CBytes(srr.Buffer)
	srr.iovBuf = iov.iov_buf
	iov.iov_len = C.daos_size_t(len(srr.Buffer))
	iov.iov_buf_len = C.daos_size_t(cap(srr.Buffer))

	sgl.sg_nr = C.daos_nr_t{
		num: 1,
	}
	sgl.sg_iovs = iov
	srr.sgl = sgl
}

// Complete performs any necessary housekeeping after an Inspect/Fetch
func (srr *SingleRecordRequest) Complete() error {
	// noop
	if srr.iod == nil && srr.sgl == nil {
		return nil
	}

	// post-inspect
	if srr.iod != nil && srr.sgl == nil {
		srr.SetSize(uint64(srr.iod.iod_size))
		return nil
	}

	// post-fetch
	if srr.iod != nil && srr.sgl != nil {
		nr := int(srr.sgl.sg_nr.num_out)
		if nr != 1 {
			if nr == 0 {
				srr.SetSize(0)
				return nil
			}
			return fmt.Errorf("Expected 1 SGL, found %d", nr)
		}
		copy(srr.Buffer,
			C.GoBytes(
				srr.sgl.sg_iovs.iov_buf,
				C.int(srr.sgl.sg_iovs.iov_len),
			),
		)
		return nil
	}

	return errors.New("unknown state in Complete()")
}

// NewIoRequest creates a new *IoRequest
func NewIoRequest(key []byte, records ...RecordRequest) *IoRequest {
	return &IoRequest{
		DistKey: key,
		Records: records,
	}
}

// AddRecordRequest adds a RecordRequest to the list
func (ir *IoRequest) AddRecordRequest(rr RecordRequest) {
	ir.Records = append(ir.Records, rr)
}

// StringKey returns the request's distribution key as a string
func (ir *IoRequest) StringKey() string {
	return (*IoVector)(ir.DKey()).String()
}

// Length returns the number of record requests
func (ir *IoRequest) Length() int {
	return len(ir.Records)
}

// NeedsPrefetch indicates whether or not we need to determine the size
// of any of the records being fetched before fetching them
func (ir *IoRequest) NeedsPrefetch() bool {
	for _, rr := range ir.Records {
		if rr.Size() == RecAny {
			return true
		}
	}

	return false
}

// DKey returns a *C.daos_key_t representing the distribution key for
// the request
func (ir *IoRequest) DKey() *C.daos_key_t {
	if ir.dkey == nil {
		ir.dkey = NewIoVector(ir.DistKey)

	}
	return (*C.daos_key_t)(ir.dkey)
}

// Free releases all C memory used by the request
func (ir *IoRequest) Free() {
	for _, rr := range ir.Records {
		rr.Free()
	}
	if ir.dkey != nil {
		ir.dkey.Free()
	}
}

// IODs returns a pointer to a slice of IODs
func (ir *IoRequest) IODs() *C.daos_iod_t {
	if ir.Length() == 0 {
		return nil
	}

	iods := make([]C.daos_iod_t, ir.Length())
	for i, rr := range ir.Records {
		rr.PrepareIOD(&iods[i])
	}

	return (*C.daos_iod_t)(&iods[0])
}

// SGLs returns a pointer to a slice of SGLs
func (ir *IoRequest) SGLs() *C.daos_sg_list_t {
	if ir.Length() == 0 {
		return nil
	}

	sgls := make([]C.daos_sg_list_t, ir.Length())
	for i, rr := range ir.Records {
		rr.PrepareSGL(&sgls[i])
	}

	return (*C.daos_sg_list_t)(&sgls[0])
}

// Complete performs any necessary data copying between C/Go memory
// and other housekeeping after a Fetch or Inspect
func (ir *IoRequest) Complete() error {
	nonZero := ir.Records[:0]
	for _, rr := range ir.Records {
		if err := rr.Complete(); err != nil {
			return err
		}
		// Cut out zero-length records
		if rr.Size() != 0 {
			nonZero = append(nonZero, rr)
		}
	}
	ir.Records = nonZero
	return nil
}

// Update submits the extents and buffers to the object.
func (oh *ObjectHandle) Update(e Epoch, ir *IoRequest) error {
	rc, err := C.daos_obj_update(oh.H(), e.Native(), ir.DKey(),
		C.uint(ir.Length()), ir.IODs(), ir.SGLs(), nil)
	return rc2err("daos_obj_update", rc, err)
}

// Inspect attempts to fetch the record size for each akey provided in the
// KeyRequest Buffers.  No data will be fetched, but the updated
func (oh *ObjectHandle) Inspect(e Epoch, ir *IoRequest) error {
	rc, err := C.daos_obj_fetch(oh.H(), e.Native(), ir.DKey(), C.uint(ir.Length()), ir.IODs(), nil, nil, nil)
	if err := rc2err("daos_obj_fetch", rc, err); err != nil {
		return err
	}

	debug.Print("Completing")
	return ir.Complete()
}

// Fetch reads the specified extents and returns them in newly allocated
// KeyRequest Buffers. If any extents have a RecSize set to RecAny, then
// a second fetch will done iff all the extents have a valid record size after
// first fetch.
func (oh *ObjectHandle) Fetch(e Epoch, ir *IoRequest) error {
	if ir.NeedsPrefetch() {
		if err := oh.Inspect(e, ir); err != nil {
			return err
		}
	}

	if ir.NeedsPrefetch() {
		return errors.New("one or more records missing a size after prefetch")
	}

	rc, err := C.daos_obj_fetch(oh.H(), e.Native(), ir.DKey(), C.uint(ir.Length()), ir.IODs(), ir.SGLs(), nil, nil)
	if err := rc2err("daos_obj_fetch", rc, err); err != nil {
		return err
	}

	return ir.Complete()
}
