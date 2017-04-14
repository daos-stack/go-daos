package daos

//
// #cgo LDFLAGS:  -ldaos  -lcrt_util -lcrt -ldaos_common -ldaos_tier -ldaos_array -luuid
// #include <stdlib.h>
// #include <daos.h>
// #include <daos/common.h>
// #include <daos_array.h>
//
import "C"
import (
	"unsafe"

	"github.com/pkg/errors"
)

type (
	// BufferedRange describes a byte range within a DAOS Array
	BufferedRange struct {
		Buffer []byte
		Length int64
		Offset int64
	}

	// ArrayRanges describes a list of RangeBuffers within a DAOS array
	ArrayRanges []*BufferedRange

	// ArrayRequest describes a data structure used for writing to
	// or reading from a DAOS Array
	ArrayRequest struct {
		Ranges ArrayRanges
	}

	// Array provides an idiomatic interface to DAOS Arrays
	Array struct {
		oh *ObjectHandle
	}
)

// AddRange adds a BufferedRange to the ArrayRequest and allocates a buffer
// for it if one was not provided
func (ar *ArrayRequest) AddRange(br *BufferedRange) {
	if cap(br.Buffer) == 0 {
		br.Buffer = make([]byte, br.Length)
	}
	ar.Ranges = append(ar.Ranges, br)
}

// Buffers implements the RequestBufferSlicer interface. In this case,
// we'll only ever need a single SGL buffer per request.
func (ar *ArrayRequest) Buffers() []RequestBuffers {
	bufs := make(RequestBuffers, 0, len(ar.Ranges))
	for _, r := range ar.Ranges {
		bufs = append(bufs, r.Buffer)
	}
	return []RequestBuffers{bufs}
}

// Length implements the RequestBufferSlicer interface. In this case,
// we only ever want to create a single SGL per request.
func (ar *ArrayRequest) Length() int {
	// This is the number of SGLs we want to create, which will always
	// be 1 for a DAOS Array.
	return 1
}

// Size returns the total size of all ranges in the request.
func (ar *ArrayRequest) Size() (total int64) {
	for _, br := range ar.Ranges {
		total += br.Length
	}

	return
}

// Native constructs a C.daos_array_ranges_t from the request's Range list
// and returns a pointer to it along with a callback to free the C memory
// that was allocated for it.
func (ar *ArrayRequest) Native() (*C.daos_array_ranges_t, func(), error) {
	nr := len(ar.Ranges)
	dar := C.daos_array_ranges_t{
		ranges_nr: C.daos_size_t(nr),
		ranges:    (*C.daos_range_t)(C.calloc(C.size_t(nr), C.size_t(unsafe.Sizeof(C.daos_range_t{})))),
	}
	if dar.ranges == nil {
		return nil, nil, errors.New("calloc() failed")
	}

	// Convert the C array into a slice so we can index into it
	ranges := (*[1 << 30]C.daos_range_t)(unsafe.Pointer(dar.ranges))[:nr:nr]
	for i, r := range ar.Ranges {
		ranges[i].len = C.daos_size_t(r.Length)
		ranges[i].index = C.daos_off_t(r.Offset)
	}

	return &dar, func() { C.free(unsafe.Pointer(dar.ranges)) }, nil
}

// NewArrayRequest accepts an optional list of *BufferedRange items to
// add to a new *ArrayRequest
func NewArrayRequest(rl ...*BufferedRange) *ArrayRequest {
	ar := &ArrayRequest{}
	for _, br := range rl {
		ar.AddRange(br)
	}

	return ar
}

// ArrayOpen opens an Array Object
func (coh *ContHandle) ArrayOpen(oid *ObjectID, e Epoch, mode ObjectOpenFlag) (*Array, error) {
	oh, err := coh.ObjectOpen(oid, e, mode)
	if err != nil {
		return nil, err
	}

	return NewArray(oh), nil
}

// NewArray wraps an *ObjectHandle with DAOS Array methods
func NewArray(oh *ObjectHandle) *Array {
	return &Array{oh: oh}
}

// Close closes the Array's object handle
func (a *Array) Close() error {
	return a.oh.Close()
}

func (a *Array) Read(e Epoch, req *ArrayRequest) (int64, error) {
	var total int64

	dar, freeReq, err := req.Native()
	if err != nil {
		return 0, err
	}
	defer freeReq()

	// Create 1 SGL for each range in the request. Unfortunately,
	// we can't have C code writing directly into Go-allocated buffers,
	// so we have to malloc() some in C-land and then copy back into Go.
	sgls := SGListSlice(make([]SGList, len(req.Ranges)))
	for i, br := range req.Ranges {
		iovs := allocIOV(1)
		iolist := (*[1 << 30]C.daos_iov_t)(unsafe.Pointer(iovs))[:1:1]
		iolist[0].iov_len = C.daos_size_t(br.Length)
		iolist[0].iov_buf_len = iolist[0].iov_len
		iolist[0].iov_buf = C.malloc(C.size_t(iolist[0].iov_len))
		sgls[i].sg_nr.num = C.uint32_t(1)
		sgls[i].sg_iovs = iovs
	}
	defer sgls.Free()

	rc, err := C.daos_array_read(a.oh.H(), e.Native(), dar, sgls.Pointer(), nil, nil)
	if err := rc2err("daos_array_read", rc, err); err != nil {
		return 0, err
	}

	// Now that we've got data in the SGL buffers, we need to copy it
	// back into Go.
	for i, bufs := range req.Buffers() {
		if int(sgls[i].sg_nr.num_out) == 0 {
			continue
		}
		iolist := (*[1 << 30]C.daos_iov_t)(unsafe.Pointer(sgls[i].sg_iovs))[:len(bufs):len(bufs)]
		for j := 0; j < len(bufs); j++ {
			iovlen := C.int(iolist[j].iov_len)
			if int(iovlen) > cap(bufs[j]) {
				return 0, errors.Errorf("Can't copy IOV with length %d into buffer with cap %d", iovlen, cap(bufs[j]))
			}
			n := copy(bufs[j][:iovlen], C.GoBytes(iolist[j].iov_buf, iovlen))
			bufs[j] = bufs[j][:n]
			total += int64(n)
		}
	}

	return total, nil
}

func (a *Array) Write(e Epoch, req *ArrayRequest) (int64, error) {
	dar, free, err := req.Native()
	if err != nil {
		return 0, err
	}
	defer free()

	sgls := InitSGUpdate(req)
	defer sgls.Free()

	rc, err := C.daos_array_write(a.oh.H(), e.Native(), dar, sgls.Pointer(), nil, nil)
	return req.Size(), rc2err("daos_array_write", rc, err)
}

// GetSize returns the size of the Array
func (a *Array) GetSize(e Epoch) (int64, error) {
	var size C.daos_size_t
	rc, err := C.daos_array_get_size(a.oh.H(), e.Native(), &size, nil)

	// In Go, everything file-related uses int64. Sizes come out
	// of DAOS as uint64, so we need to convert, but check to make
	// sure we're not losing any information.
	if int64(size) < 0 {
		return 0, errors.Errorf("%d overflows int64", size)
	}
	return int64(size), rc2err("daos_array_get_size", rc, err)
}

// SetSize preallocates an Array up to the given size
func (a *Array) SetSize(e Epoch, size int64) error {
	rc, err := C.daos_array_set_size(a.oh.H(), e.Native(), C.daos_size_t(size), nil)
	return rc2err("daos_array_set_size", rc, err)
}
