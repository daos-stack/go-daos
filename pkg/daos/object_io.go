package daos

// #include <stdlib.h>
// #include <daos.h>
// #include <daos/common.h>
import "C"

import (
	"errors"
	"log"
	"unsafe"
)

type (
	// KeyRequest is a list of extents to fetch or update for a specific Attribute key.
	KeyRequest struct {
		Attr    []byte
		Extents []Extent
		Buffers [][]byte
	}

	// KeyRequests is a slice of *KeyRequest
	KeyRequests []*KeyRequest

	// Extent is a range of records to be fetched or udpated.
	Extent struct {
		Index   uint64
		Count   uint64
		RecSize uint64
	}
)

// NewKeyRequest returns initialized KeyRequest.
func NewKeyRequest(key []byte) *KeyRequest {
	return &KeyRequest{Attr: key}
}

// Put adds new extent and buffer for an udpate request.
func (kr *KeyRequest) Put(index, count, recsize uint64, data []byte) {
	kr.Extents = append(kr.Extents, Extent{index, count, recsize})
	kr.Buffers = append(kr.Buffers, data)
}

// Get adds a new extent used for fetch.
func (kr *KeyRequest) Get(index, count, recsize uint64) {
	kr.Extents = append(kr.Extents, Extent{index, count, recsize})
}

// Update submits the extents and buffers to the object.
func (oh *ObjectHandle) Update(e Epoch, dkey []byte, reqs KeyRequests) error {
	distKey := ByteToDistKey(dkey)
	defer distKey.Free()

	iods := InitIOD(reqs)
	defer iods.Free()

	sgls := InitSGUpdate(reqs)
	defer sgls.Free()
	//log.Printf("KR: %#v", request)
	//log.Printf("iov: %#v\nsg: %#v", iods.Pointer(), sgls.Pointer())
	//log.Printf("rex: %#v", iods[0].vd_recxs)
	//log.Printf("sg: %#v", sgls[0].sg_iovs)
	rc, err := C.daos_obj_update(oh.H(), e.Native(), distKey.Pointer(), C.uint(len(reqs)), iods.Pointer(), sgls.Pointer(), nil)
	return rc2err("daos_obj_update", rc, err)
}

// Inspect attempts to fetch the record size for each akey provided in the
// KeyRequest Buffers.  No data will be fetched, but the updated
func (oh *ObjectHandle) Inspect(e Epoch, dkey []byte, reqs KeyRequests) error {
	distKey := ByteToDistKey(dkey)
	defer distKey.Free()

	iods := InitIOD(reqs)
	defer iods.Free()

	rc, err := C.daos_obj_fetch(oh.H(), e.Native(), distKey.Pointer(), C.uint(len(reqs)), iods.Pointer(), nil, nil, nil)
	if err := rc2err("daos_obj_fetch", rc, err); err != nil {
		return err
	}

	CopyIOD(reqs, iods)
	return nil
}

// Fetch reads the specified extents and returns them in newly allocated
// KeyRequest Buffers. If any extents have a RecSize set to RecAny, then
// a second fetch will done iff all the extents have a valid record size after
// first fetch.
func (oh *ObjectHandle) Fetch(e Epoch, dkey []byte, reqs KeyRequests) error {
	distKey := ByteToDistKey(dkey)
	defer distKey.Free()

	iods := InitIOD(reqs)
	defer iods.Free()

	sgls := InitSGFetch(reqs)
	preFetch := false
	if sgls != nil {
		defer sgls.Free()
	} else {
		preFetch = true
	}

	//log.Printf("KR: %#v", *request[0])
	//log.Printf("iov: %#v\nsg: %#v", iods.Pointer(), sgls.Pointer())
	//log.Printf("rex: %#v", iods[0].vd_recxs)

	rc, err := C.daos_obj_fetch(oh.H(), e.Native(), distKey.Pointer(), C.uint(len(reqs)), iods.Pointer(), sgls.Pointer(), nil, nil)
	if err := rc2err("daos_obj_fetch", rc, err); err != nil {
		return err
	}

	CopyIOD(reqs, iods)

	if preFetch {
		sgls = InitSGFetch(reqs)
		if sgls == nil {
			return errors.New("Unable to complete request. One or more extents missing a RecSize.")
		}
		defer sgls.Free()
		rc, err := C.daos_obj_fetch(oh.H(), e.Native(), distKey.Pointer(), C.uint(len(reqs)), iods.Pointer(), sgls.Pointer(), nil, nil)
		if err := rc2err("daos_obj_update", rc, err); err != nil {
			return err
		}
	}

	//log.Printf("after sg: %#v", sgls.Pointer())
	//log.Printf("after rex: %#v", iods[0].vd_recxs)
	//log.Printf("num: %d num_out %d records", sgls[0].sg_nr.num, sgls[0].sg_nr.num_out)

	CopySG(reqs, sgls)
	return nil
}

type (
	IoVec             C.daos_iov_t
	DistKey           IoVec
	AttrKey           IoVec
	IODescriptor      C.daos_vec_iod_t
	SGList            C.daos_sg_list_t
	SGListSlice       []SGList
	IODescriptorSlice []IODescriptor
)

// allocIOV allocates an array of iovs in C memory
// return value must be released with C.free
func allocIOV(nr int) *C.daos_iov_t {
	return (*C.daos_iov_t)(C.calloc(C.size_t(nr), C.size_t(unsafe.Sizeof(C.daos_iov_t{}))))
}

// allocRecx allocates an array of recx in C memory
// return value must be released with C.free
func allocRecx(nr int) *C.daos_recx_t {
	return (*C.daos_recx_t)(C.calloc(C.size_t(nr), C.size_t(unsafe.Sizeof(C.daos_recx_t{}))))
}

func InitIOD(reqs KeyRequests) IODescriptorSlice {
	iod := make(IODescriptorSlice, len(reqs))

	for i, req := range reqs {
		ak := ByteToAttrKey(req.Attr)
		iod[i].vd_name = ak.Native()
		nr := len(req.Extents)
		iod[i].vd_nr = C.uint(nr)
		recxs := allocRecx(nr)
		reclist := (*[1 << 30]C.daos_recx_t)(unsafe.Pointer(recxs))[:nr:nr]
		for j, ext := range req.Extents {
			reclist[j].rx_rsize = C.uint64_t(ext.RecSize)
			reclist[j].rx_idx = C.uint64_t(ext.Index)
			reclist[j].rx_nr = C.uint64_t(ext.Count)
		}
		iod[i].vd_recxs = recxs
	}
	return iod
}

func CopyIOD(reqs KeyRequests, iod IODescriptorSlice) {
	for i, req := range reqs {
		nr := len(req.Extents)
		reclist := (*[1 << 30]C.daos_recx_t)(unsafe.Pointer(iod[i].vd_recxs))[:nr:nr]
		for j := range req.Extents {
			req.Extents[j].RecSize = uint64(reclist[j].rx_rsize)
		}
	}
}

func (iod IODescriptorSlice) Free() {
	for _, io := range iod {
		(*AttrKey)(&io.vd_name).Free()
		C.free(unsafe.Pointer(io.vd_recxs))
	}
}

func (iod IODescriptorSlice) Pointer() *C.daos_vec_iod_t {
	return (*C.daos_vec_iod_t)(&iod[0])
}

func InitSGUpdate(reqs KeyRequests) SGListSlice {
	sg := make([]SGList, len(reqs))

	for i, req := range reqs {
		nr := len(req.Buffers)
		iovs := allocIOV(nr)
		iolist := (*[1 << 30]C.daos_iov_t)(unsafe.Pointer(iovs))[:nr:nr]
		for j, buf := range req.Buffers {
			copyToIov((*IoVec)(&iolist[j]), buf)
		}
		sg[i].sg_nr.num = C.uint32_t(nr)
		sg[i].sg_iovs = iovs
	}
	return SGListSlice(sg)
}

func InitSGFetch(reqs KeyRequests) SGListSlice {
	sg := make([]SGList, len(reqs))

	// Return immediately if any of the extents is missing a size.
	// Initial fetch will populate record sizes.
	for _, req := range reqs {
		for _, extent := range req.Extents {
			if extent.RecSize == RecAny {
				return nil
			}
		}
	}

	for i, req := range reqs {
		nr := len(req.Extents)
		iovs := allocIOV(nr)
		iolist := (*[1 << 30]C.daos_iov_t)(unsafe.Pointer(iovs))[:nr:nr]
		for j, extent := range req.Extents {
			sz := extent.Count * extent.RecSize
			iolist[j].iov_len = C.daos_size_t(sz)
			iolist[j].iov_buf_len = iolist[j].iov_len
			iolist[j].iov_buf = C.malloc(C.size_t(sz))
		}
		sg[i].sg_nr.num = C.uint32_t(nr)
		sg[i].sg_iovs = iovs
	}
	return SGListSlice(sg)
}

func CopySG(reqs KeyRequests, sg SGListSlice) {
	if sg == nil {
		return
	}
	for i := range reqs {
		nr := int(sg[i].sg_nr.num_out)
		if nr == 0 {
			log.Printf("num: %d num_out %d records", sg[i].sg_nr.num, sg[i].sg_nr.num_out)
			continue
		}
		iolist := (*[1 << 30]C.daos_iov_t)(unsafe.Pointer(sg[i].sg_iovs))[:nr:nr]
		for j := 0; j < nr; j++ {
			reqs[i].Buffers = append(reqs[i].Buffers, C.GoBytes(iolist[j].iov_buf, C.int(iolist[j].iov_len)))
		}
	}
}

func (sgl SGListSlice) Free() {
	if sgl == nil {
		return
	}
	for _, sg := range sgl {
		iolist := (*[1 << 30]C.daos_iov_t)(unsafe.Pointer(sg.sg_iovs))[:int(sg.sg_nr.num):int(sg.sg_nr.num)]
		for i := range iolist {
			if iolist[i].iov_buf != nil {
				C.free(unsafe.Pointer(iolist[i].iov_buf))
				iolist[i].iov_buf = nil
			}
		}
		C.free(unsafe.Pointer(sg.sg_iovs))
	}
}

func (sgl SGListSlice) Pointer() *C.daos_sg_list_t {
	if sgl == nil {
		return nil
	}
	return (*C.daos_sg_list_t)(&sgl[0])
}
