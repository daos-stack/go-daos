package daos

// #include <stdlib.h>
// #include <daos.h>
// #include <daos/common.h>
import "C"
import (
	"log"
	"unsafe"
)

type (
	KeyDescriptor      C.daos_key_desc_t
	KeyDescriptorSlice []KeyDescriptor
	Anchor             C.daos_hash_out_t
)

func (oh ObjectHandle) DistKeys(e Epoch, anchor *Anchor) ([][]byte, *Anchor, error) {
	if anchor == nil {
		anchor = &Anchor{}
	}
	kd := make(KeyDescriptorSlice, 32)
	nr := C.uint32_t(len(kd))
	sg := SGAlloc(512)
	defer sg.Free()

	rc, err := C.daos_obj_list_dkey(oh.H(), e.Native(), &nr, kd.Pointer(), sg.Pointer(), anchor.Pointer(), nil)
	if err := rc2err("daos_obj_list_dkey", rc, err); err != nil {
		return nil, nil, err
	}

	data := sg.Data()
	buf := make([][]byte, 0, int(nr))

	var offset int
	for i := 0; i < int(nr); i++ {
		d := kd[i]
		log.Printf("kd: %#v", d)
		log.Printf("offset: %d  data: %v", offset, data[offset:3])
		buf = append(buf, data[offset:int(d.kd_key_len)])
		offset += int(d.kd_key_len) + int(d.kd_csum_len)
	}

	return buf, anchor, nil
}

func (oh ObjectHandle) AttrKeys(e Epoch, dkey []byte, anchor *Anchor) ([][]byte, *Anchor, error) {
	if anchor == nil {
		anchor = &Anchor{}
	}
	distKey := ByteToDistKey(dkey)
	defer distKey.Free()

	kd := make(KeyDescriptorSlice, 32)
	nr := C.uint32_t(len(kd))
	sg := SGAlloc(512)
	defer sg.Free()

	rc, err := C.daos_obj_list_akey(oh.H(), e.Native(), distKey.Pointer(), &nr, kd.Pointer(), sg.Pointer(), anchor.Pointer(), nil)
	if err := rc2err("daos_obj_list_dkey", rc, err); err != nil {
		return nil, nil, err
	}

	data := sg.Data()
	buf := make([][]byte, 0, int(nr))

	var offset int
	for i := 0; i < int(nr); i++ {
		d := kd[i]
		buf = append(buf, data[offset:int(d.kd_key_len)])
		offset += int(d.kd_key_len) + int(d.kd_csum_len)
	}

	return buf, anchor, nil
}

func (kdl KeyDescriptorSlice) Free() {

}

func (kdl KeyDescriptorSlice) Pointer() *C.daos_key_desc_t {
	return (*C.daos_key_desc_t)(&kdl[0])
}

func (a *Anchor) Pointer() *C.daos_hash_out_t {
	return (*C.daos_hash_out_t)(a)
}

func SGAlloc(sz int) *SGList {
	var sg SGList
	iov := (*C.daos_iov_t)(C.calloc(C.size_t(unsafe.Sizeof(C.daos_iov_t{})), 1))
	iov.iov_len = C.daos_size_t(sz)
	iov.iov_buf_len = C.daos_size_t(sz)
	iov.iov_buf = C.malloc(C.size_t(sz))
	C.memset(iov.iov_buf, 0, C.size_t(sz))
	sg.sg_nr.num = 1
	sg.sg_iovs = iov
	return &sg
}

func (sg *SGList) Data() []byte {
	if sg == nil {
		return nil
	}
	return C.GoBytes(sg.sg_iovs.iov_buf, C.int(sg.sg_iovs.iov_len))
}

func (sg *SGList) Free() {
	if sg != nil {
		if sg.sg_iovs != nil {
			C.free(unsafe.Pointer(sg.sg_iovs.iov_buf))
			C.free(unsafe.Pointer(sg.sg_iovs))
			sg.sg_iovs = nil
		}
	}
}

func (sg *SGList) Pointer() *C.daos_sg_list_t {
	return (*C.daos_sg_list_t)(sg)
}
