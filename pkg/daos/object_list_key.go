package daos

// #include <stdlib.h>
// #include <daos.h>
// #include <daos/common.h>
import "C"
import "github.com/pkg/errors"

type (
	// KeyDescriptor wraps a C.daos_key_desc_t
	KeyDescriptor C.daos_key_desc_t
	// KeyDescriptorSlice is a slice of KeyDescriptor
	KeyDescriptorSlice []KeyDescriptor
	// Anchor is used for pagination
	Anchor C.daos_hash_out_t
)

// EOF indicates whether or not the final record has been retrieved
func (a Anchor) EOF() bool {
	rc, _ := C.daos_hash_is_eof(a.Pointer())
	return bool(rc)
}

// Pointer returns a *C.daos_hash_out_t
func (a *Anchor) Pointer() *C.daos_hash_out_t {
	return (*C.daos_hash_out_t)(a)
}

// DistKeys returns a slice of dkeys as []bytes
func (oh ObjectHandle) DistKeys(e Epoch, anchor *Anchor) ([][]byte, error) {
	if anchor == nil {
		return nil, errors.New("anchor must not be null")
	}

	kd := make(KeyDescriptorSlice, 32)
	nr := C.uint32_t(len(kd))
	data := make([]byte, 512)
	sgl := NewSGList([][]byte{data})
	defer sgl.Free()

	rc, err := C.daos_obj_list_dkey(oh.H(), e.Native(), &nr, kd.Pointer(), sgl.Pointer(), anchor.Pointer(), nil)
	if err := rc2err("daos_obj_list_dkey", rc, err); err != nil {
		return nil, err
	}
	if err := sgl.Complete(); err != nil {
		return nil, err
	}

	keys := make([][]byte, 0, int(nr))

	var offset int
	for i := 0; i < int(nr); i++ {
		d := kd[i]
		keys = append(keys, data[offset:offset+int(d.kd_key_len)])
		offset += int(d.kd_key_len) + int(d.kd_csum_len)
	}

	return keys, nil
}

// AttrKeys returns a slice of akeys as []bytes
func (oh ObjectHandle) AttrKeys(e Epoch, dkey []byte, anchor *Anchor) ([][]byte, error) {
	if anchor == nil {
		return nil, errors.New("anchor must not be null")
	}
	ir := NewIoRequest(dkey)
	defer ir.Free()

	kd := make(KeyDescriptorSlice, 32)
	nr := C.uint32_t(len(kd))
	data := make([]byte, 512)
	sgl := NewSGList([][]byte{data})
	defer sgl.Free()

	rc, err := C.daos_obj_list_akey(oh.H(), e.Native(), ir.DKey(), &nr, kd.Pointer(), sgl.Pointer(), anchor.Pointer(), nil)
	if err := rc2err("daos_obj_list_akey", rc, err); err != nil {
		return nil, err
	}
	if err := sgl.Complete(); err != nil {
		return nil, err
	}

	keys := make([][]byte, 0, int(nr))

	var offset int
	for i := 0; i < int(nr); i++ {
		d := kd[i]
		keys = append(keys, data[offset:offset+int(d.kd_key_len)])
		offset += int(d.kd_key_len) + int(d.kd_csum_len)
	}

	return keys, nil
}

// Pointer returns a *C.daos_key_desc_t
func (kdl KeyDescriptorSlice) Pointer() *C.daos_key_desc_t {
	return (*C.daos_key_desc_t)(&kdl[0])
}
