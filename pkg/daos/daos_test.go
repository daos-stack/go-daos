package daos_test

import (
	"testing"

	"github.com/daos-stack/go-daos/pkg/daos"
)

var objectIDBinaryEncodingTests = []*daos.ObjectID{
	daos.ObjectIDInit(1, 2, 3, daos.ClassTinyRW),
	daos.ObjectIDInit(0, 0, 1, daos.ClassLargeRW),
	daos.ObjectIDInit(0, 0, uint64((1<<64)-1), daos.ClassSmallRW),
	daos.ObjectIDInit(uint32((1<<32)-1), uint64((1<<64)-1), uint64((1<<64)-1), daos.ClassReplMaxRW),
}

func TestObjectIDBinaryEncoding(t *testing.T) {
	for _, tt := range objectIDBinaryEncodingTests {
		decoded := &daos.ObjectID{}
		b, err := tt.MarshalBinary()
		if err != nil {
			t.Errorf("%v Encode error = %q, want nil", tt, err)
		}
		if err = decoded.UnmarshalBinary(b); err != nil {
			t.Errorf("%v Decode error = %q, want nil", b, err)
		}
		if tt.Hi() != decoded.Hi() {
			t.Errorf("%v -> %s Decode error Hi(%d != %d)", b, tt, decoded.Hi(), tt.Hi())
		}
		if tt.Mid() != decoded.Mid() {
			t.Errorf("%v -> %s Decode error Mid(%d != %d)", b, tt, decoded.Mid(), tt.Mid())
		}
		if tt.Lo() != decoded.Lo() {
			t.Errorf("%v -> %s Decode error Lo(%d != %d)", b, tt, decoded.Lo(), tt.Lo())
		}
		if tt.Class() != decoded.Class() {
			t.Errorf("%v -> %s Decode error Class(%d != %d)", b, tt, decoded.Class(), tt.Class())
		}
	}
}

var invalidObjectIDBinaryEncodingTests = []struct {
	bytes []byte
	want  string
}{
	{[]byte{}, "ObjectID.UnmarshalBinary: no data"},
	{[]byte{0, 2, 3}, "ObjectID.UnmarshalBinary: unsupported version"},
	{[]byte{1, 2, 3}, "ObjectID.UnmarshalBinary: invalid length"},
}

func TestInvalidObjectIDBinaryEncoding(t *testing.T) {
	for _, tt := range invalidObjectIDBinaryEncodingTests {
		oid := &daos.ObjectID{}
		err := oid.UnmarshalBinary(tt.bytes)
		if err == nil || err.Error() != tt.want {
			t.Errorf("ObjectID.UnmarshalBinary(%#v) error %v, want %v", tt.bytes, err, tt.want)
		}
	}
}
