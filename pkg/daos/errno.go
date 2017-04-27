package daos

// #include <daos.h>
import "C"
import "fmt"

type (
	Error int
)

const (
	DER_NO_PERM         = Error(C.DER_NO_PERM)
	DER_NO_HDL          = Error(C.DER_NO_HDL)
	DER_INVAL           = Error(C.DER_INVAL)
	DER_EXIST           = Error(C.DER_EXIST)
	DER_NONEXIST        = Error(C.DER_NONEXIST)
	DER_UNREACH         = Error(C.DER_UNREACH)
	DER_NOSPACE         = Error(C.DER_NOSPACE)
	DER_ALREADY         = Error(C.DER_ALREADY)
	DER_NOMEM           = Error(C.DER_NOMEM)
	DER_NOSYS           = Error(C.DER_NOSYS)
	DER_TIMEDOUT        = Error(C.DER_TIMEDOUT)
	DER_BUSY            = Error(C.DER_BUSY)
	DER_AGAIN           = Error(C.DER_AGAIN)
	DER_PROTO           = Error(C.DER_PROTO)
	DER_UNINIT          = Error(C.DER_UNINIT)
	DER_TRUNC           = Error(C.DER_TRUNC)
	DER_OVERFLOW        = Error(C.DER_OVERFLOW)
	DER_CANCELED        = Error(C.DER_CANCELED)
	DER_OOG             = Error(C.DER_OOG)
	DER_CRT_HG          = Error(C.DER_CRT_HG)
	DER_CRT_UNREG       = Error(C.DER_CRT_UNREG)
	DER_CRT_ADDRSTR_GEN = Error(C.DER_CRT_ADDRSTR_GEN)
	DER_CRT_PMIX        = Error(C.DER_CRT_PMIX)
	DER_IVCB_FORWARD    = Error(C.DER_IVCB_FORWARD)
	DER_MISC            = Error(C.DER_MISC)
	DER_BADPATH         = Error(C.DER_BADPATH)
	DER_NOTDIR          = Error(C.DER_NOTDIR)
	DER_UNKNOWN         = Error(C.DER_UNKNOWN)
	DER_IO              = Error(C.DER_IO)
	DER_FREE_MEM        = Error(C.DER_FREE_MEM)
	DER_ENOENT          = Error(C.DER_ENOENT)
	DER_NOTYPE          = Error(C.DER_NOTYPE)
	DER_NOSCHEMA        = Error(C.DER_NOSCHEMA)
	DER_NOLOCAL         = Error(C.DER_NOLOCAL)
	DER_STALE           = Error(C.DER_STALE)
	DER_TGT_CREATE      = Error(C.DER_TGT_CREATE)
	DER_EP_RO           = Error(C.DER_EP_RO)
	DER_EP_OLD          = Error(C.DER_EP_OLD)
	DER_KEY2BIG         = Error(C.DER_KEY2BIG)
	DER_REC2BIG         = Error(C.DER_REC2BIG)
	DER_IO_INVAL        = Error(C.DER_IO_INVAL)
	DER_EQ_BUSY         = Error(C.DER_EQ_BUSY)
	DER_DOMAIN          = Error(C.DER_DOMAIN)
)

var (
	Errors map[Error]string = map[Error]string{
		DER_NO_PERM:         "No permission",
		DER_NO_HDL:          "Invalid handle",
		DER_INVAL:           "Invalid parameters",
		DER_EXIST:           "Entity already exists",
		DER_NONEXIST:        "Nonexistent entity",
		DER_UNREACH:         "Unreachable node",
		DER_NOSPACE:         "No space on storage target",
		DER_ALREADY:         "Already did sth",
		DER_NOMEM:           "NO memory",
		DER_NOSYS:           "Function not implemented",
		DER_TIMEDOUT:        "Timed out",
		DER_BUSY:            "Busy",
		DER_AGAIN:           "Try again",
		DER_PROTO:           "incompatible protocol",
		DER_UNINIT:          "Un-initialized",
		DER_TRUNC:           "Buffer too short, larger buffer needed",
		DER_OVERFLOW:        "Value too large for defined data type",
		DER_CANCELED:        "Operation cancelled",
		DER_OOG:             "Out-Of-Group or member list",
		DER_CRT_HG:          "Transport layer mercury error",
		DER_CRT_UNREG:       "CRT RPC",
		DER_CRT_ADDRSTR_GEN: "CRT failed to generate an address string",
		DER_CRT_PMIX:        "CRT PMIx layer error",
		DER_IVCB_FORWARD:    "CRT IV callback - cannot handle locally",
		DER_MISC:            "CRT miscellaneous error",
		DER_BADPATH:         "Bad path name",
		DER_NOTDIR:          "Not a directory",
		DER_UNKNOWN:         "Unknown error",
		DER_IO:              "Generic I/O error",
		DER_FREE_MEM:        "Memory free error",
		DER_ENOENT:          "Entry not found",
		DER_NOTYPE:          "Unknown object type",
		DER_NOSCHEMA:        "Unknown object schema",
		DER_NOLOCAL:         "Object is not local",
		DER_STALE:           "stale pool map version",
		DER_TGT_CREATE:      "Target create error",
		DER_EP_RO:           "Epoch is read-only",
		DER_EP_OLD:          "Epoch is too old, all data have been recycled",
		DER_KEY2BIG:         "Key is too large",
		DER_REC2BIG:         "Record is too large",
		DER_IO_INVAL:        "IO buffers can't match object extents",
		DER_EQ_BUSY:         "Event queue is busy",
		DER_DOMAIN:          "Domain of cluster component can't match",
	}
)

func (e Error) String() string {
	if m, ok := Errors[e]; ok {
		return fmt.Sprintf("%s (%d)", m, int(e))
	}
	return fmt.Sprintf("Unknown DAOS error (%d)", e)
}
