package daosfs

type Error int

const (
	ErrNoXattr Error = 1000 + iota
)

// ErrStrings are erorr messages for Daosfs errors
var ErrStrings map[Error]string = map[Error]string{
	ErrNoXattr: "No attribute",
}

func (e Error) Error() string {
	if m, ok := ErrStrings[e]; ok {
		return m
	}

	return "Unknown Error"
}
