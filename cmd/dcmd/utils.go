package main

import (
	"os"
	"os/user"
	"strconv"
)

func strtou32(s string) (uint32, error) {
	v, err := strconv.ParseUint(s, 0, 32)
	if err != nil {
		return 0, err
	}
	return uint32(v), nil
}

func lookupUser(s string) (uint32, error) {
	if s == "" {
		return uint32(os.Geteuid()), nil
	}

	if val, err := strtou32(s); err == nil {
		return val, nil
	}

	u, err := user.Lookup(s)
	if err != nil {
		return 0, err
	}

	return strtou32(u.Uid)
}

func lookupGroup(s string) (uint32, error) {
	if s == "" {
		return uint32(os.Getegid()), nil
	}

	if val, err := strtou32(s); err == nil {
		return val, nil
	}

	g, err := user.LookupGroup(s)
	if err != nil {
		return 0, err
	}

	return strtou32(g.Gid)

}
