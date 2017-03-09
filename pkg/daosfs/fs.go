package daosfs

import (
	"os"

	"github.com/daos-stack/go-daos/pkg/daos"
	"github.com/daos-stack/go-daos/pkg/ufd"
	"github.com/intel-hpdd/logging/debug"
	"github.com/pkg/errors"
)

var (
	// RootOID is the root of the filesystem
	RootOID = daos.ObjectIDInit(0, 0, 1, daos.ClassTinyRW)
	// MetaOID is the object where misc. filesystem metadata is stashed
	MetaOID = daos.ObjectIDInit(0, 1, 1, daos.ClassTinyRW)
)

// DaosFileSystem provides a filesystem-like interface to DAOS
type DaosFileSystem struct {
	Name string
	root *Node
	uh   *ufd.Handle
	ch   *daos.ContHandle
	og   *oidGenerator
}

// NewDaosFileSystem connects to the given pool and creates a container
// for the given filesystem name
func NewDaosFileSystem(group, pool, container string) (*DaosFileSystem, error) {
	debug.Printf("Connecting to %s (group: %q)", pool, group)
	uh, err := ufd.Connect(group, pool)
	if err != nil {
		return nil, errors.Wrapf(err, "Connection to %q failed", pool)
	}
	debug.Printf("Connected to %s", pool)

	dfs := &DaosFileSystem{
		Name: container,
		uh:   uh,
		root: &Node{
			oid:      RootOID,
			parent:   RootOID,
			modeType: os.ModeDir,
			Name:     "/",
		},
	}
	dfs.root.fs = dfs
	dfs.og = newOidGenerator(dfs)

	if err := dfs.openOrCreateContainer(); err != nil {
		return nil, err
	}

	if err := dfs.getRootObject(); err != nil {
		return nil, err
	}

	return dfs, nil
}

func (dfs *DaosFileSystem) openOrCreateContainer() error {
	debug.Printf("Opening filesystem container %q", dfs.Name)
	ch, err := dfs.uh.OpenContainer(dfs.Name, daos.ContOpenRW)
	if err != nil {
		debug.Printf("Error opening %q: %s; attempting to create it.", dfs.Name, err)
		if err = dfs.uh.NewContainer(dfs.Name, ""); err != nil {
			return errors.Wrapf(err, "Failed to create container %q", dfs.Name)
		}
		if ch, err = dfs.uh.OpenContainer(dfs.Name, daos.ContOpenRW); err != nil {
			return errors.Wrapf(err, "Failed to open container %q after create", dfs.Name)
		}
	}
	dfs.ch = ch

	return nil
}

func (dfs *DaosFileSystem) getRootObject() error {
	if err := dfs.root.openObjectLatest(); err != nil {
		return errors.Wrap(err, "Failed to open root object")
	}
	defer dfs.root.closeObject()

	return nil
}

// Root returns the root node
func (dfs *DaosFileSystem) Root() *Node {
	return dfs.root
}

// Fini shuts everything down
func (dfs *DaosFileSystem) Fini() error {
	dfs.ch.Close()
	dfs.uh.Close()
	return nil
}
