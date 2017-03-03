package ufd

// User Friendly DAOS interface
// Simplify DAOS pool and container management.
//   - Additional Pool metadata (owner and create time)
//   - Create and open Contianers by name

import (
	"encoding/json"
	"os/user"
	"time"

	"github.com/daos-stack/go-daos/pkg/daos"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
)

type (
	// Handle is a pool handle and is the main interface.
	Handle struct {
		poh  *daos.PoolHandle
		uuid string
	}

	// Meta provides additional information about the managed pool.
	Meta interface {
		Close() error
		Creator() (string, error)
		Created() (time.Time, error)
		ContTable() (*daos.ObjectID, error)
	}

	// OpenFlag indicates if open is Readonly or ReadWrite
	OpenFlag int

	metaHandle struct {
		coh    *daos.ContHandle
		oid    *daos.ObjectID
		oh     *daos.ObjectHandle
		update bool
		epoch  daos.Epoch
	}
)

const (
	Readonly  OpenFlag = iota
	ReadWrite          = iota
)

const (
	PoolMetaDkey  = "metadata"
	CreatorAkey   = "creator"
	CreatedAkey   = "created"
	ContTableAkey = "cont_table"
)

// Connect returns a Handle. This is the main entry point.
func Connect(group, pool string) (*Handle, error) {
	if pool == "" {
		return nil, errors.New("no pool uuid provided")
	}

	poh, err := daos.PoolConnect(pool, group, daos.PoolConnectRW)
	if err != nil {
		return nil, errors.Wrap(err, "pool connect")
	}
	return &Handle{poh: poh, uuid: pool}, nil
}

func (h *Handle) Close() error {
	return h.poh.Disconnect()
}

// PoolMetaInit creates a container for pool metadata and creates two
// objects. One for pool level metadata (creator,etc) and the other
// to store container information.
// This should only be called once in the lifetime of a pool.
func (h *Handle) PoolMetaInit() error {
	err := h.poh.NewContainer(h.uuid)
	if err != nil {
		return errors.Wrap(err, "unable to create pool metadata container")
	}

	coh, err := h.poh.Open(h.uuid, daos.ContOpenRW)
	if err != nil {
		return errors.Wrap(err, "open container failed")
	}
	defer coh.Close()

	e, err := coh.EpochHold(0)
	if err != nil {
		return errors.Wrap(err, "epoch hold failed")
	}

	commit := coh.EpochDiscard
	defer func() {
		commit(e)
	}()

	oid := daos.ObjectIDInit(0, 0, 1, daos.ClassLargeRW)

	err = coh.ObjectDeclare(oid, e, nil)
	if err != nil {
		return errors.Wrap(err, "obj declare failed")
	}

	oh, err := coh.ObjectOpen(oid, daos.EpochMax, daos.ObjOpenRW)
	if err != nil {
		return errors.Wrap(err, "open object failed")
	}
	defer oh.Close()
	user, err := user.Current()
	if err != nil {
		return errors.Wrap(err, "lookup current user")
	}

	err = oh.Put(e, PoolMetaDkey, CreatorAkey, []byte(user.Name))
	if err != nil {
		return errors.Wrap(err, "put creator")
	}

	b, err := json.Marshal(time.Now())
	if err != nil {
		return errors.Wrap(err, "marshal time")
	}

	err = oh.Put(e, PoolMetaDkey, CreatedAkey, b)
	if err != nil {
		return errors.Wrap(err, "put created")
	}

	coid := daos.GenerateOID(daos.ClassTinyRW)
	err = coh.ObjectDeclare(coid, e, nil)
	if err != nil {
		return errors.Wrap(err, "declare object")
	}

	b2, err := json.Marshal(coid)
	if err != nil {
		return errors.Wrap(err, "marshal oid")
	}
	err = oh.Put(e, PoolMetaDkey, ContTableAkey, b2)
	if err != nil {
		return errors.Wrap(err, "put cont oid")
	}

	pm, err := h.openMeta(ReadWrite)
	if err != nil {
		return errors.Wrap(err, "openMeta")
	}
	defer pm.Close()
	err = pm.AddContainer("pool_meta", h.uuid)

	commit = coh.EpochCommit
	return nil
}

// Info returns the underlying pool's native Info struct.
func (h *Handle) Info() (*daos.PoolInfo, error) {
	return h.poh.Info()
}

// Meta returns object containing additional data for the managed pool.
func (h *Handle) Meta() (Meta, error) {
	return h.openMeta(Readonly)
}

// NewContainer creates a new conatiner with given name.
// The ID parameter is optional.
func (h *Handle) NewContainer(name string, id string) error {
	if name == "" {
		return errors.New("name not specified")
	}

	if id == "" {
		id = uuid.New()
	}
	m, err := h.openMeta(ReadWrite)
	if err != nil {
		return errors.Wrap(err, "openMeta")
	}
	defer m.Close()

	err = h.poh.NewContainer(id)
	if err != nil {
		return errors.Wrap(err, "new container")
	}

	err = m.AddContainer(name, id)
	return err
}

func (h *Handle) List() ([][]byte, error) {
	pm, err := h.openMeta(Readonly)
	if err != nil {
		return nil, errors.Wrap(err, "openMeta")
	}
	defer pm.Close()
	oh, err := pm.OpenContTable()
	if err != nil {
		return nil, err
	}
	defer oh.Close()
	var dkeys [][]byte
	var anchor daos.Anchor
	for !anchor.EOF() {
		result, err := oh.DistKeys(daos.EpochMax, &anchor)
		if err != nil {
			return nil, err
		}
		dkeys = append(dkeys, result...)
	}
	return dkeys, nil

}

// LookupContainer returns the uuid of the named container.
func (h *Handle) LookupContainer(name string) (string, error) {
	pm, err := h.openMeta(Readonly)
	if err != nil {
		return "", errors.Wrap(err, "openMeta")
	}
	defer pm.Close()
	oh, err := pm.OpenContTable()
	if err != nil {
		return "", err
	}
	defer oh.Close()
	buf, err := oh.Get(pm.epoch, name, "uuid")
	if err != nil {
		return "", errors.Wrap(err, "fetch uuid")
	}

	return string(buf), nil
}

// OpenContainer opens the named container and returns native
// daos handle for the container.
func (pm *Handle) OpenContainer(cont string, flags int) (*daos.ContHandle, error) {
	id, err := pm.LookupContainer(cont)
	if err != nil {
		//log.Printf("%s: lookup failed, assuming uuid", cont)
		id = cont
	}

	return pm.poh.Open(id, flags)
}

func (h *Handle) openMeta(of OpenFlag) (*metaHandle, error) {
	flag := daos.ContOpenRO

	if of == ReadWrite {
		flag = daos.ContOpenRW
	}
	coh, err := h.poh.Open(h.uuid, flag)
	if err != nil {
		return nil, errors.Wrap(err, "open container failed")
	}

	m := &metaHandle{}
	m.coh = coh
	m.update = of == ReadWrite
	if m.update {
		e, err := coh.EpochHold(0)
		if err != nil {
			coh.Close()
			return nil, errors.Wrap(err, "epoch hold")
		}
		m.epoch = e
	} else {
		es, err := coh.EpochQuery()
		if err != nil {
			return nil, errors.Wrap(err, "epoch query")
		}
		m.epoch = es.GHCE()
	}

	m.oid = daos.ObjectIDInit(0, 0, 1, daos.ClassLargeRW)
	return m, nil

}

func (m *metaHandle) Close() error {
	if m.oh != nil {
		m.oh.Close()
		m.oh = nil
	}

	if m.update {
		m.coh.EpochCommit(m.epoch)
	}

	return m.coh.Close()
}

// AddContainer adds conatainer name and id to the container map.
func (m *metaHandle) AddContainer(name string, id string) error {
	oh, err := m.OpenContTable()
	if err != nil {
		return err
	}
	defer oh.Close()
	return oh.Put(m.epoch, name, "uuid", []byte(id))
}

// Opens the meta object.
func (m *metaHandle) open() (*daos.ObjectHandle, error) {
	if m.oh != nil {
		return m.oh, nil
	}
	oh, err := m.coh.ObjectOpen(m.oid, m.epoch, daos.ObjOpenRO)
	if err != nil {
		return nil, err
	}
	m.oh = oh
	return oh, nil
}

// Get a metadata attribute.
func (m *metaHandle) Get(akey string) ([]byte, error) {
	oh, err := m.open()
	if err != nil {
		return nil, errors.Wrap(err, "open meta")
	}

	return oh.Get(m.epoch, PoolMetaDkey, akey)
}

// Creator returns the username of user that created the pool.
func (m *metaHandle) Creator() (string, error) {
	creator, err := m.Get(CreatorAkey)
	if err != nil {
		return "", errors.Wrap(err, "get creator failed")
	}
	return string(creator), nil
}

// Created returns tiem the pool was created.
func (m *metaHandle) Created() (time.Time, error) {
	buf, err := m.Get(CreatedAkey)
	if err != nil {
		return time.Time{}, errors.Wrap(err, "get created failed")
	}
	var created time.Time
	err = json.Unmarshal(buf, &created)
	if err != nil {
		return time.Time{}, errors.Wrapf(err, "create time: %s", buf)
	}
	return created, nil
}

// ContTable returns DAOS ObjectID for the container map object.
func (m *metaHandle) ContTable() (*daos.ObjectID, error) {
	buf, err := m.Get(ContTableAkey)
	if err != nil {
		return nil, errors.Wrap(err, "get cont table failed")
	}
	var coid daos.ObjectID
	err = json.Unmarshal(buf, &coid)
	if err != nil {
		return nil, errors.Wrapf(err, "unmarshal coid: '%s'", buf)
	}
	return &coid, nil
}

// OpenContTable retuns open handle for the container map.
func (m *metaHandle) OpenContTable() (*daos.ObjectHandle, error) {
	oid, err := m.ContTable()
	if err != nil {
		return nil, errors.Wrap(err, "fetch cont_table id")
	}
	flags := daos.ObjOpenRO
	if m.update {
		flags = daos.ObjOpenRW
	}
	return m.coh.ObjectOpen(oid, m.epoch, flags)
}
