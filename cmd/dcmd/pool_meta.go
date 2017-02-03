package main

import (
	"encoding/json"
	"log"
	"os/user"
	"time"

	"github.com/daos-stack/go-daos/pkg/daos"
	"github.com/pkg/errors"
)

const (
	PoolMetaDkey  = "metadata"
	CreatorAkey   = "creator"
	CreatedAkey   = "created"
	ContTableAkey = "cont_table"
)

// PoolMetaInit creates a container for pool metadata and creates two
// objects. One for pool level metadata (creator,etc) and the other
// to store container information.
func PoolMetaInit(poh *daos.PoolHandle, uuid string) error {
	err := poh.NewContainer(uuid)
	if err != nil {
		return errors.Wrap(err, "unable to create pool metadata container")
	}

	coh, err := poh.Open(uuid, daos.ContOpenRW)
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

	commit = coh.EpochCommit
	return nil
}

type PoolMeta struct {
	poh    *daos.PoolHandle
	coh    *daos.ContHandle
	oid    *daos.ObjectID
	oh     *daos.ObjectHandle
	update bool
	epoch  daos.Epoch
}

func OpenMeta(poh *daos.PoolHandle, uuid string, update bool) (*PoolMeta, error) {
	var pm PoolMeta
	flag := daos.ContOpenRO
	if update {
		flag = daos.ContOpenRW
	}
	coh, err := poh.Open(uuid, flag)
	if err != nil {
		return nil, errors.Wrap(err, "open container failed")
	}

	pm.poh = poh // for convenience, don't close this
	pm.coh = coh
	pm.update = update
	if update {
		e, err := coh.EpochHold(0)
		if err != nil {
			coh.Close()
			return nil, errors.Wrap(err, "epoch hold")
		}
		pm.epoch = e
	} else {
		pm.epoch = daos.EpochMax
	}

	pm.oid = daos.ObjectIDInit(0, 0, 1, daos.ClassLargeRW)
	return &pm, nil

}

func (pm *PoolMeta) Close() error {
	if pm.oh != nil {
		pm.oh.Close()
		pm.oh = nil
	}
	if pm.update {
		pm.coh.EpochCommit(pm.epoch)
	}
	return pm.coh.Close()
}

func (pm *PoolMeta) openMeta() (*daos.ObjectHandle, error) {
	if pm.oh != nil {
		return pm.oh, nil
	}
	oh, err := pm.coh.ObjectOpen(pm.oid, pm.epoch, daos.ObjOpenRO)
	if err != nil {
		return nil, err
	}
	pm.oh = oh
	return oh, nil
}

func (pm *PoolMeta) Get(akey string) ([]byte, error) {
	oh, err := pm.openMeta()
	if err != nil {
		return nil, errors.Wrap(err, "open meta")
	}

	return oh.Get(pm.epoch, PoolMetaDkey, akey)
}

func (pm *PoolMeta) Creator() (string, error) {
	creator, err := pm.Get(CreatorAkey)
	if err != nil {
		return "", errors.Wrap(err, "get creator failed")
	}
	return string(creator), nil
}

func (pm *PoolMeta) Created() (time.Time, error) {
	buf, err := pm.Get(CreatedAkey)
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

func (pm *PoolMeta) ContTable() (*daos.ObjectID, error) {
	buf, err := pm.Get(ContTableAkey)
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

func (pm *PoolMeta) OpenContTable() (*daos.ObjectHandle, error) {
	oid, err := pm.ContTable()
	if err != nil {
		return nil, errors.Wrap(err, "fetch cont_table id")
	}
	flags := daos.ObjOpenRO
	if pm.update {
		flags = daos.ObjOpenRW
	}
	return pm.coh.ObjectOpen(oid, pm.epoch, flags)
}

func (pm *PoolMeta) AddContainer(name string, uuid string) error {
	oh, err := pm.OpenContTable()
	if err != nil {
		return err
	}
	defer oh.Close()
	return oh.Put(pm.epoch, name, "uuid", []byte(uuid))
}

func (pm *PoolMeta) LookupContainer(name string) (string, error) {
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
func (pm *PoolMeta) OpenContainer(cont string, flags int) (*daos.ContHandle, error) {
	id, err := pm.LookupContainer(cont)
	if err != nil {
		log.Printf("%s: lookup failed,  a uuid", cont)
		id = cont
	}

	return pm.poh.Open(id, flags)
}
