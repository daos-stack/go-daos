package daosfs

import (
	"encoding/binary"
	"sync"

	"github.com/daos-stack/go-daos/pkg/daos"
	"github.com/intel-hpdd/logging/debug"
	"github.com/pkg/errors"
)

const (
	// OIDBatchSize is the number of OIDs to claim in a batch
	OIDBatchSize uint64 = 4096 // TODO: Figure out what makes sense here.
)

type oidGenerator struct {
	sync.Mutex

	fs *DaosFileSystem

	last    uint64
	current uint64
}

func newOidGenerator(fs *DaosFileSystem) *oidGenerator {
	return &oidGenerator{
		fs:      fs,
		current: 1,
	}
}

func (g *oidGenerator) getNextBatch() error {
	/* FIXME: This stuff is still racy for multiple client mounts.
	 * Need to understand epochs better and see if we can figure out
	 * a way to guarantee that each batch is only used by a single
	 * DaosFileSystem process.
	 */
	debug.Print("getting next batch of OIDs")
	epoch, err := g.fs.ch.EpochHold(0)
	if err != nil {
		return errors.Wrap(err, "Unable to hold epoch")
	}

	tx := g.fs.ch.EpochDiscard
	defer func() {
		tx(epoch)
	}()

	g.last, err = g.Load(epoch)
	if err != nil {
		// FIXME: Distinguish between ENOENT and other errors
		debug.Printf("warning, got error from Load(); assuming init")
	}
	g.last += OIDBatchSize
	// Declare that we are using this batch. Other users will get
	// batches > this one.
	if err := g.Save(epoch, g.last); err != nil {
		return err
	}

	// Move the current index into the new batch, unless this is the
	// very first batch.
	if g.last != OIDBatchSize {
		g.current = g.last - OIDBatchSize + 1
		debug.Printf("set current to %d", g.current)
	}

	tx = g.fs.ch.EpochCommit

	return nil
}

func (g *oidGenerator) Load(e daos.Epoch) (uint64, error) {
	oh, err := g.fs.ch.ObjectOpen(MetaOID, e, daos.ObjOpenRW)
	if err != nil {
		return 0, errors.Wrap(err, "Unable to open meta object")
	}
	defer oh.Close()

	val, err := oh.Getb(e, []byte("LastOID"), []byte("LastOID"))
	if err != nil {
		return 0, err
	}
	last := binary.LittleEndian.Uint64(val)
	debug.Printf("loaded %d as last", last)

	return last, nil
}

func (g *oidGenerator) Save(e daos.Epoch, newLast uint64) error {
	oh, err := g.fs.ch.ObjectOpen(MetaOID, e, daos.ObjOpenRW)
	if err != nil {
		return errors.Wrap(err, "Unable to open meta object")
	}
	defer oh.Close()

	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf, newLast)
	if err := oh.Putb(e, []byte("LastOID"), []byte("LastOID"), buf); err != nil {
		return errors.Wrap(err, "Unable to put LastOID")
	}
	debug.Printf("saved %d as LastOID", newLast)

	return nil
}

// TODO: We should probably be maintaining separate generators for each
// class.
// TODO: Should there be a way to roll this back on failure so we don't
// leak OIDs?
func (g *oidGenerator) Next(class daos.OClassID) (*daos.ObjectID, error) {
	g.Lock()
	defer g.Unlock()
	g.current++
	// TODO: As an optimization, kick off a goroutine to get a new batch
	// if we've hit a low-water mark to avoid blocking?
	if g.current > g.last {
		if err := g.getNextBatch(); err != nil {
			return nil, err
		}
	}

	return daos.ObjectIDInit(0, 0, g.current, class), nil
}
