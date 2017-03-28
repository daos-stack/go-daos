package daosfs

import (
	"sort"
	"sync"

	"github.com/daos-stack/go-daos/pkg/daos"
	"github.com/intel-hpdd/logging/debug"
	"github.com/pkg/errors"
)

type (
	writeFn func(e daos.Epoch) error

	// This is an attempt to simplify other parts of the code by
	// consolidating epoch management into one place. It should be
	// safe to use with multiple goroutines in a single process,
	// but will probably not achieve expected/desired results with
	// multiple processes.
	epochManager struct {
		sync.RWMutex

		// An open DAOS container handle for the filesystem
		// contianer.
		ch *daos.ContHandle

		// Latest consistent-reads epoch we know about
		readEpoch daos.Epoch
		// Highest uncommitted epoch we know about
		writeEpoch daos.Epoch
		// Our held epoch (epochs >= are dirty)
		heldEpoch daos.Epoch

		// A map of "dirty" epochs known to this instance
		holds map[daos.Epoch]int
	}
)

func (em *epochManager) withCommit(fn writeFn) (err error) {
	var epoch daos.Epoch
	if epoch, err = em.GetWriteEpoch(); err != nil {
		return err
	}
	tx := em.Discard
	defer func() {
		if txe := tx(epoch); txe != nil {
			err = errors.Wrapf(txe, "Failure in deferred %s, previous err was\n%s", tx, err)
		}
	}()

	if err = fn(epoch); err != nil {
		err = errors.Wrap(err, "writeFn failed in withCommit")
		return
	}

	tx = em.Commit
	return
}

func (em *epochManager) Discard(e daos.Epoch) error {
	em.Lock()
	defer em.Unlock()

	if _, ok := em.holds[e]; !ok {
		return errors.Errorf("Epoch %d was never held", e)
	}

	// Unlike the Commit case, we don't care to know who else might
	// be using this epoch. By deleting it from the map of held epochs,
	// we guarantee that anyone else using it will get an error when
	// they try to commit.
	debug.Printf("Discarding epoch %d", e)
	s, err := em.ch.EpochDiscard(e)
	if err != nil {
		return errors.Wrapf(err, "Failed to discard epoch %d", e)
	}
	delete(em.holds, e)
	em.updateEpochs(s)

	return nil
}

func (em *epochManager) Commit(e daos.Epoch) error {
	em.Lock()
	defer em.Unlock()

	if _, ok := em.holds[e]; !ok {
		return errors.Errorf("Epoch %d was never held", e)
	}
	if em.holds[e] > 0 {
		em.holds[e]--
	}
	if em.holds[e] == 0 {
		// Simple case: No other epochs to wait for, so just
		// commit.
		if len(em.holds) == 1 {
			debug.Printf("Committing epoch %d", e)
			s, err := em.ch.EpochCommit(e)
			if err != nil {
				return errors.Wrapf(err, "Failed to commit epoch %d", e)
			}
			delete(em.holds, e)
			em.updateEpochs(s)
			return nil
		}

		// More complicated: Run through the set of held epochs
		// and see if we can commit them. Any held epoch with > 0
		// hold counts means that we have to return immediately and
		// try again later.
		candidates := em.heldEpochs()
		for i, epoch := range candidates {
			if em.holds[epoch] > 0 {
				return nil
			}

			debug.Printf("Committing epoch %d (%d/%d)", epoch, i+1, len(candidates))
			s, err := em.ch.EpochCommit(epoch)
			if err != nil {
				return errors.Wrapf(err, "Failed to commit epoch %d", epoch)
			}
			delete(em.holds, epoch)
			em.updateEpochs(s)
		}
	}

	return nil
}

func (em *epochManager) Hold(proposed daos.Epoch) (daos.Epoch, error) {
	em.Lock()
	defer em.Unlock()

	if em.heldEpoch != daos.EpochMax {
		em.holds[proposed]++
		debug.Printf("Held internally at %d", proposed)
		return proposed, nil
	}

	actual, err := em.ch.EpochHold(proposed)
	if err != nil {
		return 0, errors.Wrapf(err, "Could not hold @ %d", proposed)
	}
	em.holds[actual]++
	em.heldEpoch = actual
	debug.Printf("DAOS hold at %d (proposed %d)", actual, proposed)

	return actual, nil
}

func (em *epochManager) GetReadEpoch() daos.Epoch {
	em.RLock()
	defer em.RUnlock()

	return em.readEpoch
}

func (em *epochManager) GetWriteEpoch() (daos.Epoch, error) {
	var we daos.Epoch

	em.Lock()
	held := em.heldEpochs()
	if len(held) == 0 {
		we = em.writeEpoch
	} else {
		we = held[len(held)-1] + 1
	}
	em.Unlock()
	debug.Printf("held queue: %d, current: %d, holding: %d", len(held), em.writeEpoch, we)

	return em.Hold(we)
}

func (em *epochManager) heldEpochs() []daos.Epoch {
	// NB: This function is not safe to call outside of a lock!

	epochs := make([]daos.Epoch, 0, len(em.holds))
	for epoch := range em.holds {
		epochs = append(epochs, epoch)
	}

	sort.Slice(epochs, func(i, j int) bool {
		return epochs[i] < epochs[j]
	})

	return epochs
}

func (em *epochManager) updateEpochs(s *daos.EpochState) {
	// NB: This function is not safe to call outside of a lock!

	// Reads at this epoch will always be consistent, as all epochs up
	// to and including this one have been committed.
	// NB: This epoch is global, so it's possible that another process
	// may prevent this one from reading data which is committed on this
	// one's handle. We might want to revisit this decision, but it
	// seems safest.
	em.readEpoch = s.GHCE()

	// At GHPCE, we know that at least one handle has committed all of its
	// epochs up to this point. As DAOSFS has last-writer-wins semantics,
	// we'll start writing on this handle at the next epoch.
	em.writeEpoch = s.GHPCE() + 1

	// Set our currently-held epoch, which should be EpochMax to start with.
	em.heldEpoch = s.LHE()

	debug.Printf("Set readEpoch=%d; writeEpoch=%d, heldEpoch=%d", em.readEpoch, em.writeEpoch, em.heldEpoch)
}

func newEpochManager(ch *daos.ContHandle) (*epochManager, error) {
	em := &epochManager{
		ch:    ch,
		holds: make(map[daos.Epoch]int),
	}

	s, err := em.ch.EpochQuery()
	if err != nil {
		return nil, errors.Wrap(err, "Could not query container epoch")
	}
	em.updateEpochs(s)

	return em, nil
}
