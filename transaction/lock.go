package transaction

import (
	"fmt"
	"sync"

	"mit.edu/dsg/godb/common"
)

// DBLockTag identifies a unique resource (Table or Tuple). It represents Tuple if it has a full RecordID, and
// represents a table if only the oid is set and the rest are set to -1
type DBLockTag struct {
	common.RecordID
}

type WaitEntry struct {
	tid         common.TransactionID
	mode        DBLockMode
	grantedChan chan bool
}

// 5 holder maps... one for each mode type -> txn compatibility checks are easy
// waiters are just general waiters (can construct wait for graph)
type LockObject struct {
	holders [5]map[common.TransactionID]struct{}
	waiters []WaitEntry // might want to use a ring buffer instead
	upgrade *WaitEntry
}

func (lockObj *LockObject) removeAllLocksHeldByTxn(tid common.TransactionID) bool {
	removed := false
	for mode := 0; mode < 5; mode++ {
		if _, found := lockObj.holders[mode][tid]; found {
			delete(lockObj.holders[mode], tid)
			removed = true
		}
	}
	return removed
}

// NewTableLockTag creates a DBLockTag representing a whole table.
func NewTableLockTag(oid common.ObjectID) DBLockTag {
	return DBLockTag{
		RecordID: common.RecordID{
			PageID: common.PageID{
				Oid:     oid,
				PageNum: -1,
			},
			Slot: -1,
		},
	}
}

// NewTupleLockTag creates a DBLockTag representing a specific tuple (row).
func NewTupleLockTag(rid common.RecordID) DBLockTag {
	return DBLockTag{
		RecordID: rid,
	}
}

func (t DBLockTag) String() string {
	if t.PageNum == -1 {
		return fmt.Sprintf("Table(%d)", t.Oid)
	}
	return fmt.Sprintf("Tuple(%d, %d, %d)", t.Oid, t.PageNum, t.Slot)
}

// DBLockMode represents the type of access a transaction is requesting.
// GoDB supports a standard Multi-Granularity Locking hierarchy.
type DBLockMode int

const (
	// LockModeS (Shared) allows reading a resource. Multiple transactions can hold S locks simultaneously.
	LockModeS DBLockMode = iota
	// LockModeX (Exclusive) allows modification. It is incompatible with all other modes.
	LockModeX
	// LockModeIS (Intent Shared) indicates the intention to read resources at a lower level (e.g., locking a table IS to read tuples).
	LockModeIS
	// LockModeIX (Intent Exclusive) indicates the intention to modify resources at a lower level (e.g., locking a table IX to modify tuples).
	LockModeIX
	// LockModeSIX (Shared Intent Exclusive) allows reading the resource (like S) AND the intention to modify lower-level resources (like IX).
	LockModeSIX
)

func (m DBLockMode) String() string {
	switch m {
	case LockModeS:
		return "LockModeS"
	case LockModeX:
		return "LockModeX"
	case LockModeIS:
		return "LockModeIS"
	case LockModeIX:
		return "LockModeIX"
	case LockModeSIX:
		return "LockModeSIX"
	}
	return "Unknown lock mode"
}

// LockManager manages the granting, releasing, and waiting of locks on database resources.
// Note: Probably want a pool of "reusable lock objs"
type LockManager struct {
	mu    sync.Mutex
	locks map[DBLockTag]*LockObject
}

// NewLockManager initializes a new LockManager.
func NewLockManager() *LockManager {
	lm := LockManager{
		mu:    sync.Mutex{},
		locks: make(map[DBLockTag]*LockObject),
	}
	return &lm
}

func (lockObj *LockObject) isLockReqCompatible(tid common.TransactionID, reqMode DBLockMode) bool {
	switch reqMode {
	case LockModeIS:
		return !lockObj.existsOtherLockOfType(LockModeX, tid)
	case LockModeIX:
		return !(lockObj.existsOtherLockOfType(LockModeS, tid) || lockObj.existsOtherLockOfType(LockModeSIX, tid) || lockObj.existsOtherLockOfType(LockModeX, tid))
	case LockModeS:
		return !(lockObj.existsOtherLockOfType(LockModeIX, tid) || lockObj.existsOtherLockOfType(LockModeSIX, tid) || lockObj.existsOtherLockOfType(LockModeX, tid))
	case LockModeSIX:
		return !(lockObj.existsOtherLockOfType(LockModeIX, tid) || lockObj.existsOtherLockOfType(LockModeS, tid) || lockObj.existsOtherLockOfType(LockModeSIX, tid) || lockObj.existsOtherLockOfType(LockModeX, tid))
	case LockModeX:
		return !(lockObj.existsOtherLockOfType(LockModeIS, tid) || lockObj.existsOtherLockOfType(LockModeIX, tid) || lockObj.existsOtherLockOfType(LockModeS, tid) || lockObj.existsOtherLockOfType(LockModeSIX, tid) || lockObj.existsOtherLockOfType(LockModeX, tid))
	default:
		return false // probably want to pass an error back in this case
	}
}

func (lockObj *LockObject) existsOtherLockOfType(mode DBLockMode, tid common.TransactionID) bool {
	_, isCurrentlyHolding := lockObj.holders[mode][tid]
	return !(len(lockObj.holders[mode]) == 0 || (len(lockObj.holders[mode]) == 1 && isCurrentlyHolding))
}

func isReqCoveredByMode(reqMode DBLockMode, existingMode DBLockMode) bool {
	switch existingMode {
	case LockModeX:
		return true
	case LockModeSIX:
		return !(reqMode == LockModeX)
	case LockModeS:
		return (reqMode == LockModeIS)
	case LockModeIX:
		return (reqMode == LockModeIS)
	case LockModeIS:
		return false
	default:
		return false
	}
}

/*
Returns true if potential deadlock is detected
Applies the wait-die strategy... applies to all current holders and waiters!
Higher TID = Lower priority
*/
func (lockObj *LockObject) detectDeadlock(tid common.TransactionID, reqMode DBLockMode) bool {
	for mode := 0; mode < 5; mode++ {
		for holder := range lockObj.holders[mode] {
			if tid > holder {
				// fmt.Println("My tid: ", tid, " Detected holder: ", holder)
				return true
			}
		}
	}

	for _, waiter := range lockObj.waiters {
		if tid > waiter.tid { // && !isCompatible(reqMode, waiter.mode) {
			// fmt.Println("My tid: ", tid, " Detected: ", waiter.tid)
			return true
		}
	}
	if lockObj.upgrade != nil && tid > lockObj.upgrade.tid {
		return true
	}

	return false
}

func (lockObj *LockObject) currentlyHeldLockMode(tid common.TransactionID) DBLockMode {
	for mode := 0; mode < 5; mode++ {
		if _, exists := lockObj.holders[mode][tid]; exists {
			return DBLockMode(mode)
		}
	}

	return -1
}

// Lock acquires a lock on a specific resource (Table or Tuple) with the requested mode. If the lock cannot be acquired
// immediately, the transaction blocks until it is granted or aborted. It returns nil if the lock is successfully
// acquired, or GoDBError(DeadlockError) in case of a (potential or detected) deadlock.
func (lm *LockManager) Lock(tid common.TransactionID, tag DBLockTag, mode DBLockMode) error {
	lm.mu.Lock()

	lockObj, exists := lm.locks[tag]
	if !exists {
		var holders [5]map[common.TransactionID]struct{}
		for i := 0; i < 5; i++ {
			holders[i] = make(map[common.TransactionID]struct{})
		}

		newLockObj := &LockObject{
			holders: holders,
			waiters: make([]WaitEntry, 0),
		}
		lm.locks[tag] = newLockObj
		lockObj = newLockObj
	}

	currentLockMode := lockObj.currentlyHeldLockMode(tid)

	if currentLockMode != -1 { // I currently hold a lock
		if currentLockMode == mode || isReqCoveredByMode(mode, DBLockMode(currentLockMode)) { // if this is not an upgrade request, we should do nothing
			lm.mu.Unlock()
			return nil
		}
	}

	// Do not allow new requests to bypass queued lock requests/upgrades.
	// Existing holders are allowed to proceed (or queue as upgrades) on their own requests.
	hasQueuedRequests := lockObj.upgrade != nil || len(lockObj.waiters) > 0
	if lockObj.isLockReqCompatible(tid, mode) && (!hasQueuedRequests || currentLockMode != -1) {
		if currentLockMode != -1 {
			lockObj.removeAllLocksHeldByTxn(tid)
		}
		lockObj.holders[mode][tid] = struct{}{}
		lm.mu.Unlock()
		return nil
	}

	if lockObj.detectDeadlock(tid, mode) {
		lm.mu.Unlock()
		return common.GoDBError{
			Code:      common.DeadlockError,
			ErrString: "Potential deadlock detected",
		}
	}

	if currentLockMode == -1 {
		grantedChan := make(chan bool, 1)
		newWaiter := WaitEntry{
			tid:         tid,
			mode:        mode,
			grantedChan: grantedChan,
		}

		lockObj.waiters = append(lockObj.waiters, newWaiter)
		lm.mu.Unlock()
		<-grantedChan
		return nil
	} else {
		if lockObj.upgrade == nil {
			grantedChan := make(chan bool, 1)
			newWaiter := WaitEntry{
				tid:         tid,
				mode:        mode,
				grantedChan: grantedChan,
			}

			lockObj.upgrade = &newWaiter
			lm.mu.Unlock()
			<-grantedChan
			return nil
		} else {
			lm.mu.Unlock()
			return common.GoDBError{
				Code:      common.DeadlockError,
				ErrString: "Potential deadlock detected",
			}
		}
	}

}

var INCOMPATIBLE_MATRIX = [5]uint8{
	LockModeIS:  1 << LockModeX,
	LockModeIX:  1<<LockModeS | 1<<LockModeSIX | 1<<LockModeX,
	LockModeS:   1<<LockModeIX | 1<<LockModeSIX | 1<<LockModeX,
	LockModeSIX: 1<<LockModeIX | 1<<LockModeS | 1<<LockModeSIX | 1<<LockModeX,
	LockModeX:   1<<LockModeIS | 1<<LockModeIX | 1<<LockModeS | 1<<LockModeSIX | 1<<LockModeX,
}

func isCompatible(lockMode DBLockMode, otherLockMode DBLockMode) bool {
	return INCOMPATIBLE_MATRIX[lockMode]&(1<<otherLockMode) == 0
}

func (lockObj *LockObject) tryGrantLockToWaiter(waiter WaitEntry) bool {
	// if there is no conflict -> grant
	// if the only conflict is with the same tid, (note can only hold one lock at time) can also grant (but need to move holder to new set)
	for mode := 0; mode < 5; mode++ {
		if INCOMPATIBLE_MATRIX[mode]&(1<<waiter.mode) != 0 { // the request is incompatible with this mode
			_, isReqTxnHolding := lockObj.holders[mode][waiter.tid]
			if !(len(lockObj.holders[mode]) == 1 && isReqTxnHolding) && (len(lockObj.holders[mode]) > 0) {
				return false
			}
		}
	}

	lockObj.removeAllLocksHeldByTxn(waiter.tid)

	// fmt.Println("Lock Granted to: ", waiter.tid)

	lockObj.holders[waiter.mode][waiter.tid] = struct{}{}
	waiter.grantedChan <- true

	return true
}

func (lockObj *LockObject) tryGrantLocks() {
	if lockObj.upgrade != nil {
		if lockObj.tryGrantLockToWaiter(*lockObj.upgrade) {
			lockObj.upgrade = nil
		} else {
			return // upgrade blocks everyone behind it
		}
	}

	i := 0
	for i < len(lockObj.waiters) {
		w := lockObj.waiters[i]
		if !lockObj.tryGrantLockToWaiter(w) {
			break
		}
		i++
	}

	// remove granted prefix only
	lockObj.waiters = lockObj.waiters[i:]
}

// Unlock releases the lock held by the transaction on the specified resource. If the requesting transaction does not
// hold the specified lock, it should return GoDBError(LockNotFoundError)
func (lm *LockManager) Unlock(tid common.TransactionID, tag DBLockTag) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// fmt.Println("Unlocking: ", tid)

	lockObj, exists := lm.locks[tag]
	if !exists {
		return common.GoDBError{
			Code:      common.LockNotFoundError,
			ErrString: "Lock object not found",
		}
	}

	if lockObj.removeAllLocksHeldByTxn(tid) {
		// fmt.Println("Trying to Grant Locks: ", tid)
		lockObj.tryGrantLocks()
		return nil
	}

	return common.GoDBError{
		Code:      common.LockNotFoundError,
		ErrString: "Transaction does not hold lock on requested resource",
	}
}

// LockHeld checks if any transaction currently holds a lock on the given resource.
func (lm *LockManager) LockHeld(tag DBLockTag) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lockObj, exists := lm.locks[tag]
	if !exists {
		return false
	}

	return len(lockObj.holders[0]) > 0 ||
		len(lockObj.holders[1]) > 0 ||
		len(lockObj.holders[2]) > 0 ||
		len(lockObj.holders[3]) > 0 ||
		len(lockObj.holders[4]) > 0
}
