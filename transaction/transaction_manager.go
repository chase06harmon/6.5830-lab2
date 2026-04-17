package transaction

import (
	"sync"
	"sync/atomic"

	"github.com/puzpuzpuz/xsync/v3"
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/storage"
)

// activeTxnEntry tracks a running transaction and its starting point in the log.
type activeTxnEntry struct {
	txn      *TransactionContext
	startLsn storage.LSN
}

// TransactionManager is the central component managing the lifecycle of transactions.
// It coordinates with the LockManager for concurrency control and the LogManager for
// Write-Ahead Logging (WAL) and recovery.
type TransactionManager struct {
	// activeTxns maps TransactionIDs to their runtime context and metadata
	activeTxns *xsync.MapOf[common.TransactionID, activeTxnEntry]

	logManager  storage.LogManager
	bufferPool  *storage.BufferPool
	lockManager *LockManager

	nextTxnID atomic.Uint64
	// Pool to recycle transaction contexts
	txnPool sync.Pool
}

// NewTransactionManager initializes the transaction manager.
func NewTransactionManager(logManager storage.LogManager, bufferPool *storage.BufferPool, lockManager *LockManager) *TransactionManager {
	tm := &TransactionManager{
		logManager:  logManager,
		bufferPool:  bufferPool,
		lockManager: lockManager,
		activeTxns:  xsync.NewMapOf[common.TransactionID, activeTxnEntry](),
	}

	tm.txnPool.New = func() any {
		return &TransactionContext{
			lm:         lockManager,
			logRecords: newLogRecordBuffer(),
			heldLocks:  make(map[DBLockTag]DBLockMode),
		}
	}

	return tm
}

// Begin starts a new transaction and returns the initialized context.
func (tm *TransactionManager) Begin() (*TransactionContext, error) {
	txnId := common.TransactionID(tm.nextTxnID.Add(1))

	txnContext := tm.txnPool.Get().(*TransactionContext)
	txnContext.Reset(common.TransactionID(txnId))

	lsn, err := tm.logManager.Append(txnContext.NewBeginTransactionRecord())
	if err != nil {
		return nil, err
	}

	tm.activeTxns.Store(txnId, activeTxnEntry{
		txn:      txnContext,
		startLsn: lsn,
	})

	return txnContext, nil
}

// Commit completes a transaction and makes its effects durable and visible.
func (tm *TransactionManager) Commit(txn *TransactionContext) error {
	txnId := txn.ID()

	_, exists := tm.activeTxns.Load(txnId)

	if !exists {
		return common.GoDBError{
			Code:      common.NoSuchObjectError,
			ErrString: "Did not find txn to commit",
		}
	}

	lsn, err := tm.logManager.Append(txn.NewCommitRecord())

	if err != nil {
		return err
	}

	err = tm.logManager.WaitUntilFlushed(lsn)

	if err != nil {
		return err
	}

	// Execute In-Memory changes (Indexes) after flushed. Think about how this should interleave with the commit logic.
	for _, task := range txn.commitActions {
		task.Target.Invoke(task.Type, task.Key, task.RID)
	}

	txn.ReleaseAllLocks()

	tm.activeTxns.Delete(txnId)
	tm.txnPool.Put(txn)

	return nil
}

// Abort stops a transaction and ensures its effects are rolled back
func (tm *TransactionManager) Abort(txn *TransactionContext) error {
	// Rollback In-Memory changes (Indexes)
	// YOU SHOULD NOT NEED TO MODIFY THIS LOGIC
	for i := len(txn.abortActions) - 1; i >= 0; i-- {
		cleanupTask := txn.abortActions[i]
		cleanupTask.Target.Invoke(cleanupTask.Type, cleanupTask.Key, cleanupTask.RID)
	}

	txnId := txn.ID()

	// how do i roll back in memory changes from my txn log records?

	for i := storage.LSN(txn.logRecords.len() - 1); i >= 0; i-- {
		rec := txn.logRecords.get(int(i))

		if rec.RecordType() == storage.LogBeginTransaction {
			break
		}

		rid := rec.RID()

		var clr storage.LogRecord
		var reverse func(hp storage.HeapPage, rid common.RecordID)

		switch rec.RecordType() {
		case storage.LogInsert:
			clr = txn.NewInsertCLR(rec)
			reverse = func(hp storage.HeapPage, rid common.RecordID) {
				// undo insert
				hp.MarkDeleted(rid, true)
			}

		case storage.LogDelete:
			clr = txn.NewDeleteCLR(rec)
			reverse = func(hp storage.HeapPage, rid common.RecordID) {
				// undo delete
				hp.MarkDeleted(rid, false)
			}

		case storage.LogUpdate:
			before := append([]byte(nil), rec.BeforeImage()...) // optional defensive copy
			clr = txn.NewUpdateCLR(rec)
			reverse = func(hp storage.HeapPage, rid common.RecordID) {
				// undo update
				copy(hp.AccessTuple(rid), before)
			}

		default:
			continue
		}

		lsn, err := tm.logManager.Append(clr)

		if err != nil {
			return err
		}

		frame, err := tm.bufferPool.GetPage(rid.PageID)

		if err != nil {
			return err
		}

		frame.MonotonicallyUpdateLSN(lsn)

		frame.PageLatch.Lock()

		hp := frame.AsHeapPage()

		reverse(hp, rid)

		frame.PageLatch.Unlock()
		tm.bufferPool.UnpinPage(frame, true)

	}

	lsn, err := tm.logManager.Append(txn.NewAbortRecord())

	if err != nil {
		return err
	}

	err = tm.logManager.WaitUntilFlushed(lsn)

	if err != nil {
		return err
	}

	txn.ReleaseAllLocks()
	tm.activeTxns.Delete(txnId)
	tm.txnPool.Put(txn)

	return nil
}

// RestartTransactionForRecovery is used during database recovery (ARIES Redo phase).
// It reconstructs a TransactionContext for a transaction that was active at the time of the crash.
//
// Hint: You do not need to worry about this function until lab 4
func (tm *TransactionManager) RestartTransactionForRecovery(txnId common.TransactionID) *TransactionContext {
	panic("unimplemented")
}

// ATTEntry represents a snapshot of an active transaction for the Active Transaction Table (ATT).
type ATTEntry struct {
	ID       common.TransactionID
	StartLSN storage.LSN
}

// GetActiveTransactionsSnapshot returns a snapshot of currently active transaction IDs and their start LSNs.
//
// Hint: You do not need to worry about this function until lab 4
func (tm *TransactionManager) GetActiveTransactionsSnapshot() []ATTEntry {
	panic("unimplemented")
}
