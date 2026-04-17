package transaction

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/storage"
)

// logRecordBuffer manages a contiguous block of memory for transaction rollback logs.
// It allows efficient allocation and reuse of memory for LogRecords, as well as for the transaction
// to maintain a history of its operations for efficient undo without scanning the log.
type logRecordBuffer struct {
	// buffer holds the serialized undo records.
	buffer []byte

	// offsets tracks the starting indexing of each record in the buffer.
	offsets []int
}

// newLogRecordBuffer creates a stack with some pre-allocated capacity.
func newLogRecordBuffer() *logRecordBuffer {
	return &logRecordBuffer{
		buffer:  make([]byte, 0, common.PageSize),
		offsets: make([]int, 0, 64),
	}
}

// allocate reserves `totalSize` bytes in the buffer for a new record.
// It returns a slice referencing the allocated space.
// It also records the offset of this new record, effectively pushing it onto the stack.
func (s *logRecordBuffer) allocate(totalSize int) []byte {
	if totalSize < 0 {
		panic("negative totalSize")
	}

	start := 0
	if n := len(s.offsets); n > 0 {
		start = s.offsets[n-1]
	}

	s.buffer = append(s.buffer, make([]byte, totalSize)...)
	end := len(s.buffer)

	s.offsets = append(s.offsets, end)
	return s.buffer[start:end]
}

// len returns the number of records currently stored in the buffer.
func (s *logRecordBuffer) len() int {
	return len(s.offsets)
}

// get returns the LogRecord at the specified index `i`.
// The index `i` corresponds to the order of insertion (0 is the first record).
func (s *logRecordBuffer) get(i int) storage.LogRecord {
	numLogRecords := s.len()
	if i < 0 || i >= numLogRecords {
		panic("no log record for index")
	}

	start := 0
	if i > 0 {
		start = s.offsets[i-1]
	}

	end := s.offsets[i]
	return storage.AsLogRecord(s.buffer[start:end])
}

// pop removes the most recently added record from the buffer.
// This effectively rewinds the stack by one record.
func (s *logRecordBuffer) pop() {
	new_end := 0
	if len(s.offsets) > 1 {
		new_end = s.offsets[len(s.offsets)-2]
	}
	s.buffer = s.buffer[:new_end]
	s.offsets = s.offsets[:len(s.offsets)-1]
}

// reset clears the buffer (sets length to 0) without releasing the underlying memory.
// This is used when reusing the TransactionContext.
func (s *logRecordBuffer) reset() {
	s.offsets = s.offsets[:0]
	s.buffer = s.buffer[:0]
}

// TransactionContext holds the runtime state of a single transaction.
type TransactionContext struct {
	id         common.TransactionID
	lm         *LockManager
	logRecords *logRecordBuffer
	heldLocks  map[DBLockTag]DBLockMode

	// These holds in-memory actions (e.g., indexing rollbacks) deferred until transactions end. This is used because in
	// GoDB, indexes are memory-only for simplicity, and do not need to participate in the WAL-driven recovery process.
	// In a real DBMS, indexes are often also on disk and must be protected by a WAL; in fact, indexing recovery is
	// often much more complicated than just the table storage itself, due to multi-page structural modifications
	// (e.g., B-tree merge/split). YOU SHOULD NOT NEED TO MANIPULATE THIS FIELD
	abortActions, commitActions []IndexTask
}

// ID returns the transaction's unique identifier.
func (txn *TransactionContext) ID() common.TransactionID { return txn.id }

// AddAbortTask registers an index task to be executed if the transaction Aborts.
// YOU SHOULD NOT NEED TO CALL OR MODIFY THIS FUNCTION
func (txn *TransactionContext) AddAbortTask(task IndexTask) {
	txn.abortActions = append(txn.abortActions, task)
}

// AddCommitTask registers an index task to be executed just before locks are released on Commit.
// Used to defer index deletions, ensuring the X-lock is still held when stale entries are removed.
// YOU SHOULD NOT NEED TO CALL OR MODIFY THIS FUNCTION
func (txn *TransactionContext) AddCommitTask(task IndexTask) {
	txn.commitActions = append(txn.commitActions, task)
}

func isDowngrade(reqMode DBLockMode, existingMode DBLockMode) bool {
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

// AcquireLock attempts to acquire a lock on the specified resource, checking for reentrancy (if the lock is already
// held).  If the lock cannot be acquired immediately, this call may block or fail due
// to a deadlock.
func (txn *TransactionContext) AcquireLock(tag DBLockTag, mode DBLockMode) error {
	if currentMode, exists := txn.heldLocks[tag]; exists && isDowngrade(mode, currentMode) {
		return nil
	}

	err := txn.lm.Lock(txn.id, tag, mode)
	if err == nil {
		txn.heldLocks[tag] = mode
	}
	return err
}

// HeldLock returns the lock mode this transaction currently holds on the specified resource,
// along with a boolean indicating whether any lock is held.
func (txn *TransactionContext) HeldLock(tag DBLockTag) (DBLockMode, bool) {
	currentMode, exists := txn.heldLocks[tag]

	return currentMode, exists
}

// ReleaseAllLocks releases all locks held by this transaction.
// This is typically called during the Commit or Abort phase of the transaction lifecycle.
func (txn *TransactionContext) ReleaseAllLocks() {
	for tag := range txn.heldLocks {
		txn.lm.Unlock(txn.id, tag)
		delete(txn.heldLocks, tag)
	}
}

// Reset clears the transaction context for reuse.
// This is critical when using sync.Pool to avoid leaking data between users.
func (txn *TransactionContext) Reset(id common.TransactionID) {
	txn.logRecords.reset()
	for dt := range txn.heldLocks {
		txn.lm.Unlock(txn.id, dt)
		delete(txn.heldLocks, dt)
	}
	txn.id = id
	txn.abortActions = txn.abortActions[:0]
	txn.commitActions = txn.commitActions[:0]
}

// NewTestTransactionContext creates a TransactionContext for use in tests that need
// to bypass the TransactionManager. The returned context is fully initialized and
// ready to acquire locks and buffer log records, but no Begin record is written to
// the WAL. Callers are responsible for releasing locks by calling ReleaseAllLocks.
func NewTestTransactionContext(lm *LockManager, id common.TransactionID) *TransactionContext {
	return &TransactionContext{
		id:         id,
		lm:         lm,
		logRecords: newLogRecordBuffer(),
		heldLocks:  make(map[DBLockTag]DBLockMode),
	}
}

// NewBeginTransactionRecord creates a 'Begin' log record using the context's buffer.
func (txn *TransactionContext) NewBeginTransactionRecord() storage.LogRecord {
	buf := txn.logRecords.allocate(storage.BeginTransactionRecordSize())
	return storage.NewBeginTransactionRecord(buf, txn.id)
}

// NewCommitRecord creates a 'Commit' log record using the context's buffer.
func (txn *TransactionContext) NewCommitRecord() storage.LogRecord {
	buf := txn.logRecords.allocate(storage.CommitRecordSize())
	return storage.NewCommitRecord(buf, txn.id)
}

// NewAbortRecord creates an 'Abort' log record using the context's buffer.
func (txn *TransactionContext) NewAbortRecord() storage.LogRecord {
	buf := txn.logRecords.allocate(storage.AbortRecordSize())
	return storage.NewAbortRecord(buf, txn.id)
}

// NewInsertCLR creates a Compensation Log Record (CLR) for an Insert operation.
func (txn *TransactionContext) NewInsertCLR(insertRecord storage.LogRecord) storage.LogRecord {
	buf := txn.logRecords.allocate(storage.InsertCLRSize())
	return storage.NewInsertCLR(buf, insertRecord)
}

// NewInsertRecord creates a log record for an Insert operation.
func (txn *TransactionContext) NewInsertRecord(rid common.RecordID, row storage.RawTuple) storage.LogRecord {
	buf := txn.logRecords.allocate(storage.InsertRecordSize(row))
	return storage.NewInsertRecord(buf, txn.id, rid, row)
}

// NewDeleteCLR creates a Compensation Log Record (CLR) for a Delete operation.
func (txn *TransactionContext) NewDeleteCLR(deleteRecord storage.LogRecord) storage.LogRecord {
	buf := txn.logRecords.allocate(storage.DeleteCLRSize())
	return storage.NewDeleteCLR(buf, deleteRecord)
}

// NewDeleteRecord creates a log record for a Delete operation.
func (txn *TransactionContext) NewDeleteRecord(rid common.RecordID) storage.LogRecord {
	buf := txn.logRecords.allocate(storage.DeleteRecordSize())
	return storage.NewDeleteRecord(buf, txn.id, rid)
}

// NewUpdateCLR creates a Compensation Log Record (CLR) for an Update operation.
func (txn *TransactionContext) NewUpdateCLR(updateRecord storage.LogRecord) storage.LogRecord {
	buf := txn.logRecords.allocate(storage.UpdateCLRSize(updateRecord))
	return storage.NewUpdateCLR(buf, updateRecord)
}

// NewUpdateRecord creates a log record for an Update operation.
func (txn *TransactionContext) NewUpdateRecord(rid common.RecordID, before, after storage.RawTuple) storage.LogRecord {
	buf := txn.logRecords.allocate(storage.UpdateRecordSize(before, after))
	return storage.NewUpdateRecord(buf, txn.id, rid, before, after)
}

// BufferRecordForRecovery updates the transaction's internal undo log when replaying for recovery
//
// Hint: You do not need to worry about this function until lab 4
func (txn *TransactionContext) BufferRecordForRecovery(r storage.LogRecord) {
	panic("unimplemented")
}
