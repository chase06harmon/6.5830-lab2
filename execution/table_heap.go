package execution

import (
	"errors"
	"fmt"
	"sync"

	"mit.edu/dsg/godb/catalog"
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/transaction"
)

// TableHeap represents a physical table stored as a heap file on disk.
// It handles the insertion, update, deletion, and reading of tuples, managing
// interactions with the BufferPool, LockManager, and LogManager.
type TableHeap struct {
	oid         common.ObjectID
	desc        *storage.RawTupleDesc
	bufferPool  *storage.BufferPool
	logManager  storage.LogManager
	lockManager *transaction.LockManager

	numPages        int
	lock            sync.Mutex
	expandingCond   *sync.Cond
	isExpanding     bool
	insertablePages []int
}

// NewTableHeap creates a TableHeap and performs a metadata scan to initialize stats.
func NewTableHeap(table *catalog.Table, bufferPool *storage.BufferPool, logManager storage.LogManager, lockManager *transaction.LockManager) (*TableHeap, error) {
	columnTypes := make([]common.Type, len(table.Columns))
	fmt.Println("CALLING NEW TABLE HEAP")

	for i, col := range table.Columns {
		columnTypes[i] = col.Type
	}

	desc := storage.NewRawTupleDesc(columnTypes)
	dbFile, err := bufferPool.StorageManager().GetDBFile(table.Oid)
	if err != nil {
		return nil, err
	}
	pageNum, err := dbFile.NumPages()
	if err != nil {
		return nil, err
	}

	insertablePages := make([]int, 0)

	if pageNum == 0 {
		if _, err := dbFile.AllocatePage(1); err != nil {
			return nil, err
		}

		frame, err := bufferPool.GetPage(common.PageID{Oid: table.Oid, PageNum: 0})
		if err != nil {
			return nil, err
		}
		frame.PageLatch.Lock()
		storage.InitializeHeapPage(desc, frame)
		frame.PageLatch.Unlock()
		bufferPool.UnpinPage(frame, true)
		pageNum = 1
		insertablePages = append(insertablePages, 0)

	} else {
		for i := 0; i < pageNum; i++ {
			frame, err := bufferPool.GetPage(common.PageID{Oid: table.Oid, PageNum: int32(i)})
			if err != nil {
				return nil, err
			}

			hp := frame.AsHeapPage()
			if hp.NumUsed() < hp.NumSlots() {
				insertablePages = append(insertablePages, i)
			}
		}

	}

	tableHeap := TableHeap{
		oid:             table.Oid,
		desc:            desc,
		bufferPool:      bufferPool,
		logManager:      logManager,
		lockManager:     lockManager,
		numPages:        pageNum,
		insertablePages: insertablePages,
	}

	tableHeap.expandingCond = sync.NewCond(&tableHeap.lock)

	return &tableHeap, nil
}

// StorageSchema returns the physical byte-layout descriptor of the tuples in this table.
func (tableHeap *TableHeap) StorageSchema() *storage.RawTupleDesc {
	return tableHeap.desc
}

// InsertTuple inserts a tuple into the TableHeap. It should find a free space, allocating if needed, and return the found slot.
func (tableHeap *TableHeap) InsertTuple(txn *transaction.TransactionContext, row storage.RawTuple) (common.RecordID, error) {
	for {
		// 1. Safely read numPages, waiting if another goroutine is mid-expansion
		tableHeap.lock.Lock()
		for tableHeap.isExpanding {
			tableHeap.expandingCond.Wait()
		}

		if numInsertable := len(tableHeap.insertablePages); numInsertable > 0 {
			pageNum := tableHeap.insertablePages[numInsertable-1]
			tableHeap.insertablePages = tableHeap.insertablePages[:numInsertable-1]
			tableHeap.lock.Unlock()

			frame, err := tableHeap.bufferPool.GetPage(common.PageID{Oid: tableHeap.oid, PageNum: int32(pageNum)})
			if err != nil {
				return common.RecordID{}, err
			}

			frame.PageLatch.Lock()

			hp := frame.AsHeapPage()
			slot := int32(hp.FindFreeSlot())
			if slot != -1 {
				// Found space — insert and return
				rid := common.RecordID{
					PageID: common.PageID{Oid: tableHeap.oid, PageNum: int32(pageNum)},
					Slot:   slot,
				}
				hp.MarkAllocated(rid, true)
				hp.MarkDeleted(rid, false)

				rawTup := hp.AccessTuple(rid)
				copy(rawTup, row)
				numUsed := hp.NumUsed()
				numSlots := hp.NumSlots()

				frame.PageLatch.Unlock()
				tableHeap.bufferPool.UnpinPage(frame, true)

				tableHeap.lock.Lock()
				if numUsed < numSlots {
					tableHeap.insertablePages = append(tableHeap.insertablePages, pageNum)
				}
				tableHeap.lock.Unlock()

				return rid, nil
			} else {
				frame.PageLatch.Unlock()
				tableHeap.bufferPool.UnpinPage(frame, false)
				continue
			}

		}

		lastPageNum := tableHeap.numPages - 1
		tableHeap.lock.Unlock()

		frame, err := tableHeap.bufferPool.GetPage(common.PageID{Oid: tableHeap.oid, PageNum: int32(lastPageNum)})
		if err != nil {
			return common.RecordID{}, err
		}

		frame.PageLatch.Lock()
		hp := frame.AsHeapPage()

		slot := int32(hp.FindFreeSlot())
		if slot != -1 {
			// Found space — insert and return
			rid := common.RecordID{
				PageID: common.PageID{Oid: tableHeap.oid, PageNum: int32(lastPageNum)},
				Slot:   slot,
			}
			hp.MarkAllocated(rid, true)
			hp.MarkDeleted(rid, false)

			rawTup := hp.AccessTuple(rid)
			copy(rawTup, row)
			frame.PageLatch.Unlock()
			tableHeap.bufferPool.UnpinPage(frame, true)
			return rid, nil
		}

		// Page is full — release it before trying to expand
		frame.PageLatch.Unlock()
		tableHeap.bufferPool.UnpinPage(frame, false)

		// 2. Try to become the expander
		tableHeap.lock.Lock()
		if tableHeap.numPages == lastPageNum+1 && !tableHeap.isExpanding {
			// Nobody else expanded yet — we do it
			tableHeap.isExpanding = true
			tableHeap.lock.Unlock()

			dbFile, err := tableHeap.bufferPool.StorageManager().GetDBFile(tableHeap.oid)
			if err != nil {
				tableHeap.lock.Lock()
				tableHeap.isExpanding = false
				tableHeap.expandingCond.Broadcast()
				tableHeap.lock.Unlock()
				return common.RecordID{}, err
			}
			startPage, err := dbFile.AllocatePage(1)
			if err != nil {
				tableHeap.lock.Lock()
				tableHeap.isExpanding = false
				tableHeap.expandingCond.Broadcast()
				tableHeap.lock.Unlock()
				return common.RecordID{}, err
			}

			newPageNum := startPage

			newFrame, err := tableHeap.bufferPool.GetPage(common.PageID{Oid: tableHeap.oid, PageNum: int32(newPageNum)})
			if err != nil {
				// Clean up the expanding flag so others aren't stuck waiting
				tableHeap.lock.Lock()
				tableHeap.isExpanding = false
				tableHeap.expandingCond.Broadcast()
				tableHeap.lock.Unlock()
				return common.RecordID{}, err
			}

			newFrame.PageLatch.Lock()
			storage.InitializeHeapPage(tableHeap.desc, newFrame)
			newFrame.PageLatch.Unlock()
			tableHeap.bufferPool.UnpinPage(newFrame, true)

			tableHeap.lock.Lock()
			if pages, err := dbFile.NumPages(); err == nil {
				tableHeap.numPages = pages
			} else {
				tableHeap.numPages++
			}
			tableHeap.isExpanding = false
			tableHeap.expandingCond.Broadcast()
			tableHeap.lock.Unlock()
		} else {
			// Someone else already expanded — just retry
			tableHeap.lock.Unlock()
		}
	}
}

var ErrTupleDeleted = errors.New("tuple has been deleted")

// DeleteTuple marks a tuple as deleted in the TableHeap. If the tuple has been deleted, return ErrTupleDeleted
func (tableHeap *TableHeap) DeleteTuple(txn *transaction.TransactionContext, rid common.RecordID) error {
	if tableHeap.oid != rid.PageID.Oid {
		panic("Searching the wrong table")
	}

	frame, err := tableHeap.bufferPool.GetPage(rid.PageID)
	if err != nil {
		return err
	}

	dirty := false
	frame.PageLatch.Lock()

	hp := frame.AsHeapPage()
	if !hp.IsAllocated(rid) || hp.IsDeleted(rid) {
		frame.PageLatch.Unlock()
		tableHeap.bufferPool.UnpinPage(frame, false)
		return ErrTupleDeleted
	}

	hp.MarkDeleted(rid, true)
	dirty = true

	frame.PageLatch.Unlock()
	tableHeap.bufferPool.UnpinPage(frame, dirty)

	tableHeap.lock.Lock()
	defer tableHeap.lock.Unlock()
	for i := 0; i < len(tableHeap.insertablePages); i++ {
		if tableHeap.insertablePages[i] == int(rid.PageID.PageNum) {
			return nil
		}
	}

	tableHeap.insertablePages = append(tableHeap.insertablePages, int(rid.PageID.PageNum))

	return nil
}

// ReadTuple reads the physical bytes of a tuple into the provided buffer. If forUpdate is true, read should acquire
// exclusive lock instead of shared. If the tuple has been deleted, return ErrTupleDeleted
func (tableHeap *TableHeap) ReadTuple(txn *transaction.TransactionContext, rid common.RecordID, buffer []byte, forUpdate bool) error {
	if tableHeap.oid != rid.PageID.Oid {
		panic("Searching the wrong table")
	}

	frame, err := tableHeap.bufferPool.GetPage(rid.PageID)
	if err != nil {
		return err
	}

	defer tableHeap.bufferPool.UnpinPage(frame, false) // or true if you dirtied it

	frame.PageLatch.Lock()
	defer frame.PageLatch.Unlock()

	hp := frame.AsHeapPage()
	if !hp.IsAllocated(rid) || hp.IsDeleted(rid) {
		return ErrTupleDeleted
	} else {
		rawTup := hp.AccessTuple(rid)
		copy(buffer, rawTup)
		return nil
	}
}

// UpdateTuple updates a tuple in-place with new binary data. If the tuple has been deleted, return ErrTupleDeleted.
func (tableHeap *TableHeap) UpdateTuple(txn *transaction.TransactionContext, rid common.RecordID, updatedTuple storage.RawTuple) error {
	if tableHeap.oid != rid.PageID.Oid {
		panic("Searching the wrong table")
	}

	frame, err := tableHeap.bufferPool.GetPage(rid.PageID)
	if err != nil {
		return err
	}

	dirty := false
	defer func() { tableHeap.bufferPool.UnpinPage(frame, dirty) }()
	frame.PageLatch.Lock()
	defer frame.PageLatch.Unlock()

	hp := frame.AsHeapPage()
	if !hp.IsAllocated(rid) || hp.IsDeleted(rid) {
		return ErrTupleDeleted
	}

	rawTup := hp.AccessTuple(rid)
	copy(rawTup, updatedTuple)
	dirty = true
	return nil
}

// VacuumPage attempts to clean up deleted slots on a specific page.
// If slots are deleted AND no transaction holds a lock on them, they are marked as free.
// This is used to reclaim space in the background.
func (tableHeap *TableHeap) VacuumPage(pageID common.PageID) error {
	fmt.Println("CALLING VACUUM PAGE")
	pageFrame, err := tableHeap.bufferPool.GetPage(pageID)
	if err != nil {
		return err
	}

	dirty := false
	defer func() { tableHeap.bufferPool.UnpinPage(pageFrame, dirty) }()
	pageFrame.PageLatch.Lock()
	defer pageFrame.PageLatch.Unlock()

	hp := pageFrame.AsHeapPage()

	for slot := 0; slot < hp.NumSlots(); slot++ {
		rid := common.RecordID{PageID: pageID, Slot: int32(slot)}
		if hp.IsDeleted(rid) {
			hp.MarkAllocated(rid, false)
			hp.MarkDeleted(rid, false)
			dirty = true
		}
	}

	return nil
}

// Iterator creates a new TableHeapIterator to scan the table. It acquires the supplied lock on the table (S, X, or SIX),
// and uses the supplied byte slice to fetch tuples in the returned iterator (for zero-allocation scanning).
func (tableHeap *TableHeap) Iterator(txn *transaction.TransactionContext, mode transaction.DBLockMode, buffer []byte) (TableHeapIterator, error) {
	tableHeap.lock.Lock()
	for tableHeap.isExpanding {
		tableHeap.expandingCond.Wait()
	}
	pages := tableHeap.numPages
	tableHeap.lock.Unlock()

	if pages == 0 {
		return TableHeapIterator{}, nil
	}

	it := TableHeapIterator{
		tableHeap:  tableHeap,
		curPageNum: 0,
		curSlot:    -1,
		curBuf:     buffer,
		numPages:   pages,
		txn:        txn,
		mode:       mode,
	}

	frame, err := tableHeap.bufferPool.GetPage(common.PageID{Oid: tableHeap.oid, PageNum: it.curPageNum})
	if err != nil {
		return TableHeapIterator{}, err
	}
	it.curPageFrame = frame

	return it, nil
}

// TableHeapIterator iterates over all valid (allocated and non-deleted) tuples in the heap.
type TableHeapIterator struct {
	tableHeap    *TableHeap
	curPageNum   int32
	curSlot      int32
	curBuf       []byte
	curPageFrame *storage.PageFrame
	numPages     int // snapshot from creation time

	txn  *transaction.TransactionContext
	mode transaction.DBLockMode
}

// IsNil returns true if the TableHeapIterator is the default, uninitialized value
func (it *TableHeapIterator) IsNil() bool {
	return it == nil || it.tableHeap == nil
}

// Next advances the iterator to the next valid tuple.
// It manages page pins automatically (unpinning the old page when moving to a new one).
func (it *TableHeapIterator) Next() bool {
	for it.curPageNum < int32(it.numPages) {
		it.curPageFrame.PageLatch.Lock()
		found := false

		curHeapPage := it.curPageFrame.AsHeapPage()
		totalSlots := curHeapPage.NumSlots()

		for newSlotNum := it.curSlot + 1; newSlotNum < int32(totalSlots); newSlotNum++ {
			rid := common.RecordID{
				PageID: common.PageID{Oid: it.tableHeap.oid, PageNum: it.curPageNum},
				Slot:   newSlotNum,
			}
			if curHeapPage.IsAllocated(rid) && !curHeapPage.IsDeleted(rid) {
				copy(it.curBuf, curHeapPage.AccessTuple(rid))
				it.curSlot = newSlotNum
				found = true
				break
			}
		}

		it.curPageFrame.PageLatch.Unlock()

		if found {
			return true
		} else {
			it.tableHeap.bufferPool.UnpinPage(it.curPageFrame, false)
			it.curPageFrame = nil

			it.curPageNum++
			it.curSlot = -1

			if it.curPageNum >= int32(it.numPages) {
				return false
			}

			frame, err := it.tableHeap.bufferPool.GetPage(common.PageID{Oid: it.tableHeap.oid, PageNum: it.curPageNum})
			if err != nil {
				// optionally stash err into iterator state and return false
				return false
			}
			it.curPageFrame = frame
		}
	}

	return false
}

// CurrentTuple returns the raw bytes of the tuple at the current cursor position.
// The bytes are valid only until Next() is called again.
func (it *TableHeapIterator) CurrentTuple() storage.RawTuple {
	return it.curBuf
}

// CurrentRID returns the RecordID of the current tuple.
func (it *TableHeapIterator) CurrentRID() common.RecordID {
	return common.RecordID{
		PageID: common.PageID{Oid: it.tableHeap.oid, PageNum: it.curPageNum},
		Slot:   it.curSlot,
	}
}

// CurrentRID returns the first error encountered during iteration, if any.
func (it *TableHeapIterator) Error() error {
	return nil
}

// Close releases any resources associated with the TableHeapIterator
func (it *TableHeapIterator) Close() error {
	if it.curPageFrame != nil {
		it.tableHeap.bufferPool.UnpinPage(it.curPageFrame, false)
		it.curPageFrame = nil
	}

	return nil
}
