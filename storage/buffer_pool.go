package storage

import (
	"sync"

	"mit.edu/dsg/godb/common"
)

// BufferPool manages the reading and writing of database pages between the DiskFileManager and memory.
// It acts as a central cache to keep "hot" pages in memory with fixed capacity and selectively evicts
// pages to disk when the pool becomes full. Users will need to coordinate concurrent access to pages
// using page-level latches and metadata (which you should define in page.go). All methods
// must be thread-safe, as multiple threads will request the same or different pages concurrently.
// To get full credit, you likely need to do better than coarse-grained latching (i.e., a global latch for the entire
// BufferPool instance).
type BufferPool struct {
	numPages       int
	pageTable      map[common.PageID]int
	pageArray      []*PageFrame
	storageManager DBFileManager
	logManager     LogManager
	rareQueue      Queue
	frequentQueue  Queue
	lock           sync.Mutex
	evictCond      *sync.Cond
}

type QueueNode struct {
	pageId common.PageID
	next   *QueueNode
	prev   *QueueNode
}

type Queue struct {
	head *QueueNode
	tail *QueueNode
}

func makeQueue() Queue {
	head := &QueueNode{}
	tail := &QueueNode{}

	head.next = tail
	head.prev = nil

	tail.prev = head
	tail.next = nil

	return Queue{head: head, tail: tail}

}

func (q *Queue) insertFront(qNode *QueueNode) {
	head := q.head
	originalFirst := q.head.next

	head.next = qNode
	qNode.prev = head

	qNode.next = originalFirst
	originalFirst.prev = qNode
}

func (q *Queue) removeFromQueue(qNode *QueueNode) {
	originalPrev := qNode.prev
	originalNext := qNode.next

	originalPrev.next = originalNext
	originalNext.prev = originalPrev

	qNode.next = nil
	qNode.prev = nil
}

func (q *Queue) moveToFront(qNode *QueueNode) {
	originalPrev := qNode.prev
	originalNext := qNode.next

	originalPrev.next = originalNext
	originalNext.prev = originalPrev

	head := q.head
	originalFirst := q.head.next

	head.next = qNode
	qNode.prev = head

	qNode.next = originalFirst
	originalFirst.prev = qNode
}

// Assumes the thread already holds the buffer pool lock
func (bp *BufferPool) findEvictionVictim(q *Queue) (*QueueNode, bool) {
	for cur := q.tail.prev; cur != nil && cur.prev != nil; cur = cur.prev {
		idx, ok := bp.pageTable[cur.pageId]
		if !ok {
			panic("page in queue but not in pageTable")
		}

		pf := bp.pageArray[idx]
		if pf.pinCount == 0 && pf.state == FrameReady {
			// A frame can briefly reach pinCount==0 before its page latch is released
			// by user code. Avoid evicting while the latch is still held.
			if !pf.PageLatch.TryLock() {
				continue
			}
			pf.PageLatch.Unlock()
			return cur, true
		}
	}
	return nil, false
}

// NewBufferPool creates a new BufferPool with a fixed capacity defined by numPages. It requires a
// storageManager to handle the underlying disk I/O operations.
//
// Hint: You will need to worry about logManager until Lab 3
func NewBufferPool(numPages int, storageManager DBFileManager, logManager LogManager) *BufferPool {
	pageTable := make(map[common.PageID]int)
	pageArray := make([]*PageFrame, numPages)

	for idx := range pageArray {
		pf := PageFrame{}
		pageArray[idx] = &pf
	}

	bp := BufferPool{
		numPages:       numPages,
		pageTable:      pageTable,
		pageArray:      pageArray,
		storageManager: storageManager,
		logManager:     logManager,
		rareQueue:      makeQueue(),
		frequentQueue:  makeQueue(),
	}

	bp.evictCond = sync.NewCond(&bp.lock)

	for i := range bp.pageArray {
		pf := bp.pageArray[i]
		pf.stateCond = sync.NewCond(&bp.lock)
		pf.state = FrameFree

	}

	return &bp
}

// StorageManager returns the underlying disk manager.
func (bp *BufferPool) StorageManager() DBFileManager {
	return bp.storageManager
}

// assumes you hold the bp.lock
func (bp *BufferPool) insertNewPage(pageFrame *PageFrame, pageID common.PageID) {
	pageFrame.state = FrameLoading

	qn := &QueueNode{
		pageId: pageID,
	}

	bp.rareQueue.insertFront(qn)

	pageFrame.whichQueue = RareQueue
	pageFrame.queueNode = qn
	pageFrame.pinCount = 1
	pageFrame.dirty = false
	pageFrame.pageId = pageID

	bp.lock.Unlock()

	dbFile, _ := bp.storageManager.GetDBFile(pageID.Oid) // maybe want to propogate this error?
	dbFile.ReadPage(int(pageID.PageNum), pageFrame.Bytes[:])

	bp.lock.Lock()

	pageFrame.state = FrameReady
	// pageFrame.resident = true
	pageFrame.stateCond.Broadcast()

}

// assumes you hold the bp.lock
func (bp *BufferPool) evictAndInsert(queue *Queue, nodeToEvict *QueueNode, replacementPageID common.PageID) {
	oldPageId := nodeToEvict.pageId
	// fmt.Println("CALLING EVICT PAGE ON: ", oldPageId)
	pageArrayIdx, ok := bp.pageTable[oldPageId]
	if !ok {
		panic("selected eviction victim is not in pageTable")
	}

	oldPageFrame := bp.pageArray[pageArrayIdx]

	oldPageFrame.state = FrameEvicting

	bp.pageTable[replacementPageID] = pageArrayIdx // need to tell other people trying to access this page that it is in the process of being evicted

	if oldPageFrame.dirty {
		bp.lock.Unlock()
		// fmt.Println("DIRTY")
		dbFile, _ := bp.storageManager.GetDBFile(oldPageId.Oid)
		dbFile.WritePage(int(oldPageId.PageNum), oldPageFrame.Bytes[:])

		bp.lock.Lock()
	}

	pageFrame := oldPageFrame

	delete(bp.pageTable, oldPageId)
	queue.removeFromQueue(nodeToEvict)

	pageFrame.state = FrameLoading

	qn := &QueueNode{
		pageId: replacementPageID,
	}

	bp.rareQueue.insertFront(qn)

	pageFrame.whichQueue = RareQueue
	pageFrame.queueNode = qn
	pageFrame.pinCount = 1
	pageFrame.dirty = false
	pageFrame.pageId = replacementPageID

	bp.lock.Unlock()

	dbFile, _ := bp.storageManager.GetDBFile(replacementPageID.Oid) // maybe want to propogate this error?
	dbFile.ReadPage(int(replacementPageID.PageNum), pageFrame.Bytes[:])

	bp.lock.Lock()

	pageFrame.state = FrameReady
	// pageFrame.resident = true
	pageFrame.stateCond.Broadcast()
	bp.evictCond.Broadcast()

}

type GetPageError struct{}

func (err GetPageError) Error() string {
	return "No pages available for eviction"
}

func (bp *BufferPool) waitReady(pf *PageFrame) {
	for pf.state != FrameReady {
		pf.stateCond.Wait()
	}
}

// Sometimes you just need "not busy with IO/eviction"
func (bp *BufferPool) waitNotBusy(pf *PageFrame) {
	for pf.state == FrameLoading || pf.state == FrameFlushing || pf.state == FrameEvicting {
		pf.stateCond.Wait()
	}
}

// GetPage retrieves a page from the buffer pool, ensuring it is pinned (i.e. prevented from eviction until
// unpinned) and ready for use. If the page is already in the pool, the cached bytes are returned. If the page is not
// present, the method must first make space by selecting a victim frame to evict
// (potentially writing it to disk if dirty), and then read the requested page from disk into that frame.
func (bp *BufferPool) GetPage(pageID common.PageID) (*PageFrame, error) {
	bp.lock.Lock()
	defer bp.lock.Unlock()

	for {
		pageArrayIdx, cached := bp.pageTable[pageID]

		if !cached {
			// --- Not in page table at all: load it --- once the page table fills the map should never decrease in size below bp.numPages
			if len(bp.pageTable) < bp.numPages {
				idx := len(bp.pageTable)
				bp.pageTable[pageID] = idx
				pf := bp.pageArray[idx]
				bp.insertNewPage(pf, pageID)
				return pf, nil
			}

			if node, ok := bp.findEvictionVictim(&bp.rareQueue); ok {
				pf := bp.pageArray[bp.pageTable[node.pageId]]
				bp.evictAndInsert(&bp.rareQueue, node, pageID)
				return pf, nil
			}
			if node, ok := bp.findEvictionVictim(&bp.frequentQueue); ok {
				pf := bp.pageArray[bp.pageTable[node.pageId]]
				bp.evictAndInsert(&bp.frequentQueue, node, pageID)
				return pf, nil
			}

			bp.evictCond.Wait()
			continue
		}

		// --- Cached: we have a valid index ---
		pf := bp.pageArray[pageArrayIdx]

		if pf.state == FrameEvicting {
			// Another thread is evicting the old occupant to load our page
			pf.stateCond.Wait()
			continue
		}

		pf.pinCount++
		bp.waitReady(pf)

		switch pf.whichQueue {
		case RareQueue:
			bp.rareQueue.removeFromQueue(pf.queueNode)
			bp.frequentQueue.insertFront(pf.queueNode)
			pf.whichQueue = FrequentQueue
		case FrequentQueue:
			bp.frequentQueue.moveToFront(pf.queueNode)
		}

		return pf, nil
	}
}

// UnpinPage indicates that the caller is done using a page. It unpins the page, making the page potentially evictable
// if no other thread is accessing it. If the setDirty flag is true, the page is marked as modified, ensuring
// it will be written back to disk before eviction.
func (bp *BufferPool) UnpinPage(frame *PageFrame, setDirty bool) {
	bp.lock.Lock()
	defer bp.lock.Unlock()

	frame.pinCount--

	if setDirty {
		// fmt.Println("SETTING DIRTY")
		frame.dirty = true
	}

	if frame.pinCount == 0 {
		bp.evictCond.Broadcast() // or Broadcast() if you want
	}
}

<<<<<<< HEAD
// FlushAllPages flushes all dirty pages to disk that have an LSN less than `flushedUntil`, regardless of pins.
// This is typically called during a checkpoint or Shutdown to ensure durability, but also useful for tests
=======
// FlushAllPages flushes all dirty pages to disk that have an LSN less than `flushedUntil`, regardless of pins.F
// This is typically called during a Checkpoint or Shutdown to ensure durability, but also useful for tests
>>>>>>> a6e2bbc (working)
func (bp *BufferPool) FlushAllPages() error {
	type req struct {
		pid common.PageID
		buf []byte
		pf  *PageFrame
	}
	var reqs []req

	bp.lock.Lock()
	for pid, idx := range bp.pageTable {
		pf := bp.pageArray[idx]
		bp.waitNotBusy(pf)

		if !pf.dirty {
			continue
		}

		// Prevent eviction / reuse while we drop bp.lock and do IO.
		pf.state = FrameFlushing

		pf.PageLatch.RLock()
		buf := make([]byte, common.PageSize)
		copy(buf, pf.Bytes[:])
		pf.PageLatch.RUnlock()

		reqs = append(reqs, req{pid: pid, buf: buf, pf: pf})
	}
	bp.lock.Unlock()

	// Do IO without bp.lock
	for _, r := range reqs {
		dbFile, _ := bp.storageManager.GetDBFile(r.pid.Oid)
		_ = dbFile.WritePage(int(r.pid.PageNum), r.buf)
	}

	bp.lock.Lock()
	for _, r := range reqs {
		r.pf.dirty = false
		r.pf.state = FrameReady
		r.pf.stateCond.Broadcast()
	}

	bp.evictCond.Broadcast()
	bp.lock.Unlock()
	return nil
}

// GetDirtyPageTableSnapshot returns a map of all currently dirty pages and their RecoveryLSN.
// This is called during checkpoint to snapshot the current DPT into the log.
//
// Hint: You do not need to worry about this function until lab 4
func (bp *BufferPool) GetDirtyPageTableSnapshot() map[common.PageID]LSN {
	// You will not need to implement this until lab4
	panic("unimplemented")
}
