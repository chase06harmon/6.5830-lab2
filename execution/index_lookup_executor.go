package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// IndexLookupExecutor implements a Point Lookup using an index. Unlike a full Index Scan, which iterates over a
// range of keys, this executor efficiently retrieves only the tuples that match a specific equality key
// (e.g., "SELECT * FROM users WHERE id = 5").
type IndexLookupExecutor struct {
	planNode      *planner.IndexLookupNode
	index         indexing.Index
	tableHeap     *TableHeap
	context       *ExecutorContext
	buf           []byte
	rids          []common.RecordID
	currentRIDIdx int
}

func NewIndexLookupExecutor(plan *planner.IndexLookupNode, index indexing.Index, tableHeap *TableHeap) *IndexLookupExecutor {
	buffer := make([]byte, tableHeap.desc.BytesPerTuple())
	rids := make([]common.RecordID, 0)

	indexLookupExecutor := IndexLookupExecutor{
		planNode:      plan,
		index:         index,
		tableHeap:     tableHeap,
		context:       nil,
		buf:           buffer,
		rids:          rids,
		currentRIDIdx: -1,
	}

	return &indexLookupExecutor
}

func (e *IndexLookupExecutor) PlanNode() planner.PlanNode {
	return e.planNode
}

func (e *IndexLookupExecutor) Init(ctx *ExecutorContext) error {
	e.context = ctx
	e.rids = e.rids[:0]
	rids, err := e.index.ScanKey(e.planNode.EqualityKey, e.rids, ctx.GetTransaction())

	if err != nil {
		return err
	} else {
		e.rids = rids
		e.currentRIDIdx = -1
		return nil
	}
}

func (e *IndexLookupExecutor) Next() bool {
	if e.currentRIDIdx+1 < len(e.rids) {
		e.currentRIDIdx++
		return true
	}

	return false
}

func (e *IndexLookupExecutor) Current() storage.Tuple {
	rid := e.rids[e.currentRIDIdx]
	e.tableHeap.ReadTuple(e.context.GetTransaction(), rid, e.buf, e.planNode.ForUpdate)
	return storage.FromRawTuple(e.buf, e.tableHeap.desc, rid)
}

func (e *IndexLookupExecutor) Close() error {
	return nil
}

func (e *IndexLookupExecutor) Error() error {
	return nil
}
