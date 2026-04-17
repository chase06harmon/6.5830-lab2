package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// IndexScanExecutor executes a range scan over an index.
// It iterates through the B+Tree (or other index type) starting from a specific key
// and traversing in a specific direction (Forward or Backward).
type IndexScanExecutor struct {
	planNode      *planner.IndexScanNode
	index         indexing.Index
	tableHeap     *TableHeap
	context       *ExecutorContext
	iter          indexing.ScanIterator
	buf           []byte
	recheckKeyBuf []byte
	curRID        common.RecordID
	err           error
}

func NewIndexScanExecutor(plan *planner.IndexScanNode, index indexing.Index, tableHeap *TableHeap) *IndexScanExecutor {
	buffer := make([]byte, tableHeap.desc.BytesPerTuple())
	recheckKeyBuf := make([]byte, index.Metadata().KeySize())

	indexScanExecutor := IndexScanExecutor{
		planNode:      plan,
		index:         index,
		tableHeap:     tableHeap,
		context:       nil,
		iter:          nil,
		buf:           buffer,
		recheckKeyBuf: recheckKeyBuf,
		curRID:        common.RecordID{},
		err:           nil,
	}

	return &indexScanExecutor
}

func (e *IndexScanExecutor) PlanNode() planner.PlanNode {
	return e.planNode
}

func (e *IndexScanExecutor) Init(ctx *ExecutorContext) error {
	e.context = ctx
	e.err = nil
	e.curRID = common.RecordID{}
	scanIter, err := e.index.Scan(e.planNode.StartKey, e.planNode.Direction, ctx.GetTransaction())
	if err != nil {
		return err
	}

	e.iter = scanIter
	return nil
}

func (e *IndexScanExecutor) Next() bool {
	if e.err != nil || e.iter == nil {
		return false
	}

	for e.iter.Next() {
		rid := e.iter.Value()
		if err := e.tableHeap.ReadTuple(e.context.GetTransaction(), rid, e.buf, e.planNode.ForUpdate); err != nil {
			if err == ErrTupleDeleted {
				continue
			} else {
				e.err = err
				return false
			}
		}

		if e.iter.Key().Compare(
			makeProjectedKeyFromTuple(e.buf, e.tableHeap.desc, e.index.Metadata(), e.recheckKeyBuf),
		) != 0 {
			continue
		}

		e.curRID = rid
		return true
	}

	if err := e.iter.Error(); err != nil {
		e.err = err
	}

	return false
}

func (e *IndexScanExecutor) Current() storage.Tuple {
	return storage.FromRawTuple(e.buf, e.tableHeap.desc, e.curRID)
}

func (e *IndexScanExecutor) Close() error {
	if e.iter == nil {
		return nil
	}
	return e.iter.Close()
}

func (e *IndexScanExecutor) Error() error {
	if e.err != nil {
		return e.err
	}
	if e.iter == nil {
		return nil
	}
	return e.iter.Error()
}
