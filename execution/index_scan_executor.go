package execution

import (
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// IndexScanExecutor executes a range scan over an index.
// It iterates through the B+Tree (or other index type) starting from a specific key
// and traversing in a specific direction (Forward or Backward).
type IndexScanExecutor struct {
	planNode  *planner.IndexScanNode
	index     indexing.Index
	tableHeap *TableHeap
	context   *ExecutorContext
	iter      indexing.ScanIterator
	buf       []byte
}

func NewIndexScanExecutor(plan *planner.IndexScanNode, index indexing.Index, tableHeap *TableHeap) *IndexScanExecutor {
	buffer := make([]byte, tableHeap.desc.BytesPerTuple())

	indexScanExecutor := IndexScanExecutor{
		planNode:  plan,
		index:     index,
		tableHeap: tableHeap,
		context:   nil,
		iter:      nil,
		buf:       buffer,
	}

	return &indexScanExecutor
}

func (e *IndexScanExecutor) PlanNode() planner.PlanNode {
	return e.planNode
}

func (e *IndexScanExecutor) Init(ctx *ExecutorContext) error {
	e.context = ctx
	scanIter, err := e.index.Scan(e.planNode.StartKey, e.planNode.Direction, ctx.GetTransaction())
	if err != nil {
		return err
	} else {
		e.iter = scanIter
		return nil
	}
}

func (e *IndexScanExecutor) Next() bool {
	return e.iter.Next()
}

func (e *IndexScanExecutor) Current() storage.Tuple {
	rid := e.iter.Value()
	e.tableHeap.ReadTuple(e.context.GetTransaction(), rid, e.buf, e.planNode.ForUpdate)
	return storage.FromRawTuple(e.buf, e.tableHeap.desc, rid)
}

func (e *IndexScanExecutor) Close() error {
	return e.iter.Close()
}

func (e *IndexScanExecutor) Error() error {
	return e.iter.Error()
}
