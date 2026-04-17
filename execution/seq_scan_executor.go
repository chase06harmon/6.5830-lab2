package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// SeqScanExecutor implements a sequential scan over a table.
type SeqScanExecutor struct {
	scanNode  *planner.SeqScanNode
	tableHeap *TableHeap

	iterator *TableHeapIterator
	buffer   []byte
}

// NewSeqScanExecutor creates a new SeqScanExecutor.
func NewSeqScanExecutor(plan *planner.SeqScanNode, tableHeap *TableHeap) *SeqScanExecutor {
	buf := make([]byte, tableHeap.desc.BytesPerTuple())
	seqScanExecutor := SeqScanExecutor{
		plan,
		tableHeap,
		nil,
		buf,
	}

	return &seqScanExecutor
}

func (e *SeqScanExecutor) PlanNode() planner.PlanNode {
	return e.scanNode
}

func (e *SeqScanExecutor) Init(context *ExecutorContext) error {
	// might need to clear buffer here.. close original e.iterator
	iter, err := e.tableHeap.Iterator(
		context.GetTransaction(), e.scanNode.Mode, e.buffer,
	)

	e.iterator = &iter

	if err != nil {
		return err
	}

	return nil

}

// Note: Probably want to preallocate buffer and copy contents

func (e *SeqScanExecutor) Next() bool {
	return e.iterator.Next()
}

func (e *SeqScanExecutor) Current() storage.Tuple {
	return storage.FromRawTuple(e.iterator.CurrentTuple(), e.tableHeap.StorageSchema(), e.iterator.CurrentRID())
}

func (e *SeqScanExecutor) Error() error {
	return e.iterator.Error()
}

func (e *SeqScanExecutor) Close() error {
	// maybe release the buffer here?
	return e.iterator.Close()
}
