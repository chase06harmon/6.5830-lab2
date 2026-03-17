package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// DeleteExecutor executes a DELETE query.
// It iterates over the child (which produces the tuples to be deleted with all rows read),
// removes them from the TableHeap, and cleans up all associated Index entries.
type DeleteExecutor struct {
	planNode          *planner.DeleteNode
	childExecutor     Executor
	deletionTableHeap *TableHeap
	indexes           []indexing.Index
	context           *ExecutorContext
	numAffected       int
}

func NewDeleteExecutor(plan *planner.DeleteNode, child Executor, tableHeap *TableHeap, indexes []indexing.Index) *DeleteExecutor {
	deletionExecutor := DeleteExecutor{
		planNode:          plan,
		childExecutor:     child,
		deletionTableHeap: tableHeap,
		indexes:           indexes,
		context:           nil,
		numAffected:       0,
	}

	return &deletionExecutor
}

func (e *DeleteExecutor) PlanNode() planner.PlanNode {
	return e.planNode
}

func (e *DeleteExecutor) Init(ctx *ExecutorContext) error {
	err := e.childExecutor.Init(ctx)
	e.context = ctx
	return err
}

func (e *DeleteExecutor) Next() bool {
	numAffected := 0
	txnContext := e.context.GetTransaction()
	for exists := e.childExecutor.Next(); exists; exists = e.childExecutor.Next() {
		curTup := e.childExecutor.Current()
		err := e.deletionTableHeap.DeleteTuple(txnContext, curTup.RID())
		if err != nil {
			break
		}
		for _, index := range e.indexes {
			md := index.Metadata()
			keyBuf := make([]byte, md.KeySize())

			for k, colIdx := range md.ProjectionList {
				v := curTup.GetValue(colIdx)
				md.KeySchema.SetValue(keyBuf, k, v)
			}
			key := md.AsKey(keyBuf)
			index.DeleteEntry(key, curTup.RID(), txnContext)
		}
		numAffected++
	}

	e.numAffected = numAffected
	return numAffected > 0
}

func (e *DeleteExecutor) Current() storage.Tuple {
	return storage.FromValues(common.NewIntValue(int64(e.numAffected)))
}

func (e *DeleteExecutor) Close() error {
	return e.childExecutor.Close()
}

func (e *DeleteExecutor) Error() error {
	return e.childExecutor.Error()
}
