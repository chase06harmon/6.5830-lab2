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
	done              bool
	err               error
}

func NewDeleteExecutor(plan *planner.DeleteNode, child Executor, tableHeap *TableHeap, indexes []indexing.Index) *DeleteExecutor {
	deletionExecutor := DeleteExecutor{
		planNode:          plan,
		childExecutor:     child,
		deletionTableHeap: tableHeap,
		indexes:           indexes,
		context:           nil,
		numAffected:       0,
		done:              false,
		err:               nil,
	}

	return &deletionExecutor
}

func (e *DeleteExecutor) PlanNode() planner.PlanNode {
	return e.planNode
}

func (e *DeleteExecutor) Init(ctx *ExecutorContext) error {
	e.context = ctx
	e.numAffected = 0
	e.done = false
	e.err = nil
	return e.childExecutor.Init(ctx)
}

func (e *DeleteExecutor) Next() bool {
	if e.done || e.err != nil {
		return false
	}

	numAffected := 0
	txnContext := e.context.GetTransaction()
	for exists := e.childExecutor.Next(); exists; exists = e.childExecutor.Next() {
		curTup := e.childExecutor.Current()
		err := e.deletionTableHeap.DeleteTuple(txnContext, curTup.RID())
		if err != nil {
			e.err = err
			return false
		}
		for _, index := range e.indexes {
			md := index.Metadata()
			keyBuf := make([]byte, md.KeySize())

			for k, colIdx := range md.ProjectionList {
				v := curTup.GetValue(colIdx)
				md.KeySchema.SetValue(keyBuf, k, v)
			}
			key := md.AsKey(keyBuf)
			if err := index.DeleteEntry(key, curTup.RID(), txnContext); err != nil {
				e.err = err
				return false
			}
		}
		numAffected++
	}

	if childErr := e.childExecutor.Error(); childErr != nil {
		e.err = childErr
		return false
	}

	e.numAffected = numAffected
	e.done = true
	return true
}

func (e *DeleteExecutor) Current() storage.Tuple {
	return storage.FromValues(common.NewIntValue(int64(e.numAffected)))
}

func (e *DeleteExecutor) Close() error {
	return e.childExecutor.Close()
}

func (e *DeleteExecutor) Error() error {
	if e.err != nil {
		return e.err
	}
	return e.childExecutor.Error()
}
