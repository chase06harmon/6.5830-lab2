package execution

import (
	"fmt"

	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// UpdateExecutor implements the execution logic for updating tuples in a table.
// It iterates over the tuples provided by its child executor, which represent the full value of the current row
// and its RID. It uses the expressions defined in the plan to calculate the new values for every column in the new row.
// The executor updates the table heap in-place and ensures that all relevant indexes are updated
// if the key columns have changed. It produces a single tuple containing the count of updated rows.
type UpdateExecutor struct {
	planNode        *planner.UpdateNode
	childExecutor   Executor
	updateTableHeap *TableHeap
	indexes         []indexing.Index
	context         *ExecutorContext
	buf             []byte
	numAffected     int
}

func NewUpdateExecutor(plan *planner.UpdateNode, child Executor, tableHeap *TableHeap, indexes []indexing.Index) *UpdateExecutor {
	buf := make([]byte, tableHeap.desc.BytesPerTuple())
	insertExecutor := UpdateExecutor{
		planNode:        plan,
		childExecutor:   child,
		updateTableHeap: tableHeap,
		indexes:         indexes,
		context:         nil,
		buf:             buf,
	}

	return &insertExecutor
}

func (e *UpdateExecutor) PlanNode() planner.PlanNode {
	return e.planNode

}

func (e *UpdateExecutor) Init(ctx *ExecutorContext) error {
	err := e.childExecutor.Init(ctx)
	e.context = ctx
	return err
}

func (e *UpdateExecutor) Next() bool {
	numAffected := 0
	txnContext := e.context.GetTransaction()
	for exists := e.childExecutor.Next(); exists; exists = e.childExecutor.Next() {
		curTup := e.childExecutor.Current()
		newVals := make([]common.Value, e.updateTableHeap.desc.NumColumns())
		updated := make([]bool, e.updateTableHeap.desc.NumColumns())

		for i, expr := range e.planNode.Expressions {
			newVal := expr.Eval(curTup)
			if newVal.Compare(curTup.GetValue(i)) != 0 {
				updated[i] = true
			}
			newVals[i] = newVal
		}
		newTup := storage.FromValues(newVals...)
		newTup.WriteToBuffer(e.buf, e.updateTableHeap.desc)
		err := e.updateTableHeap.UpdateTuple(txnContext, curTup.RID(), e.buf)
		if err != nil {
			break
		}

		for _, index := range e.indexes {
			md := index.Metadata()
			needToUpdateIndex := false

			for _, colIdx := range md.ProjectionList {
				if updated[colIdx] {
					needToUpdateIndex = true
				}
			}

			if !needToUpdateIndex {
				continue
			}

			oldKeyBuf := make([]byte, md.KeySize())
			newKeyBuf := make([]byte, md.KeySize())

			for k, colIdx := range md.ProjectionList {
				old_v := curTup.GetValue(colIdx)
				md.KeySchema.SetValue(oldKeyBuf, k, old_v)

				new_v := newTup.GetValue(colIdx)
				md.KeySchema.SetValue(newKeyBuf, k, new_v)
			}

			oldKey := md.AsKey(oldKeyBuf)
			newKey := md.AsKey(newKeyBuf)

			err = index.DeleteEntry(oldKey, curTup.RID(), txnContext)
			if err != nil {
				fmt.Println(err)
				break
			}

			err = index.InsertEntry(newKey, curTup.RID(), e.context.GetTransaction())
			if err != nil {
				fmt.Println(err)
				break
			}
		}

		numAffected++

	}

	e.numAffected = numAffected
	return numAffected > 0
}

func (e *UpdateExecutor) OutputSchema() []common.Type {
	return e.planNode.OutputSchema()
}

func (e *UpdateExecutor) Current() storage.Tuple {
	return storage.FromValues(common.NewIntValue(int64(e.numAffected)))
}

func (e *UpdateExecutor) Close() error {
	return e.childExecutor.Close()
}

func (e *UpdateExecutor) Error() error {
	return e.childExecutor.Error()
}
