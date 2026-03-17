package execution

import (
	"fmt"

	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// InsertExecutor executes an INSERT query.
// It consumes tuples from its child (which could be a ValuesExecutor or a SELECT query),
// inserts them into the TableHeap, and updates all associated indexes.
//
// For this course, you may assume that the child does not read from the table you are inserting into
type InsertExecutor struct {
	planNode           *planner.InsertNode
	childExecutor      Executor
	insertionTableHeap *TableHeap
	indexes            []indexing.Index
	context            *ExecutorContext
	buf                []byte
	numAffected        int
}

func NewInsertExecutor(plan *planner.InsertNode, child Executor, tableHeap *TableHeap, indexes []indexing.Index) *InsertExecutor {
	buf := make([]byte, tableHeap.desc.BytesPerTuple())
	insertExecutor := InsertExecutor{
		planNode:           plan,
		childExecutor:      child,
		insertionTableHeap: tableHeap,
		indexes:            indexes,
		context:            nil,
		buf:                buf,
	}

	return &insertExecutor
}

func (e *InsertExecutor) PlanNode() planner.PlanNode {
	return e.planNode
}

func (e *InsertExecutor) Init(ctx *ExecutorContext) error {
	err := e.childExecutor.Init(ctx)
	e.context = ctx
	return err
}

func (e *InsertExecutor) Next() bool {
	numAffected := 0
	for exists := e.childExecutor.Next(); exists; exists = e.childExecutor.Next() {
		curTup := e.childExecutor.Current()
		curTup.WriteToBuffer(e.buf, e.insertionTableHeap.desc)
		rid, err := e.insertionTableHeap.InsertTuple(e.context.GetTransaction(), e.buf)
		if err != nil {
			fmt.Println(err)
			break
		}
		for _, index := range e.indexes {
			md := index.Metadata()
			keyBuf := make([]byte, md.KeySize())

			for k, colIdx := range md.ProjectionList {
				v := e.insertionTableHeap.StorageSchema().GetValue(e.buf, colIdx)
				md.KeySchema.SetValue(keyBuf, k, v)
			}
			key := md.AsKey(keyBuf)
			err = index.InsertEntry(key, rid, e.context.GetTransaction())
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

// Going to assume that numWritten should be cumulative or not?
func (e *InsertExecutor) Current() storage.Tuple {
	return storage.FromValues(common.NewIntValue(int64(e.numAffected)))
}

func (e *InsertExecutor) Close() error {
	return e.childExecutor.Close()

}

func (e *InsertExecutor) Error() error {
	return e.childExecutor.Error()
}
