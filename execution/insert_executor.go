package execution

import (
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
	done               bool
	err                error
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
		numAffected:        0,
		done:               false,
		err:                nil,
	}

	return &insertExecutor
}

func (e *InsertExecutor) PlanNode() planner.PlanNode {
	return e.planNode
}

func (e *InsertExecutor) Init(ctx *ExecutorContext) error {
	e.context = ctx
	e.numAffected = 0
	e.done = false
	e.err = nil
	return e.childExecutor.Init(ctx)
}

func (e *InsertExecutor) Next() bool {
	if e.done || e.err != nil {
		return false
	}

	numAffected := 0
	for exists := e.childExecutor.Next(); exists; exists = e.childExecutor.Next() {
		curTup := e.childExecutor.Current()
		curTup.WriteToBuffer(e.buf, e.insertionTableHeap.desc)
		rid, err := e.insertionTableHeap.InsertTuple(e.context.GetTransaction(), e.buf)
		if err != nil {
			e.err = err
			return false
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

// Going to assume that numWritten should be cumulative or not?
func (e *InsertExecutor) Current() storage.Tuple {
	return storage.FromValues(common.NewIntValue(int64(e.numAffected)))
}

func (e *InsertExecutor) Close() error {
	return e.childExecutor.Close()

}

func (e *InsertExecutor) Error() error {
	if e.err != nil {
		return e.err
	}
	return e.childExecutor.Error()
}
