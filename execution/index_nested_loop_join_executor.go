package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// IndexNestedLoopJoinExecutor implements an index nested loop join.
// It iterates over the left child, and for each tuple, probes the index of the right table.
// The expressions given for the left tuple should have the same schema as the right index's key
type IndexNestedLoopJoinExecutor struct {
	planNode          *planner.IndexNestedLoopJoinNode
	leftChildExecutor Executor
	rightIndex        indexing.Index
	rightTableHeap    *TableHeap
	context           *ExecutorContext
	rightBuf          []byte
	combBuf           []byte
	combDesc          *storage.RawTupleDesc
	rids              []common.RecordID
	currentRIDIdx     int
	curRID            common.RecordID
	err               error
	leftTupleSize     int
}

// NewIndexJoinExecutor creates a new IndexNestedLoopJoinExecutor.
// It assumes the left table is accessed via the provided rightIndex and rightTableHeap.
func NewIndexJoinExecutor(plan *planner.IndexNestedLoopJoinNode, left Executor, rightIndex indexing.Index, rightTableHeap *TableHeap) *IndexNestedLoopJoinExecutor {
	leftTupleSize := storage.NewRawTupleDesc(plan.Left.OutputSchema()).BytesPerTuple()

	combDesc := storage.NewRawTupleDesc(plan.OutputSchema())
	rightBuf := make([]byte, rightTableHeap.desc.BytesPerTuple())
	rids := make([]common.RecordID, 0)

	combBuf := make([]byte, combDesc.BytesPerTuple())

	indexLookupExecutor := IndexNestedLoopJoinExecutor{
		planNode:          plan,
		leftChildExecutor: left,
		rightIndex:        rightIndex,
		rightTableHeap:    rightTableHeap,
		context:           nil,
		rightBuf:          rightBuf,
		combBuf:           combBuf,
		combDesc:          combDesc,
		rids:              rids,
		currentRIDIdx:     0,
		curRID:            common.RecordID{},
		err:               nil,
		leftTupleSize:     leftTupleSize,
	}

	return &indexLookupExecutor
}

func (e *IndexNestedLoopJoinExecutor) PlanNode() planner.PlanNode {
	return e.planNode
}

func (e *IndexNestedLoopJoinExecutor) Init(ctx *ExecutorContext) error {
	e.context = ctx
	e.err = nil
	e.rids = e.rids[:0]
	e.currentRIDIdx = 0
	e.curRID = common.RecordID{}

	err := e.leftChildExecutor.Init(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (e *IndexNestedLoopJoinExecutor) Next() bool {
	if e.err != nil {
		return false
	}

	for {
		if e.currentRIDIdx < len(e.rids) {
			rightRid := e.rids[e.currentRIDIdx]
			e.currentRIDIdx++

			if err := e.rightTableHeap.ReadTuple(
				e.context.GetTransaction(),
				rightRid,
				e.rightBuf,
				e.planNode.ForUpdate,
			); err != nil {
				e.err = err
				return false
			}

			e.curRID = rightRid
			return true
		}

		if exists := e.leftChildExecutor.Next(); !exists {
			if err := e.leftChildExecutor.Error(); err != nil {
				e.err = err
			}
			return false
		}

		curLeftTup := e.leftChildExecutor.Current()

		keyVals := make([]common.Value, len(e.planNode.LeftKeys))
		nullKey := false

		for i, expr := range e.planNode.LeftKeys {
			keyVal := expr.Eval(curLeftTup)
			if keyVal.IsNull() {
				nullKey = true
				break
			}
			keyVals[i] = keyVal
		}

		if nullKey {
			continue
		}

		keyTup := storage.FromValues(keyVals...)
		keyBuf := make([]byte, e.rightIndex.Metadata().KeySchema.BytesPerTuple())
		keyTup.WriteToBuffer(keyBuf, e.rightIndex.Metadata().KeySchema)
		key := e.rightIndex.Metadata().AsKey(keyBuf)

		rids, err := e.rightIndex.ScanKey(key, e.rids[:0], e.context.GetTransaction())
		if err != nil {
			e.err = err
			return false
		}

		e.rids = rids
		e.currentRIDIdx = 0

		if len(e.rids) == 0 {
			continue
		}
	}
}

func (e *IndexNestedLoopJoinExecutor) Current() storage.Tuple {
	rightTuple := storage.FromRawTuple(e.rightBuf, e.rightTableHeap.desc, e.curRID)
	return storage.MergeTuples(e.combBuf, e.combDesc, e.leftChildExecutor.Current(), rightTuple)
}

func (e *IndexNestedLoopJoinExecutor) Error() error {
	if e.err != nil {
		return e.err
	}

	return e.leftChildExecutor.Error()

}

func (e *IndexNestedLoopJoinExecutor) Close() error {
	return e.leftChildExecutor.Close()
}
