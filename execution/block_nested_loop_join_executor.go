package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// The size of block, in bytes, that the join operator is allowed to buffer
const blockSize = 1 << 15

// BlockNestedLoopJoinExecutor implements the block nested loop join algorithm.
// It loads a block of tuples from the left child into memory and then scans the right child
// to find matches. This reduces the number of times the right child is sequentially scanned.
type BlockNestedLoopJoinExecutor struct {
	planNode         *planner.NestedLoopJoinNode
	leftExecutor     Executor
	rightExecutor    Executor
	tupsPerBlock     int
	outerChunk       []byte
	curOuterChunkIdx int
	totalLoadedTups  int
	combBuf          []byte
	combDesc         *storage.RawTupleDesc
	context          *ExecutorContext
	started          bool
}

// NewBlockNestedLoopJoinExecutor creates a new BlockNestedLoopJoinExecutor.
func NewBlockNestedLoopJoinExecutor(plan *planner.NestedLoopJoinNode, left Executor, right Executor) *BlockNestedLoopJoinExecutor {
	leftTupleSize := storage.NewRawTupleDesc(plan.Left.OutputSchema()).BytesPerTuple()
	tupsPerBlock := blockSize / leftTupleSize
	outerChunk := make([]byte, tupsPerBlock*leftTupleSize) // could optimize this... seems redudnant to store desc every time!
	combTypes := append(left.PlanNode().OutputSchema(), right.PlanNode().OutputSchema()...)
	combDesc := storage.NewRawTupleDesc(combTypes)

	combBuf := make([]byte, combDesc.BytesPerTuple())

	bNLJExec := BlockNestedLoopJoinExecutor{
		planNode:         plan,
		leftExecutor:     left,
		rightExecutor:    right,
		tupsPerBlock:     tupsPerBlock,
		outerChunk:       outerChunk,
		curOuterChunkIdx: 0,
		combBuf:          combBuf,
		combDesc:         combDesc,
	}
	return &bNLJExec
}

func (e *BlockNestedLoopJoinExecutor) PlanNode() planner.PlanNode {
	return e.planNode
}

func (e *BlockNestedLoopJoinExecutor) Init(ctx *ExecutorContext) error {
	e.context = ctx

	err := e.leftExecutor.Init(ctx)
	if err != nil {
		return err
	}
	err = e.rightExecutor.Init(ctx)
	if err != nil {
		return err
	}

	e.reloadBlock()
	e.started = false

	return nil
}

func (e *BlockNestedLoopJoinExecutor) reloadBlock() {
	leftDesc := storage.NewRawTupleDesc(e.planNode.Left.OutputSchema())
	for i := 0; i < e.tupsPerBlock; i++ {
		exists := e.leftExecutor.Next()
		if !exists {
			e.totalLoadedTups = i
			e.curOuterChunkIdx = 0
			return
		}
		e.leftExecutor.Current().WriteToBuffer(e.outerChunk[i*leftDesc.BytesPerTuple():], leftDesc)
	}

	e.totalLoadedTups = e.tupsPerBlock
	e.curOuterChunkIdx = 0
}

func (e *BlockNestedLoopJoinExecutor) Next() bool {
	if !e.started {
		status := e.rightExecutor.Next()
		if !status {
			return false
		}
		e.started = true
	}

	for {
		for { // every rightTuple
			for ; e.curOuterChunkIdx < e.totalLoadedTups; e.curOuterChunkIdx++ { // finish going through the outerChunk
				leftDesc := storage.NewRawTupleDesc(e.planNode.Left.OutputSchema())
				leftTuple := storage.FromRawTuple(e.outerChunk[e.curOuterChunkIdx*leftDesc.BytesPerTuple():(e.curOuterChunkIdx+1)*leftDesc.BytesPerTuple()], leftDesc, common.RecordID{})
				mergedTup := storage.MergeTuples(e.combBuf, e.combDesc, leftTuple, e.rightExecutor.Current())
				if planner.ExprIsTrue(e.planNode.Predicate.Eval(mergedTup)) {
					e.curOuterChunkIdx++
					return true
				}
			}
			e.curOuterChunkIdx = 0
			rightExists := e.rightExecutor.Next()
			if !rightExists {
				break
			}
		}

		if e.totalLoadedTups < e.tupsPerBlock { // the last reload was the last one
			return false
		} else {
			e.reloadBlock()
			e.rightExecutor.Init(e.context)
			e.rightExecutor.Next()
		}
	}

	// for {
	// 	for ; e.curOuterChunkIdx < e.totalLoadedTups; e.curOuterChunkIdx++ {
	// 		for rightExists := e.rightExecutor.Next(); rightExists; rightExists = e.rightExecutor.Next() {
	// 			leftDesc := storage.NewRawTupleDesc(e.planNode.Left.OutputSchema())
	// 			leftTuple := storage.FromRawTuple(e.outerChunk[e.curOuterChunkIdx*leftDesc.BytesPerTuple():(e.curOuterChunkIdx+1)*leftDesc.BytesPerTuple()], leftDesc, common.RecordID{})
	// 			mergedTup := storage.MergeTuples(e.combBuf, e.combDesc, leftTuple, e.rightExecutor.Current())
	// 			//fmt.Println(mergedTup.GetValue(0), mergedTup.GetValue(6))
	// 			if planner.ExprIsTrue(e.planNode.Predicate.Eval(mergedTup)) {
	// 				return true
	// 			}
	// 		}

	// 		e.rightExecutor.Init(e.context)
	// 	}

	// 	if e.totalLoadedTups < e.tupsPerBlock {
	// 		return false
	// 	} else {
	// 		e.reloadBlock()
	// 	}
	// }
}

func (e *BlockNestedLoopJoinExecutor) Current() storage.Tuple {
	return storage.FromRawTuple(e.combBuf, e.combDesc, common.RecordID{})
}

func (e *BlockNestedLoopJoinExecutor) Error() error {
	return nil
}

func (e *BlockNestedLoopJoinExecutor) Close() error {
	err := e.rightExecutor.Close()
	if err != nil {
		return nil
	}
	err = e.leftExecutor.Close()
	if err != nil {
		return nil
	}

	return nil
}
