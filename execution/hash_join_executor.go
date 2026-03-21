package execution

import (
	"fmt"

	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// HashJoinExecutor implements the hash join algorithm.
// It builds a hash table from the left child and probes it with the right child.
// It only supports Equi-Joins.
type HashJoinExecutor struct {
	planNode           *planner.HashJoinNode
	leftChildExecutor  Executor
	rightChildExecutor Executor
	hashTable          *ExecutionHashTable[*[]storage.Tuple]
	curRightTuple      storage.Tuple
	curNumOutput       int
	combBuf            []byte
	combDesc           *storage.RawTupleDesc
	err                error
}

// NewHashJoinExecutor creates a new HashJoinExecutor.
func NewHashJoinExecutor(plan *planner.HashJoinNode, left Executor, right Executor) *HashJoinExecutor {
	combTypes := append(left.PlanNode().OutputSchema(), right.PlanNode().OutputSchema()...)
	combDesc := storage.NewRawTupleDesc(combTypes)

	combBuf := make([]byte, combDesc.BytesPerTuple())

	hashJoinExec := HashJoinExecutor{
		planNode:           plan,
		leftChildExecutor:  left,
		rightChildExecutor: right,
		curNumOutput:       -1,
		combBuf:            combBuf,
		combDesc:           combDesc,
		err:                nil,
	}
	return &hashJoinExec
}

func (e *HashJoinExecutor) PlanNode() planner.PlanNode {
	return e.planNode
}

// NOTE: Assumes that must have at least a predicate on each side! Otherwise, equivalent to a filter

type InvalidHashJoinKeysError struct{}

func (InvalidHashJoinKeysError) Error() string {
	return "Insufficient keys for a hash join"
}

func (e *HashJoinExecutor) Init(ctx *ExecutorContext) error {
	e.err = nil

	err := e.leftChildExecutor.Init(ctx)
	if err != nil {
		return err
	}
	err = e.rightChildExecutor.Init(ctx)
	if err != nil {
		return err
	}

	if len(e.planNode.LeftKeys) == 0 || len(e.planNode.RightKeys) == 0 {
		return InvalidHashJoinKeysError{}
	}

	var hashTable *ExecutionHashTable[*[]storage.Tuple]

	joinKeyTypes := make([]common.Type, len(e.planNode.LeftKeys))
	for i, expr := range e.planNode.LeftKeys {
		joinKeyTypes[i] = expr.OutputType()
	}
	hashTable = NewExecutionHashTable[*[]storage.Tuple](storage.NewRawTupleDesc(joinKeyTypes))

	e.hashTable = hashTable

	for exists := e.leftChildExecutor.Next(); exists; exists = e.leftChildExecutor.Next() {
		curTup := e.leftChildExecutor.Current()
		keyVals := make([]common.Value, len(e.planNode.LeftKeys))
		nullKey := false

		for i, keyExpr := range e.planNode.LeftKeys {
			keyVal := keyExpr.Eval(curTup)
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
		curTupsPtr, exists := e.hashTable.Get(keyTup)
		curTupCopy := curTup.DeepCopy(storage.NewRawTupleDesc(e.planNode.Left.OutputSchema()))
		if exists {
			*curTupsPtr = append(*curTupsPtr, curTupCopy)
		} else {
			curTups := []storage.Tuple{curTupCopy}
			e.hashTable.Insert(keyTup, &curTups)
		}
	}
	if leftErr := e.leftChildExecutor.Error(); leftErr != nil {
		e.err = leftErr
		return leftErr
	}

	e.curNumOutput = -1

	return nil
}

func printTupleVal(tup storage.Tuple) {
	fmt.Printf("[")
	for i := 0; i < tup.NumColumns(); i++ {
		switch x := tup.GetValue(i); x.Type() {
		case common.IntType:
			fmt.Printf("%d\t", x.IntValue())
		case common.StringType:
			fmt.Printf("%s\t", x.StringValue())
		default:
			fmt.Printf("NULL\t")
		}

	}

	fmt.Println("]")
}

func (e *HashJoinExecutor) Next() bool {
	if e.err != nil {
		return false
	}

	if e.curNumOutput < 0 {
		if !e.rightChildExecutor.Next() {
			if rightErr := e.rightChildExecutor.Error(); rightErr != nil {
				e.err = rightErr
			}
			return false
		}
		e.curNumOutput = 0
	}

	for {
		curTup := e.rightChildExecutor.Current()
		keyVals := make([]common.Value, len(e.planNode.RightKeys))
		nullKey := false

		for i, keyExpr := range e.planNode.RightKeys {
			keyVal := keyExpr.Eval(curTup)
			if keyVal.IsNull() {
				nullKey = true
				break
			}
			keyVals[i] = keyVal
		}

		if nullKey {
			if !e.rightChildExecutor.Next() {
				if rightErr := e.rightChildExecutor.Error(); rightErr != nil {
					e.err = rightErr
				}
				return false
			}
			e.curNumOutput = 0
			continue
		}

		keyTup := storage.FromValues(keyVals...)
		matches, exists := e.hashTable.Get(keyTup)

		if exists && e.curNumOutput < len(*matches) {
			leftTup := (*matches)[e.curNumOutput]
			storage.MergeTuples(e.combBuf, e.combDesc, leftTup, curTup)
			e.curNumOutput++
			return true
		} else {
			if !e.rightChildExecutor.Next() {
				if rightErr := e.rightChildExecutor.Error(); rightErr != nil {
					e.err = rightErr
				}
				return false
			}
			e.curNumOutput = 0
		}
	}
}

func (e *HashJoinExecutor) Current() storage.Tuple {
	return storage.FromRawTuple(e.combBuf, e.combDesc, common.RecordID{})
}

func (e *HashJoinExecutor) Error() error {
	if e.err != nil {
		return e.err
	}
	rightErr := e.rightChildExecutor.Error()
	leftErr := e.leftChildExecutor.Error()

	if rightErr != nil {
		return rightErr
	}

	if leftErr != nil {
		return leftErr
	}

	return nil
}

func (e *HashJoinExecutor) Close() error {
	rightErr := e.rightChildExecutor.Close()
	leftErr := e.leftChildExecutor.Close()
	if rightErr != nil {
		return rightErr
	}
	return leftErr
}
