package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// AggregateExecutor implements hash-based aggregation.
type AggregateExecutor struct {
	planNode      *planner.AggregateNode
	childExecutor Executor
	hashTable     *ExecutionHashTable[[]common.Value]
	iterChan      chan storage.Tuple
	curTuple      storage.Tuple
	err           error
}

func NewAggregateExecutor(plan *planner.AggregateNode, child Executor) *AggregateExecutor {
	numGroupByKeys := len(plan.GroupByClause)
	var hashTable *ExecutionHashTable[[]common.Value]

	if numGroupByKeys > 0 {
		groupByKeyTypes := make([]common.Type, len(plan.GroupByClause))
		for i, expr := range plan.GroupByClause {
			groupByKeyTypes[i] = expr.OutputType()
		}
		hashTable = NewExecutionHashTable[[]common.Value](storage.NewRawTupleDesc(groupByKeyTypes))
	}

	aggExec := AggregateExecutor{
		planNode:      plan,
		childExecutor: child,
		hashTable:     hashTable,
		err:           nil,
	}

	aggExec.iterChan = make(chan storage.Tuple)

	return &aggExec
}

func (e *AggregateExecutor) PlanNode() planner.PlanNode {
	return e.planNode
}

func (e *AggregateExecutor) Init(ctx *ExecutorContext) error {
	e.err = nil
	e.iterChan = make(chan storage.Tuple)

	if err := e.childExecutor.Init(ctx); err != nil {
		return err
	}

	if len(e.planNode.GroupByClause) > 0 {
		groupByKeyTypes := make([]common.Type, len(e.planNode.GroupByClause))
		for i, expr := range e.planNode.GroupByClause {
			groupByKeyTypes[i] = expr.OutputType()
		}
		e.hashTable = NewExecutionHashTable[[]common.Value](storage.NewRawTupleDesc(groupByKeyTypes))
	}

	if len(e.planNode.GroupByClause) > 0 {
		exists := e.childExecutor.Next()
		if !exists {
			if err := e.childExecutor.Error(); err != nil {
				e.err = err
				close(e.iterChan)
				return err
			}
			close(e.iterChan)
			return nil
		}
		for ; exists; exists = e.childExecutor.Next() {
			curTup := e.childExecutor.Current()
			keyVals := make([]common.Value, len(e.planNode.GroupByClause))

			for i, groupByExpr := range e.planNode.GroupByClause {
				keyVals[i] = groupByExpr.Eval(curTup)
			}
			keyTup := storage.FromValues(keyVals...)
			curAgg, exists := e.hashTable.Get(keyTup)
			if exists {
				for i, aggExpr := range e.planNode.AggClauses {
					curAggVal := curAgg[i]
					newVal := aggExpr.Expr.Eval(curTup)
					curAgg[i] = computeNewAggValue(curAggVal, newVal, aggExpr.Type)
				}
			} else {
				aggVals := make([]common.Value, len(e.planNode.AggClauses))
				for i, aggExpr := range e.planNode.AggClauses {
					var curAggVal common.Value
					if aggExpr.Type == planner.AggCount {
						curAggVal = common.NewIntValue(0)
					} else if aggExpr.Expr.OutputType() == common.IntType {
						curAggVal = common.NewNullInt()
					} else {
						curAggVal = common.NewNullString()
					}
					newVal := aggExpr.Expr.Eval(curTup)
					aggVals[i] = computeNewAggValue(curAggVal, newVal, aggExpr.Type)
				}
				e.hashTable.Insert(keyTup, aggVals)
			}
		}

		if err := e.childExecutor.Error(); err != nil {
			e.err = err
			close(e.iterChan)
			return err
		}

		go func() {
			e.hashTable.Iterate(func(key storage.Tuple, value []common.Value) {
				e.iterChan <- key.Extend(value)
			})
			close(e.iterChan)
		}()

		return nil
	} else {
		aggVals := make([]common.Value, len(e.planNode.AggClauses))
		for i, aggExpr := range e.planNode.AggClauses {
			var curAggVal common.Value
			if aggExpr.Type == planner.AggCount {
				curAggVal = common.NewIntValue(0)
			} else if aggExpr.Expr.OutputType() == common.IntType {
				curAggVal = common.NewNullInt()
			} else {
				curAggVal = common.NewNullString()
			}
			aggVals[i] = curAggVal
		}
		exists := e.childExecutor.Next()
		if !exists {
			if err := e.childExecutor.Error(); err != nil {
				e.err = err
				close(e.iterChan)
				return err
			}
			close(e.iterChan)
			return nil
		}

		for ; exists; exists = e.childExecutor.Next() {
			curTup := e.childExecutor.Current()
			for i, aggExpr := range e.planNode.AggClauses {
				curAggVal := aggVals[i]
				newVal := aggExpr.Expr.Eval(curTup)
				aggVals[i] = computeNewAggValue(curAggVal, newVal, aggExpr.Type).Copy()
			}
		}

		if err := e.childExecutor.Error(); err != nil {
			e.err = err
			close(e.iterChan)
			return err
		}

		go func() {
			e.iterChan <- storage.FromValues(aggVals...)
			close(e.iterChan)
		}()

		return nil
	}

}

func computeNewAggValue(curAggVal common.Value, newVal common.Value, aggType planner.AggregatorType) common.Value {
	// fmt.Println(curAggVal, ", ", newVal)
	if newVal.IsNull() {
		return curAggVal
	}

	if curAggVal.IsNull() {
		switch aggType {
		case planner.AggCount:
			return common.NewIntValue(1)
		default:
			return newVal
		}

	} else { // both are not null
		switch aggType {
		case planner.AggCount:
			return curAggVal.Increment()
		case planner.AggSum:
			return common.NewIntValue(curAggVal.IntValue() + newVal.IntValue())
		case planner.AggMin:
			if curAggVal.Compare(newVal) == 1 { // curAggVal > newVal
				return newVal
			} else {
				return curAggVal
			}
		case planner.AggMax:
			if curAggVal.Compare(newVal) == -1 { // curAggVal < newVal
				return newVal
			} else {
				return curAggVal
			}

		default:
			return curAggVal
		}
	}
}

func (e *AggregateExecutor) Next() bool {
	if e.err != nil {
		return false
	}
	tup, ok := <-e.iterChan
	if !ok {
		return false
	}
	e.curTuple = tup
	return true
}

func (e *AggregateExecutor) Current() storage.Tuple {
	return e.curTuple
}

func (e *AggregateExecutor) Error() error {
	if e.err != nil {
		return e.err
	}
	return e.childExecutor.Error()
}

func (e *AggregateExecutor) Close() error {
	return e.childExecutor.Close()
}
