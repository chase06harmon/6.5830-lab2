package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// LimitExecutor limits the number of tuples returned by the child executor.
type LimitExecutor struct {
	numOutput     int
	planNode      *planner.LimitNode
	childExecutor Executor
}

func NewLimitExecutor(plan *planner.LimitNode, child Executor) *LimitExecutor {
	limitExec := LimitExecutor{
		numOutput:     0,
		planNode:      plan,
		childExecutor: child,
	}

	return &limitExec
}

func (e *LimitExecutor) PlanNode() planner.PlanNode {
	return e.planNode
}

func (e *LimitExecutor) Init(ctx *ExecutorContext) error {
	err := e.childExecutor.Init(ctx)
	e.numOutput = 0
	return err
}

func (e *LimitExecutor) Next() bool {
	if e.numOutput < e.planNode.Limit {
		e.numOutput++
		return e.childExecutor.Next()
	}

	return false
}

func (e *LimitExecutor) Current() storage.Tuple {
	return e.childExecutor.Current()
}

func (e *LimitExecutor) Error() error {
	return e.childExecutor.Error()
}

func (e *LimitExecutor) Close() error {
	return e.childExecutor.Close()
}
