package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// FilterExecutor filters tuples from its child executor based on a predicate.
type FilterExecutor struct {
	planNode      *planner.FilterNode
	childExecutor Executor
}

// NewFilter creates a new FilterExecutor executor.
func NewFilter(plan *planner.FilterNode, child Executor) *FilterExecutor {
	filterExecutor := FilterExecutor{
		planNode:      plan,
		childExecutor: child,
	}

	return &filterExecutor
}

func (e *FilterExecutor) PlanNode() planner.PlanNode {
	return e.planNode
}

// Init initializes the child.
func (e *FilterExecutor) Init(context *ExecutorContext) error {
	err := e.childExecutor.Init(context)
	return err
}

func (e *FilterExecutor) Next() bool {
	for exists := e.childExecutor.Next(); exists; exists = e.childExecutor.Next() {
		tup := e.childExecutor.Current()
		if planner.ExprIsTrue(e.planNode.Predicate.Eval(tup)) {
			return true
		}
	}
	return false
}

func (e *FilterExecutor) Current() storage.Tuple {
	return e.childExecutor.Current()
}

func (e *FilterExecutor) Error() error {
	return e.childExecutor.Error()
}

func (e *FilterExecutor) Close() error {
	return e.childExecutor.Close()
}
