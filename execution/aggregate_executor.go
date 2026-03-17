package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// AggregateExecutor implements hash-based aggregation.
type AggregateExecutor struct {
	planNode      *planner.SortNode
	childExecutor Executor
}

func NewAggregateExecutor(plan *planner.AggregateNode, child Executor) *AggregateExecutor {
	panic("unimplemented")
}

func (e *AggregateExecutor) PlanNode() planner.PlanNode {
	panic("unimplemented")
}

func (e *AggregateExecutor) Init(ctx *ExecutorContext) error {
	panic("unimplemented")
}

func (e *AggregateExecutor) Next() bool {
	panic("unimplemented")
}

func (e *AggregateExecutor) Current() storage.Tuple {
	panic("unimplemented")
}

func (e *AggregateExecutor) Error() error {
	panic("unimplemented")
}

func (e *AggregateExecutor) Close() error {
	panic("unimplemented")
}
