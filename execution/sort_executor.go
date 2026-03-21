package execution

import (
	"sort"

	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// SortExecutor sorts the input tuples based on the provided ordering expressions.
// It is a blocking operator but uses lazy evaluation (sorts on first Next).
type SortExecutor struct {
	planNode      *planner.SortNode
	childExecutor Executor
	tuples        []storage.Tuple
	curIdx        int
	err           error
}

func NewSortExecutor(plan *planner.SortNode, child Executor) *SortExecutor {
	se := SortExecutor{
		planNode:      plan,
		childExecutor: child,
		err:           nil,
	}
	se.tuples = make([]storage.Tuple, 0)
	return &se
}

func (e *SortExecutor) PlanNode() planner.PlanNode {
	return e.planNode
}

func (e *SortExecutor) Init(ctx *ExecutorContext) error {
	e.tuples = e.tuples[:0]
	e.curIdx = -1
	e.err = nil

	err := e.childExecutor.Init(ctx)
	if err != nil {
		return err
	}
	for exists := e.childExecutor.Next(); exists; exists = e.childExecutor.Next() {
		e.tuples = append(e.tuples, e.childExecutor.Current().DeepCopy(storage.NewRawTupleDesc(e.planNode.Child.OutputSchema())))
	}
	if childErr := e.childExecutor.Error(); childErr != nil {
		e.err = childErr
		return childErr
	}

	sort.Slice(e.tuples, func(i, j int) bool {
		a, b := e.tuples[i], e.tuples[j]
		for _, ob := range e.planNode.OrderBy {
			cmp := ob.Expr.Eval(a).Compare(ob.Expr.Eval(b))
			if cmp == 0 {
				continue
			}
			if ob.Direction == planner.SortOrderAscending {
				return cmp < 0
			}
			return cmp > 0
		}
		return false
	})
	return nil
}

func (e *SortExecutor) Next() bool {
	if e.curIdx+1 < len(e.tuples) {
		e.curIdx++
		return true
	} else {
		return false
	}

}

func (e *SortExecutor) Current() storage.Tuple {
	return e.tuples[e.curIdx]
}

func (e *SortExecutor) Error() error {
	if e.err != nil {
		return e.err
	}
	return e.childExecutor.Error()
}

func (e *SortExecutor) Close() error {
	return e.childExecutor.Close()
}
