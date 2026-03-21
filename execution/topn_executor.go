package execution

import (
	"container/heap"
	"sort"

	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// TopNExecutor optimizes "ORDER BY ... LIMIT N" queries.
//
// This should allow an optimized implementation that avoids sorting ALL tuples (O(M log M)).

type TupleHeap struct {
	items    []storage.Tuple
	orderBys []planner.OrderByClause
}

func (h *TupleHeap) Len() int {
	return len(h.items)
}

func (h *TupleHeap) Less(i, j int) bool {
	leftTuple := h.items[i]
	rightTuple := h.items[j]

	for _, orderBy := range h.orderBys {
		leftVal := orderBy.Expr.Eval(leftTuple)
		rightVal := orderBy.Expr.Eval(rightTuple)

		comparison := leftVal.Compare(rightVal)
		if comparison == 0 {
			continue
		}

		if orderBy.Direction == planner.SortOrderDescending {
			return comparison < 0
		}
		return comparison > 0
	}

	return false
}

func (h *TupleHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *TupleHeap) Push(x interface{}) {
	h.items = append(h.items, x.(storage.Tuple))
}

func (h *TupleHeap) Pop() interface{} {
	old := h.items
	n := len(old)
	tuple := old[n-1]
	h.items = old[:n-1]
	return tuple
}

type TopNExecutor struct {
	planNode      *planner.TopNNode
	childExecutor Executor
	heap          *TupleHeap
	numOutput     int
}

func NewTopNExecutor(plan *planner.TopNNode, child Executor) *TopNExecutor {
	topNExecutor := TopNExecutor{
		planNode:      plan,
		childExecutor: child,
		heap: &TupleHeap{
			items:    make([]storage.Tuple, 0),
			orderBys: plan.OrderBy,
		},
	}
	return &topNExecutor
}

func (e *TopNExecutor) PlanNode() planner.PlanNode {
	return e.planNode
}

func (e *TopNExecutor) Init(ctx *ExecutorContext) error {
	err := e.childExecutor.Init(ctx)
	if err != nil {
		return err
	}
	heap.Init(e.heap)

	for exists := e.childExecutor.Next(); exists; exists = e.childExecutor.Next() {
		curTup := e.childExecutor.Current()
		heap.Push(e.heap, curTup.DeepCopy(storage.NewRawTupleDesc(e.childExecutor.PlanNode().OutputSchema())))
		if e.heap.Len() > e.planNode.Limit {
			heap.Pop(e.heap)
		}
	}

	sort.Slice(e.heap.items, func(i, j int) bool {
		a, b := e.heap.items[i], e.heap.items[j]
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

	e.numOutput = 0

	return nil
}

func (e *TopNExecutor) Next() bool {
	if e.numOutput < e.planNode.Limit && e.numOutput < len(e.heap.items) {
		e.numOutput++
		return true
	}
	return false
}

func (e *TopNExecutor) Current() storage.Tuple {
	return e.heap.items[e.numOutput-1]
}

func (e *TopNExecutor) Error() error {
	return e.childExecutor.Error()
}

func (e *TopNExecutor) Close() error {
	return e.childExecutor.Close()
}
