package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// ProjectionExecutor evaluates a list of expressions on the input tuples
// and produces a new tuple containing the results of those expressions.
type ProjectionExecutor struct {
	projNode      *planner.ProjectionNode
	childExecutor Executor
	buf           []byte
	rid           common.RecordID
}

// NewProjectionExecutor creates a new ProjectionExecutor.
func NewProjectionExecutor(plan *planner.ProjectionNode, child Executor) *ProjectionExecutor {
	total_buf_length := 0
	for _, t := range plan.OutputSchema() {
		total_buf_length += t.Size()
	}
	buf := make([]byte, total_buf_length)

	projExecutor := ProjectionExecutor{
		projNode:      plan,
		childExecutor: child,
		buf:           buf,
	}

	return &projExecutor
}

func (e *ProjectionExecutor) PlanNode() planner.PlanNode {
	return e.projNode
}

func (e *ProjectionExecutor) Init(ctx *ExecutorContext) error {
	err := e.childExecutor.Init(ctx)
	return err
}

func (e *ProjectionExecutor) Next() bool {
	return e.childExecutor.Next()
}

func (e *ProjectionExecutor) Current() storage.Tuple {
	tup := e.childExecutor.Current()
	outputSchema := e.projNode.OutputSchema()
	startPos := 0

	for i, expr := range e.projNode.Expressions {
		endPos := startPos + outputSchema[i].Size()
		expr.Eval(tup).WriteTo(e.buf[startPos:endPos])
		startPos = endPos
	}

	return storage.FromRawTuple(e.buf, storage.NewRawTupleDesc(outputSchema), tup.RID())
}

func (e *ProjectionExecutor) Error() error {
	return e.childExecutor.Error()
}

func (e *ProjectionExecutor) Close() error {
	return e.childExecutor.Close()
}
