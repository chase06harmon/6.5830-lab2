package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// IndexLookupExecutor implements a Point Lookup using an index. Unlike a full Index Scan, which iterates over a
// range of keys, this executor efficiently retrieves only the tuples that match a specific equality key
// (e.g., "SELECT * FROM users WHERE id = 5").
type IndexLookupExecutor struct {
	planNode      *planner.IndexLookupNode
	index         indexing.Index
	tableHeap     *TableHeap
	context       *ExecutorContext
	buf           []byte
	recheckKeyBuf []byte
	rids          []common.RecordID
	currentRIDIdx int
	curRID        common.RecordID
	err           error
}

func NewIndexLookupExecutor(plan *planner.IndexLookupNode, index indexing.Index, tableHeap *TableHeap) *IndexLookupExecutor {
	buffer := make([]byte, tableHeap.desc.BytesPerTuple())
	recheckKeyBuf := make([]byte, index.Metadata().KeySize())
	rids := make([]common.RecordID, 0)

	indexLookupExecutor := IndexLookupExecutor{
		planNode:      plan,
		index:         index,
		tableHeap:     tableHeap,
		context:       nil,
		buf:           buffer,
		recheckKeyBuf: recheckKeyBuf,
		rids:          rids,
		currentRIDIdx: -1,
		curRID:        common.RecordID{},
		err:           nil,
	}

	return &indexLookupExecutor
}

func (e *IndexLookupExecutor) PlanNode() planner.PlanNode {
	return e.planNode
}

func (e *IndexLookupExecutor) Init(ctx *ExecutorContext) error {
	e.context = ctx
	e.err = nil
	e.rids = e.rids[:0]
	e.currentRIDIdx = -1
	e.curRID = common.RecordID{}
	rids, err := e.index.ScanKey(e.planNode.EqualityKey, e.rids, ctx.GetTransaction())

	if err != nil {
		return err
	}

	e.rids = rids
	return nil
}

func (e *IndexLookupExecutor) Next() bool {
	if e.err != nil {
		return false
	}

	for e.currentRIDIdx+1 < len(e.rids) {
		e.currentRIDIdx++
		rid := e.rids[e.currentRIDIdx]
		if err := e.tableHeap.ReadTuple(e.context.GetTransaction(), rid, e.buf, e.planNode.ForUpdate); err != nil {
			if err == ErrTupleDeleted {
				continue
			}
			e.err = err
			return false
		}

		if e.planNode.EqualityKey.Compare(
			makeProjectedKeyFromTuple(e.buf, e.tableHeap.desc, e.index.Metadata(), e.recheckKeyBuf),
		) != 0 {
			continue
		}

		e.curRID = rid
		return true
	}

	return false
}

func (e *IndexLookupExecutor) Current() storage.Tuple {
	return storage.FromRawTuple(e.buf, e.tableHeap.desc, e.curRID)
}

func (e *IndexLookupExecutor) Close() error {
	return nil
}

func (e *IndexLookupExecutor) Error() error {
	return e.err
}

func makeProjectedKeyFromTuple(
	tuple storage.RawTuple,
	tupleDesc *storage.RawTupleDesc,
	md *indexing.IndexMetadata,
	keyBuf []byte,
) indexing.Key {
	for keyIdx, tupleColIdx := range md.ProjectionList {
		md.KeySchema.SetValue(keyBuf, keyIdx, tupleDesc.GetValue(tuple, tupleColIdx))
	}
	return md.AsKey(keyBuf)
}
