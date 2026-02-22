package storage

import (
	"fmt"

	"mit.edu/dsg/godb/common"
)

const LSN_IDX = 0
const ROW_SIZE_IDX = 8
const NUM_SLOTS_IDX = 10
const NUM_USED_IDX = 12
const START_HINT_IDX = 14
const ALLOCATION_BITMAP_IDX = 16
const REMAINING_BYTES = 4096 - ALLOCATION_BITMAP_IDX

// HeapPage Layout:
// LSN (8) | RowSize (2) | NumSlots (2) |  NumUsed (2) | StartHint (2) | allocation Bitmap | deleted Bitmap | rows
type HeapPage struct {
	frame                  *PageFrame
	deletedBitmapStartByte int
	rowsStartByte          int
}

func getU16LE(b []byte, i int) int {
	return int(b[i]) | int(b[i+1])<<8
}

func setU16LE(b []byte, i int, v int) {
	b[i] = byte(v)
	b[i+1] = byte(v >> 8)
}

func (frame *PageFrame) IsHeapPage() bool {
	rowSize := getU16LE(frame.Bytes[:], ROW_SIZE_IDX)
	numSlots := getU16LE(frame.Bytes[:], NUM_SLOTS_IDX)
	return rowSize > 0 && numSlots == getNumSlots(rowSize)
}

func (hp HeapPage) NumUsed() int {
	return int(getU16LE(hp.frame.Bytes[:], NUM_USED_IDX))
}

func (hp HeapPage) setNumUsed(numUsed int) {
	setU16LE(hp.frame.Bytes[:], NUM_USED_IDX, numUsed)
}

func (hp HeapPage) StartHint() int {
	return int(getU16LE(hp.frame.Bytes[:], START_HINT_IDX))
}

func (hp HeapPage) setStartHint(startHint int) {
	setU16LE(hp.frame.Bytes[:], START_HINT_IDX, startHint)
}

func (hp HeapPage) NumSlots() int {
	return int(getU16LE(hp.frame.Bytes[:], NUM_SLOTS_IDX))
}

func (hp HeapPage) RowSize() int {
	return int(getU16LE(hp.frame.Bytes[:], ROW_SIZE_IDX))
}

func getNumSlots(bytesPerRow int) int {
	n := 32512 / (2 + (8 * bytesPerRow))

	isValid := func(numRows int) bool {
		bytesPerBitmap := (((numRows + 63) / 64) * 8)
		totalBytesUsed := (2 * bytesPerBitmap) + (numRows * bytesPerRow)
		// fmt.Printf("TOTAL BYTES USED: %d\n", totalBytesUsed)

		return totalBytesUsed <= 4080
	}

	if !isValid(n) {
		panic("estimate was not conservative enough... should be impossible")
	}

	for isValid(n + 1) {
		n++
	}

	return n
}

func InitializeHeapPage(desc *RawTupleDesc, frame *PageFrame) {
	for i := range frame.Bytes {
		frame.Bytes[i] = 0
	}

	setU16LE(frame.Bytes[:], ROW_SIZE_IDX, desc.bytesPerRow)

	numSlots := getNumSlots(desc.bytesPerRow)
	setU16LE(frame.Bytes[:], NUM_SLOTS_IDX, numSlots)

	setU16LE(frame.Bytes[:], NUM_USED_IDX, 0)

	setU16LE(frame.Bytes[:], START_HINT_IDX, 0)
}

func (frame *PageFrame) AsHeapPage() HeapPage {
	numRows := getU16LE(frame.Bytes[:], NUM_SLOTS_IDX)

	bytesPerBitmap := (((numRows + 63) / 64) * 8)

	deletedBitmapStartByte := ALLOCATION_BITMAP_IDX + bytesPerBitmap
	rowsStartByte := deletedBitmapStartByte + bytesPerBitmap

	return HeapPage{
		frame:                  frame,
		deletedBitmapStartByte: deletedBitmapStartByte,
		rowsStartByte:          rowsStartByte,
	}
}

func (hp HeapPage) FindFreeSlot() int {
	startHint := hp.StartHint()
	numSlots := hp.NumSlots()
	allocatedBitmap := AsBitmap(hp.frame.Bytes[ALLOCATION_BITMAP_IDX:], numSlots)
	slot := allocatedBitmap.FindFirstZero(startHint) // note, can optimize for start hint

	if slot >= 0 {
		hp.setStartHint((slot + 1) % numSlots)
	}

	return slot

}

// IsAllocated checks the allocation bitmap to see if a slot is valid.
func (hp HeapPage) IsAllocated(rid common.RecordID) bool {
	numSlots := hp.NumSlots()
	allocatedBitmap := AsBitmap(hp.frame.Bytes[ALLOCATION_BITMAP_IDX:], numSlots)
	return allocatedBitmap.LoadBit(int(rid.Slot))
}

func (hp HeapPage) MarkAllocated(rid common.RecordID, allocated bool) {
	numSlots := hp.NumSlots()
	allocatedBitmap := AsBitmap(hp.frame.Bytes[ALLOCATION_BITMAP_IDX:], numSlots)
	deletedBitMap := AsBitmap(hp.frame.Bytes[hp.deletedBitmapStartByte:], numSlots)

	if allocated {
		allocatedSlot := int(rid.Slot)
		allocatedBitmap.SetBit(allocatedSlot, true)
		deletedBitMap.SetBit(allocatedSlot, false)
		hp.setNumUsed(hp.NumUsed() + 1)
	} else {
		freedSlot := int(rid.Slot)
		allocatedBitmap.SetBit(freedSlot, false)
		deletedBitMap.SetBit(freedSlot, false)
		hp.setNumUsed(hp.NumUsed() - 1)

		if freedSlot < hp.StartHint() {
			hp.setStartHint(freedSlot)
		}

	}
}

func (hp HeapPage) IsDeleted(rid common.RecordID) bool {
	numSlots := hp.NumSlots()
	if rid.Slot > int32(numSlots) {
		fmt.Println("slot: ", rid.Slot, " numSlots: ", numSlots)
	}
	deletedBitMap := AsBitmap(hp.frame.Bytes[hp.deletedBitmapStartByte:], numSlots)
	return deletedBitMap.LoadBit(int(rid.Slot))

}

func (hp HeapPage) MarkDeleted(rid common.RecordID, deleted bool) {
	numSlots := hp.NumSlots()
	deletedBitMap := AsBitmap(hp.frame.Bytes[hp.deletedBitmapStartByte:], numSlots)
	deletedBitMap.SetBit(int(rid.Slot), deleted)
}

func (hp HeapPage) AccessTuple(rid common.RecordID) RawTuple {
	rowSz := hp.RowSize()
	startIdx := hp.rowsStartByte + (int(rid.Slot) * rowSz)
	return hp.frame.Bytes[startIdx : startIdx+rowSz]
}
