package storage

import (
	"math/bits"
	"unsafe"

	"mit.edu/dsg/godb/common"
)

// Bitmap provides a convenient interface for manipulating bits in a byte slice.
// It does not own the underlying bytes; instead, it provides a structured view over
// an existing buffer (e.g., a database page).
//
// The implementation should be optimized for performance by performing word-level (uint64)
// operations during scans to skip full blocks of set bits.
type Bitmap struct {
	words   []uint64
	numBits int
}

// AsBitmap creates a Bitmap view over the provided byte slice.
//
// Constraints:
// 1. data must be aligned to 8 bytes to allow safe casting to uint64.
// 2. data must be large enough to contain numBits (rounded up to the nearest 8-byte word).
func AsBitmap(data []byte, numBits int) Bitmap {
	common.Assert(common.AlignedTo8(len(data)), "Bitmap bytes length must be aligned to 8")

	numWords := (numBits + 63) / 64
	common.Assert(len(data) >= numWords*8, "bitmap buffer too small")

	ptr := unsafe.Pointer(&data[0])
	// Slice reference cast to uint64
	words := unsafe.Slice((*uint64)(ptr), numWords)

	return Bitmap{
		words:   words,
		numBits: numBits,
	}
}

// SetBit sets the bit at index i to the given value.
// Returns the previous value of the bit.
func (b *Bitmap) SetBit(i int, on bool) (originalValue bool) {
	if i < 0 || i >= b.numBits {
		panic("[set] bit index out of bounds")
	}

	wordIdx := i / 64
	wordOffset := i % 64
	oldVal := b.words[wordIdx]

	if on {
		b.words[wordIdx] = oldVal | (uint64(1) << wordOffset)
	} else {
		b.words[wordIdx] = oldVal & (^(uint64(1) << wordOffset))
	}

	return (oldVal & (uint64(1) << wordOffset)) != 0
}

// LoadBit returns the value of the bit at index i.
func (b *Bitmap) LoadBit(i int) bool {
	if i < 0 || i >= b.numBits {
		panic("[load] bit index out of bounds")
	}

	wordIdx := i / 64
	wordOffset := i % 64

	return (b.words[wordIdx] & (uint64(1) << wordOffset)) != 0
}

func checkAndReturnIdx(word uint64, wordIdx int, totalNumWords int, numBits int) int {
	var validMask uint64

	if wordIdx == totalNumWords-1 {
		rem := numBits - (totalNumWords-1)*64 // number of valid bits in last word (1..64)
		if rem == 64 {
			validMask = ^uint64(0)
		} else {
			validMask = (uint64(1) << rem) - 1
		}
	} else {
		validMask = ^uint64(0)
	}

	// Only consider valid bits when checking for zeros.
	zeros := (^word) & validMask
	if zeros == 0 {
		return -1
	}
	return bits.TrailingZeros64(zeros) + wordIdx*64
}

// FindFirstZero searches for the first bit set to 0 (false) in the bitmap.
// It begins the search at startHint and scans to the end of the bitmap.
// If no zero bit is found, it wraps around and scans from the beginning (index 0)
// up to startHint.
//
// Returns the index of the first zero bit found, or -1 if the bitmap is entirely full.
func (b *Bitmap) FindFirstZero(startHint int) int {
	// fmt.Printf("STARTING SOMETHING: %d\n", startHint)
	// fmt.Printf("num bits: %d, numWords: %d\n", b.numBits, len(b.words))
	// Can check each word == 0xffff
	// if not, there is a zero somewhere

	if b.numBits == 0 {
		return -1
	}
	if startHint < 0 || startHint >= b.numBits {
		panic("[findfirstzero] startHint out of bounds")
	}

	startWordIdx := startHint / 64
	startOffset := startHint % 64
	totalNumWords := len(b.words)
	numBits := b.numBits

	firstWordFilled := b.words[startWordIdx] | ((uint64(1) << startOffset) - 1)

	if zeroIdx := checkAndReturnIdx(firstWordFilled, startWordIdx, totalNumWords, numBits); zeroIdx != -1 {
		// fmt.Printf("zeroIdx: %d", zeroIdx)
		return zeroIdx
	}

	for curWordIdx := (startWordIdx + 1) % totalNumWords; curWordIdx != startWordIdx; curWordIdx = (curWordIdx + 1) % totalNumWords {
		if zeroIdx := checkAndReturnIdx(b.words[curWordIdx], curWordIdx, totalNumWords, numBits); zeroIdx != -1 {
			// fmt.Printf("zeroIdx: %d", zeroIdx)
			return zeroIdx
		}
	}

	if zeroIdx := checkAndReturnIdx(b.words[startWordIdx], startWordIdx, totalNumWords, numBits); zeroIdx != -1 {
		// fmt.Printf("zeroIdx: %d", zeroIdx)
		return zeroIdx
	}

	return -1

}
