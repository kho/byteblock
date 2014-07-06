// Package byteblock provides tools to write and read (typically big)
// blocks of bytes. It stores and interprets data in the following
// format:
//
// 1. The data is stored in blocks.
//
// 2. Each block starts with a header of an int64 pair (length,
// offset), where length is the number of bytes of the actual data
// block and offset is the amount of padding after header and before
// the data block.
package byteblock

import (
	"errors"
	"io"
	"reflect"
	"unsafe"
)

// ByteBlockWriter writes blocks to a writer specified in
// NewByteBlockWriter. It keeps track of the number of bytes written
// since construction to allow new blocks to be aligned at any number
// of bytes.
type ByteBlockWriter struct {
	writer          io.Writer
	numBytesWritten int64
	numBytesLeft    int64
	err             error
	stub            [8]byte
}

// NewByteBlockWriter creates a ByteBlockWriter that writes to the
// specified writer. Only one ByteBlockWriter should be created for a
// given writer to prevent conflicts in writing.
func NewByteBlockWriter(w io.Writer) *ByteBlockWriter {
	return &ByteBlockWriter{writer: w}
}

// NewBlock asks the writer to create a new block with given alignment
// and length. Non-positive alignments are interpreted as 1-byte
// aligned. A previous block, if exists, must already have been
// finished; otherwise ErrNewBlockBeforeFinish is returned. Other
// errors from previous operations or the underlying writer are also
// returned.
func (w *ByteBlockWriter) NewBlock(align int64, length int64) error {
	if w.err != nil {
		return w.err
	}
	if w.numBytesLeft > 0 {
		w.err = ErrNewBlockBeforeFinish
		return w.err
	}
	// Length
	w.fillStub(int64(length))
	if w.err = w.rawWrite(w.stub[:]); w.err != nil {
		return w.err
	}
	// Offset
	offset := int64(alignOffset(align, w.numBytesWritten+8))
	w.fillStub(offset)
	if w.err = w.rawWrite(w.stub[:]); w.err != nil {
		return w.err
	}
	// Padding
	if w.err = w.rawWrite(make([]byte, offset)); w.err != nil {
		return w.err
	}
	w.numBytesLeft = length
	return nil
}

// Append appends a chunk of data to the current block. The length of
// data must not exceed the number of bytes left for the current
// block.
func (w *ByteBlockWriter) Append(data []byte) error {
	if w.err != nil {
		return w.err
	}
	length := int64(len(data))
	if length > w.numBytesLeft {
		w.err = ErrWriteMoreThanRequested
		return w.err
	}
	if w.err = w.rawWrite(data); w.err != nil {
		return w.err
	}
	return nil
}

// AppendString is like Append() except that it takes a string.
func (w *ByteBlockWriter) AppendString(data string) error {
	// Because Append() does not modify data, we can temporary fake a
	// byte slice out of data.
	var dataBytes []byte
	stringHeader := (*reflect.StringHeader)(unsafe.Pointer(&data))
	bytesHeader := (*reflect.SliceHeader)(unsafe.Pointer(&dataBytes))
	bytesHeader.Data = stringHeader.Data
	bytesHeader.Len = stringHeader.Len
	bytesHeader.Cap = stringHeader.Len
	return w.Append(dataBytes)
}

// Write is a convenience method that creates a block out of the given
// data.
func (w *ByteBlockWriter) Write(data []byte, align int64) error {
	if w.err = w.NewBlock(align, int64(len(data))); w.err != nil {
		return w.err
	}
	if w.err = w.Append(data); w.err != nil {
		return w.err
	}
	return nil
}

// WriteString is like Write() except that it takes a string.
func (w *ByteBlockWriter) WriteString(data string, align int64) error {
	if w.err = w.NewBlock(align, int64(len(data))); w.err != nil {
		return w.err
	}
	if w.err = w.AppendString(data); w.err != nil {
		return w.err
	}
	return nil
}

func (w *ByteBlockWriter) fillStub(n int64) {
	fillInt64(n, w.stub[:])
}

// rawWrite writes the given data to the underlying writer and updates
// numBytesWritten and numBytesLeft. However it does not check whether
// its updates are valid (especially for numBytesLeft), which is its
// caller's responsibility.
func (w *ByteBlockWriter) rawWrite(data []byte) error {
	n, err := w.writer.Write(data)
	w.numBytesWritten += int64(n)
	w.numBytesLeft -= int64(n)
	return err
}

// alignOffset computes the amount of padding needed to start at a
// position that is a multiple of align from pos.
func alignOffset(align, pos int64) int64 {
	if align <= 1 {
		return 0
	}
	offset := pos % align
	if offset == 0 {
		return 0
	}
	return align - offset
}

var (
	ErrNewBlockBeforeFinish   = errors.New("creating new block before finishing the previous one")
	ErrWriteMoreThanRequested = errors.New("writing more bytes than requested")
)

// ByteBlockSlicer slices a byte slice specified at construction into
// data blocks. The byte slice is usually created by a
// ByteBlockWriter.
type ByteBlockSlicer struct {
	data           []byte
	numBytesSliced int64
	err            error
}

// NewByteBlockSlicer creates a new slicer with the given backing data
// slice.
func NewByteBlockSlicer(data []byte) *ByteBlockSlicer {
	return &ByteBlockSlicer{data: data}
}

// Slice returns the next data block, sliced out of the backing data
// slice.
func (r *ByteBlockSlicer) Slice() (data []byte, err error) {
	if r.err != nil {
		return nil, r.err
	}
	if r.numBytesSliced >= int64(len(r.data)) {
		return nil, io.EOF
	}
	var b []byte
	// Length
	b, r.err = r.rawSlice(8)
	if r.err != nil {
		return nil, r.err
	}
	length := readInt64(b)
	// Offset
	b, r.err = r.rawSlice(8)
	if r.err != nil {
		return nil, r.err
	}
	offset := readInt64(b)
	// Padding
	if _, r.err = r.rawSlice(offset); r.err != nil {
		return nil, r.err
	}
	// Data
	return r.rawSlice(length)
}

var ErrNotEnoughBytes = errors.New("not enough bytes")

func (r *ByteBlockSlicer) rawSlice(n int64) ([]byte, error) {
	if r.numBytesSliced+n > int64(len(r.data)) {
		return nil, ErrNotEnoughBytes
	}
	data := r.data[r.numBytesSliced : r.numBytesSliced+n]
	r.numBytesSliced += n
	return data, nil
}

func fillInt64(n int64, out []byte) {
	for i := 0; i < 8; i++ {
		out[i] = byte(n)
		n >>= 8
	}
}

func readInt64(data []byte) (n int64) {
	for i := 0; i < 8; i++ {
		n |= int64(data[i]) << uint(8*i)
	}
	return n
}
