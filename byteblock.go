package byteblock

import (
	"errors"
	"io"
)

const intSize = 8

type ByteBlockWriter struct {
	writer          io.Writer
	numBytesWritten int64
	err             error
	stub            [intSize]byte
}

func NewByteBlockWriter(w io.Writer) *ByteBlockWriter {
	return &ByteBlockWriter{writer: w}
}

func (w *ByteBlockWriter) Write(data []byte, align int64) error {
	if w.err != nil {
		return w.err
	}
	// Length
	w.fillStub(int64(len(data)))
	if w.err = w.rawWrite(w.stub[:]); w.err != nil {
		return w.err
	}
	// Offset
	offset := int64(alignOffset(align, w.numBytesWritten+intSize))
	w.fillStub(offset)
	if w.err = w.rawWrite(w.stub[:]); w.err != nil {
		return w.err
	}
	// Padding
	if w.err = w.rawWrite(make([]byte, offset)); w.err != nil {
		return w.err
	}
	// Data
	if w.err = w.rawWrite(data); w.err != nil {
		return w.err
	}
	return nil
}

func (w *ByteBlockWriter) fillStub(n int64) {
	fillInt64(n, w.stub[:])
}

func (w *ByteBlockWriter) rawWrite(data []byte) error {
	n, err := w.writer.Write(data)
	w.numBytesWritten += int64(n)
	return err
}

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

type ByteBlockSlicer struct {
	data           []byte
	numBytesSliced int64
	err            error
}

func NewByteBlockSlicer(data []byte) *ByteBlockSlicer {
	return &ByteBlockSlicer{data: data}
}

func (r *ByteBlockSlicer) Slice() (data []byte, err error) {
	if r.err != nil {
		return nil, r.err
	}
	if r.numBytesSliced >= int64(len(r.data)) {
		return nil, io.EOF
	}
	var b []byte
	// Length
	b, r.err = r.rawSlice(intSize)
	if r.err != nil {
		return nil, r.err
	}
	length := readInt64(b)
	// Offset
	b, r.err = r.rawSlice(intSize)
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
	for i := 0; i < intSize; i++ {
		out[i] = byte(n & 0xFF)
		n >>= 8
	}
}

func readInt64(data []byte) (n int64) {
	for i := 0; i < intSize; i++ {
		n |= int64(data[i]) << uint(8*i)
	}
	return n
}
