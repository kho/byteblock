package byteblock

import (
	"errors"
	"io"
	"reflect"
	"unsafe"
)

type ByteBlockWriter struct {
	writer          io.Writer
	numBytesWritten int64
	numBytesLeft    int64
	err             error
	stub            [8]byte
}

func NewByteBlockWriter(w io.Writer) *ByteBlockWriter {
	return &ByteBlockWriter{writer: w}
}

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

func (w *ByteBlockWriter) Write(data []byte, align int64) error {
	if w.err = w.NewBlock(align, int64(len(data))); w.err != nil {
		return w.err
	}
	if w.err = w.Append(data); w.err != nil {
		return w.err
	}
	return nil
}

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

func (w *ByteBlockWriter) rawWrite(data []byte) error {
	n, err := w.writer.Write(data)
	w.numBytesWritten += int64(n)
	w.numBytesLeft -= int64(n)
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

var (
	ErrNewBlockBeforeFinish   = errors.New("creating new block before finishing the previous one")
	ErrWriteMoreThanRequested = errors.New("writing more bytes than requested")
)

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
