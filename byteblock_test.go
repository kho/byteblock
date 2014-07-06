package byteblock

import (
	"bytes"
	"reflect"
	"testing"
)

func TestFillInt64(t *testing.T) {
	out := make([]byte, 8)
	for _, i := range []struct {
		N int64
		B []byte
	}{
		{0, []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		{1, []byte{1, 0, 0, 0, 0, 0, 0, 0}},
		{0xF0000000000000F, []byte{0xF, 0, 0, 0, 0, 0, 0, 0xF}},
	} {
		fillInt64(i.N, out)
		if !reflect.DeepEqual(out, i.B) {
			t.Errorf("case %+v: fill got %v", i, out)
		}
		if n := readInt64(i.B); n != i.N {
			t.Errorf("case %+v: read got %d", i, n)
		}
	}
}

func TestAlignOffset(t *testing.T) {
	for _, i := range []struct {
		Align, Pos, Offset int64
	}{
		{0, 0, 0}, {0, 10, 0}, {1, 0, 0}, {-1, 10, 0},
		{4, 0, 0}, {4, 1, 3}, {4, 2, 2}, {4, 3, 1},
		{4, 4, 0}, {4, 5, 3}, {4, 6, 2}, {4, 7, 1},
		{5, 0, 0}, {5, 1, 4}, {5, 2, 3}, {5, 3, 2}, {5, 4, 1},
		{5, 5, 0}, {5, 6, 4}, {5, 7, 3}, {5, 8, 2}, {5, 9, 1},
	} {
		offset := alignOffset(i.Align, i.Pos)
		if offset != i.Offset {
			t.Errorf("case %+v: got %v", i, offset)
		}
	}
}

func TestWriteAndSlice(t *testing.T) {
	var buf bytes.Buffer
	writer := NewByteBlockWriter(&buf)
	data := []struct {
		Data  []byte
		Align int64
	}{
		{[]byte("hello"), 0},
		{[]byte("world"), 4},
		{[]byte("hello"), 8},
		{[]byte("world"), 16},
		{[]byte("hello"), 31},
		{[]byte("world"), 127},
	}
	for i, d := range data {
		if i%2 == 0 {
			if err := writer.Write(d.Data, d.Align); err != nil {
				t.Errorf("record %+v: unexpected error: %v", i, err)
			}
		} else {
			if err := writer.WriteString(string(d.Data), d.Align); err != nil {
				t.Errorf("record %+v: unexpected error: %v", i, err)
			}
		}
		start := int64(buf.Len() - len(d.Data))
		if d.Align > 1 && start%d.Align != 0 {
			t.Errorf("misaligned write starting at %d", start)
		}
	}
	slicer := NewByteBlockSlicer(buf.Bytes())
	for _, i := range data {
		slice, err := slicer.Slice()
		if err != nil {
			t.Errorf("record %+v: unexpected error: %v", i, err)
		}
		if !reflect.DeepEqual(slice, i.Data) {
			t.Errorf("record %+v: got %v", i, slice)
		}
	}
}

func TestNewBlockAndAppend(t *testing.T) {
	runWriter := func(f func(*ByteBlockWriter)) error {
		var buf bytes.Buffer
		writer := NewByteBlockWriter(&buf)
		f(writer)
		return writer.err
	}

	if err := runWriter(func(w *ByteBlockWriter) {
		w.NewBlock(127, 1)
		w.NewBlock(0, 1)
	}); err != ErrNewBlockBeforeFinish {
		t.Errorf("expected ErrNewBlockBeforeFinish; got %v", err)
	}

	if err := runWriter(func(w *ByteBlockWriter) {
		w.NewBlock(0, 2)
		w.Append([]byte{0})
		w.Append([]byte{1, 2})
	}); err != ErrWriteMoreThanRequested {
		t.Errorf("expcted ErrWriteMoreThanRequested; got %v", err)
	}

	if err := runWriter(func(w *ByteBlockWriter) {
		w.NewBlock(0, 2)
		w.AppendString("x")
		w.AppendString("xx")
	}); err != ErrWriteMoreThanRequested {
		t.Errorf("expcted ErrWriteMoreThanRequested; got %v", err)
	}
}

func TestNotEnoughBytes(t *testing.T) {
	var buf bytes.Buffer
	NewByteBlockWriter(&buf).Write([]byte("hello"), 7)
	numBytes := buf.Len()
	for i := 0; i < numBytes; i++ {
		data := buf.Bytes()[:i]
		slicer := NewByteBlockSlicer(data)
		if _, err := slicer.Slice(); err == nil {
			t.Errorf("expected error from slicing %v", data)
		}

		if _, err := slicer.Slice(); err == nil {
			t.Errorf("expected error from slicing %v (in error state)", data)
		}
	}
}
