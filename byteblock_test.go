package byteblock

import (
	"bytes"
	"reflect"
	"testing"
)

func TestFillInt64(t *testing.T) {
	out := make([]byte, intSize)
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
			t.Errorf("case %+v: got %v", i, out)
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
	}
	size := int64(0)
	for _, i := range data {
		size += intSize + intSize
		size += alignOffset(i.Align, size) + int64(len(i.Data))
		if err := writer.Write(i.Data, i.Align); err != nil {
			t.Errorf("record %+v: unexpected error: %v", i, err)
		}
		if size != int64(buf.Len()) {
			t.Errorf("record %+v: should have written %d bytes so far; written %d bytes: %v",
				i, size, buf.Len(), buf)
		}
	}
	reader := NewByteBlockSlicer(buf.Bytes())
	for _, i := range data {
		read, err := reader.Slice()
		if err != nil {
			t.Errorf("record %+v: unexpected error: %v", i, err)
		}
		if !reflect.DeepEqual(read, i.Data) {
			t.Errorf("record %+v: got %v", i, read)
		}
	}
}
