package cache

import "testing"

func TestMMAlloc(t *testing.T) {
	var mm *memManager = new(memManager)
	mm.init()
	for i := 0; i < 10; i++ {
		index, ok := mm.allocBlock()
		if !ok {
			t.Error("")
		}
		if int(index) != i+1 {
			t.Error(index)
		}
		if len(mm.pool) != i+2 {
			t.Error(len(mm.pool))
		}
	}
	for i := 0; i < 10; i++ {
		mm.recycleBlock(uint16(i + 1))
	}
	for i := 10; i > 0; i-- {
		index, ok := mm.allocBlock()
		if !ok {
			t.Error("")
		}
		if int(index) != i {
			t.Error("")
		}
		if len(mm.pool) != 11 {
			t.Error("")
		}
	}
	for i := 11; i < 65535; i++ {
		index, ok := mm.allocBlock()
		if !ok {
			t.Error("")
		}
		if int(index) != i {
			t.Error("")
		}
		if len(mm.pool) != i+1 {
			t.Error("")
		}
	}
	_, ok := mm.allocBlock()
	if ok {
		t.Error("")
	}
}

func TestMMAppend(t *testing.T) {
	var mm *memManager = new(memManager)
	mm.init()
	index, offset, ok := mm.append(make([]byte, 1024))
	if !ok {
		t.Error("")
	}
	if index != 0 {
		t.Error("")
	}
	if offset != 0 {
		t.Error("")
	}

	index, offset, ok = mm.append(make([]byte, 1024))
	if !ok {
		t.Error("")
	}
	if index != 0 {
		t.Error("")
	}
	if offset != 1024 {
		t.Error(offset)
	}

	index, offset, ok = mm.append(make([]byte, 4*1024*1024))
	if !ok {
		t.Error("")
	}
	if index != 1 {
		t.Error("")
	}
	if offset != 0 {
		t.Error(offset)
	}

	mm.remove(0, 2048)

	index, offset, ok = mm.append(make([]byte, 1024))
	if !ok {
		t.Error("")
	}
	if index != 0 {
		t.Error("")
	}
	if offset != 0 {
		t.Error("")
	}

	index, offset, ok = mm.append(make([]byte, 1024))
	if !ok {
		t.Error("")
	}
	if index != 0 {
		t.Error("")
	}
	if offset != 1024 {
		t.Error(offset)
	}

}

func TestMMReadWrite(t *testing.T) {
	var mm *memManager = new(memManager)
	mm.init()
	_, _, _ = mm.append([]byte("hello "))
	_, _, _ = mm.append([]byte("world."))
	_, _, _ = mm.append([]byte("nihao "))
	_, _, _ = mm.append([]byte("shijie."))
	data := mm.locate(0, 0)
	if string(data[:12]) != "hello world." {
		t.Error(string(data[:12]))
	}
	data = mm.locate(0, 12)
	if string(data[:13]) != "nihao shijie." {
		t.Error(data[:13])
	}
	mm.remove(0, 25)
	_, _, _ = mm.append([]byte("nihao "))
	data = mm.locate(0, 0)
	if string(data[:12]) != "nihao world." {
		t.Error(string(data[:12]))
	}
}

func BenchmarkAppend(b *testing.B) {
	var mm *memManager = new(memManager)
	mm.init()
	for i := 0; i < b.N; i++ {
		_, _, ok := mm.append(make([]byte, 1024, 1024))
		if !ok {
			b.Error("")
		}
	}
}

func BenchmarkRecycle(b *testing.B) {
	var mm *memManager = new(memManager)
	mm.init()
	indexList := make([]uint16, b.N, b.N)
	var ok bool
	for i := 0; i < b.N; i++ {
		indexList[i], _, ok = mm.append(make([]byte, 1024, 1024))
		if !ok {
			b.Error("")
		}
	}
	for i := 0; i < b.N; i++ {
		mm.remove(indexList[i], 1024)
		if !ok {
			b.Error("")
		}
	}
}
