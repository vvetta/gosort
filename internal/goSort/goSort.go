package goSort

import (
	"container/heap"
	"bufio"
	"bytes"
	"io"
	"os"
	"sort"
	"strconv"

	"github.com/vvetta/gosort/internal/domain"
)

const (
	ChunkLimit = 1 << 20
)

type Sorter struct {
	flags domain.SortFlags
}

type Line struct {
	line []byte
	KeyStr string
	KeyNum float64
	HasNum bool
}

func NewSorter(flags domain.SortFlags) *Sorter {
	return &Sorter{flags: flags}
}

func (s *Sorter) Sort() error {

	var in io.Reader = os.Stdin
	if s.flags.Filename != "" {
		f, err := os.Open(s.flags.Filename)
		if err != nil {
			return domain.ErrFileNotFound
		}
		defer f.Close()
		in = f
	}

	reader := bufio.NewReaderSize(in, 1<<20)
	var chunks []*Line
	var chunksBytes int64
	var tempFiles []string

	_ = os.MkdirAll("temp", 0o755)

	flushChunk := func () error {
		if len(chunks) == 0 {return nil}

		sort.Slice(chunks, func(i, j int) bool {
			return s.less(chunks[i], chunks[j])
		})

		if s.flags.U && len(chunks) > 1 {
			dst := 1
			for i := 1; i < len(chunks); i++ {
				if !s.equalKey(chunks[i], chunks[dst - 1]) {
					chunks[dst] = chunks[i]
					dst++
				}
			}
			chunks = chunks[:dst]
		}

		tmpf, err := os.CreateTemp("temp", "gosort-*.chunk")
		if err != nil {
			return err
		}
		defer tmpf.Close()

		writer := bufio.NewWriter(tmpf)
		for i := 0; i < len(chunks); i++ {
			if _, err := writer.Write(chunks[i].line); err != nil {	
				return err
			}

			if err := writer.WriteByte('\n'); err != nil {
				return err
			}
		}

		if err := writer.Flush(); err != nil {
			return err
		}

		name := tmpf.Name()
		tempFiles = append(tempFiles, name)

		chunks = chunks[:0]
		chunksBytes = 0

		return nil
	}
	
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			l := line
			if l[len(l) - 1] == '\n' {
				l = l[:len(l) - 1]
			}

			cp := make([]byte, len(l))
			copy(cp, l)
		
			keyStr, keyNum, hasNum := s.extractKey(cp)
			record := &Line{line: cp, KeyStr: keyStr, KeyNum: keyNum, HasNum: hasNum}	

			chunks = append(chunks, record)
			chunksBytes += int64(len(cp)) + 32
			if chunksBytes >= ChunkLimit {
				if err2 := flushChunk(); err2 != nil {
					return err2
				}
			}

		}
		
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}
	}

	if err := flushChunk(); err != nil {
		return err
	}

	switch len(tempFiles) {
	case 0:
		return nil
	case 1:
		if err := s.catFile(tempFiles[0], os.Stdout); err != nil {
			return err
		}
	default:
		if err := s.mergeChunks(tempFiles, os.Stdout); err != nil {
			return err
		}
	}

	for _, p := range tempFiles {
		_ = os.Remove(p)
	}

	return nil
}

func (s *Sorter) catFile(filename string, out io.Writer) error {
	f, err := os.Open(filename) 
	if err != nil {
		return err
	}

	defer f.Close()
	_, err = io.Copy(out, f)	
	return err
}

func (s *Sorter) less(a, b *Line) bool {
	if s.flags.N {
		if a.HasNum != b.HasNum {
			if a.HasNum && !b.HasNum {
				return !s.flags.R
			}
			return s.flags.R
		}
		if a.HasNum && b.HasNum {
			if a.KeyNum != b.KeyNum {
				if s.flags.R {
					return a.KeyNum > b.KeyNum
				}
				return a.KeyNum < b.KeyNum
			}	
		} else {
			if a.KeyStr != b.KeyStr {
				if s.flags.R {
					return a.KeyStr > b.KeyStr
				}
				return a.KeyStr < b.KeyStr
			}
		}
	} else {
		if a.KeyStr != b.KeyStr {
			if s.flags.R {
				return a.KeyStr > b.KeyStr
			}
			return a.KeyStr < b.KeyStr
		}
	}

	if s.flags.R {
		return string(a.line) > string(b.line)
	}
	return string(a.line) < string(b.line)
}

func (s *Sorter) equalKey(a, b *Line) bool {
	if s.flags.N {
		if a.HasNum && b.HasNum {
			return a.KeyNum == b.KeyNum
		}
		if !a.HasNum && !b.HasNum {
			return a.KeyStr == b.KeyStr
		}
		return false
	}
	return a.KeyStr == b.KeyStr
}

func (s *Sorter) extractKey(line []byte) (keyStr string, keyNum float64, hasNum bool) {
	var key []byte
	if s.flags.K > 0 {
		fields := bytes.Fields(line)
		idx := s.flags.K - 1
		if idx >= 0 && idx < len(fields) {
			key = fields[idx]
		} else {
			key = nil
		}
	} else {
		key = line
	}

	keyStr = string(key)
	if s.flags.N {
		if len(key) == 0 {
			return keyStr, 0, false
		} 

		if num, err := strconv.ParseFloat(keyStr, 64); err == nil {
			return keyStr, num, true
		}

		return keyStr, 0, false
	}

	return keyStr, 0, false
}

type mergeItem struct {
	rec    *Line
	reader *bufio.Reader
	file   *os.File
	idx    int // стабильность при равенстве ключей
}

type minHeap struct {
	arr   []*mergeItem
	lessF func(a, b *Line) bool
}

func (h minHeap) Len() int { return len(h.arr) }
func (h minHeap) Less(i, j int) bool {
	a := h.arr[i].rec
	b := h.arr[j].rec
	if h.lessF(a, b) {
		return true
	}
	if h.lessF(b, a) {
		return false
	}
	return h.arr[i].idx < h.arr[j].idx
}
func (h minHeap) Swap(i, j int) { h.arr[i], h.arr[j] = h.arr[j], h.arr[i] }
func (h *minHeap) Push(x any)   { h.arr = append(h.arr, x.(*mergeItem)) }
func (h *minHeap) Pop() any {
	n := len(h.arr)
	x := h.arr[n-1]
	h.arr = h.arr[:n-1]
	return x
}

func (s *Sorter) mergeChunks(paths []string, out io.Writer) error {
	type src struct {
		f  *os.File
		br *bufio.Reader
	}
	sources := make([]src, 0, len(paths))
	for _, p := range paths {
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		sources = append(sources, src{f: f, br: bufio.NewReaderSize(f, 1 << 20)})
	}

	h := &minHeap{lessF: func(a, b *Line) bool { return s.less(a, b) }}
	heap.Init(h)

	// инициализация кучи первой строкой из каждого файла
	for i, src := range sources {
		line, err := readLine(src.br)
		if err != nil && err != io.EOF {
			return err
		}
		if len(line) == 0 && err == io.EOF {
			continue
		}
		kstr, knum, hasNum := s.extractKey(line)
		heap.Push(h, &mergeItem{
			rec:    &Line{line: line, KeyStr: kstr, KeyNum: knum, HasNum: hasNum},
			reader: src.br,
			file:   src.f,
			idx:    i,
		})
	}

	bw := bufio.NewWriterSize(out, 1<<20)
	defer bw.Flush()

	var lastOut *Line
	for h.Len() > 0 {
		it := heap.Pop(h).(*mergeItem)
		// -u: глобально дедупим по ключу
		if !(s.flags.U && lastOut != nil && s.equalKey(it.rec, lastOut)) {
			if _, err := bw.Write(it.rec.line); err != nil {
				return err
			}
			if err := bw.WriteByte('\n'); err != nil {
				return err
			}
			tmp := *it.rec
			lastOut = &tmp
		}
		// читаем следующую строку из того же источника
		line, err := readLine(it.reader)
		if err != nil && err != io.EOF {
			return err
		}
		if len(line) > 0 || err == nil {
			kstr, knum, hasNum := s.extractKey(line)
			it.rec = &Line{line: line, KeyStr: kstr, KeyNum: knum, HasNum: hasNum}
			heap.Push(h, it)
		} else {
			_ = it.file.Close()
		}
	}
	// закрыть на всякий случай
	for _, src := range sources {
		_ = src.f.Close()
	}
	return nil
}

func readLine(r *bufio.Reader) ([]byte, error) {
	b, err := r.ReadBytes('\n')
	if err == io.EOF && len(b) > 0 {
		return chomp(b), nil
	}
	if err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, err
	}
	return chomp(b), nil
}

func chomp(b []byte) []byte {
	if len(b) > 0 && b[len(b)-1] == '\n' {
		return b[:len(b)-1]
	}
	return b
}
