package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"sync"
	"unsafe"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

func pie(e error) {
	if e != nil {
		panic(e)
	}
}

func UnsafeBytesToString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

type Statistic struct {
	keys     []byte
	measures map[string]*M
}

func newStatistic() *Statistic {
	return &Statistic{
		keys:     make([]byte, 0, 8*1024),
		measures: make(map[string]*M),
	}
}

func (s *Statistic) Add(nameBytes []byte, val int64) {
	name := UnsafeBytesToString(nameBytes)
	m, ok := s.measures[name]
	if !ok {
		s.keys = append(s.keys, nameBytes...)
		name = UnsafeBytesToString(s.keys[len(s.keys)-len(name):])
		m = newM()
		s.measures[name] = m
	}
	m.Add(val)
}

func (s *Statistic) ParseAndAddLines(lines []byte) {
	for {
		idx := bytes.IndexByte(lines, ';')
		if idx < 0 {
			return
		}
		val := int64(0)
		neg := lines[idx+1] == '-'
		i := idx + 1
		for i < len(lines) {
			if lines[i] == '\n' {
				i++
				break
			}
			if lines[i] >= '0' && lines[i] <= '9' {
				val = val*10 + int64(lines[i]-'0')
			}
			i++
		}
		if neg {
			val = -val
		}
		s.Add(lines[:idx], val)
		lines = lines[i:]
	}
}

func (s *Statistic) PrintResult() {
	printResult(s.measures)
}

type MergedStatistics struct {
	keys     [][]byte
	measures map[string]*M
}

func mergeStatistics(slice ...*Statistic) *MergedStatistics {
	r := &MergedStatistics{
		measures: make(map[string]*M),
	}

	for _, s := range slice {
		r.keys = append(r.keys, s.keys)
		for name, m := range s.measures {
			m2, ok := r.measures[name]
			if !ok {
				r.measures[name] = m
			} else {
				m2.count += m.count
				m2.sum += m.sum
				if m.min < m2.min {
					m2.min = m.min
				}
				if m.max > m2.max {
					m2.max = m.max
				}
			}
		}
	}

	return r
}

func (s *MergedStatistics) PrintResult() {
	printResult(s.measures)
}

func printResult(measures map[string]*M) {
	keys := make([]string, 0, len(measures))
	for key := range measures {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) > 0 {
		fmt.Printf("{")
		key := keys[0]
		m := measures[key]
		fmt.Printf("%s=%.1f/%.1f/%.1f", key, float64(m.min)/10, float64(m.sum)/float64(m.count*10), float64(m.max)/10)
		for _, key := range keys[1:] {
			m := measures[key]
			fmt.Printf(", %s=%.1f/%.1f/%.1f", key, float64(m.min)/10, float64(m.sum)/float64(m.count*10), float64(m.max)/10)
		}
		fmt.Printf("}\n")
	}
}

type M struct {
	name  string
	count int
	min   int64
	max   int64
	sum   int64
}

func newM() *M {
	return &M{
		count: 0,
		min:   math.MaxInt64,
		max:   math.MinInt64,
	}
}

func (m *M) Add(val int64) {
	m.count++
	m.sum += val
	if val < m.min {
		m.min = val
	}
	if val > m.max {
		m.max = val
	}
}

// 相比于scanner默认的SplitFunc，会读取多行，实现方式是按缓冲区中最后一个换行符进行区分
// 这样读取到的token实际包含多行数据，并且需要注意可能会有多余的'\r'字符
func scanManyLines(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.LastIndexByte(data, '\n'); i >= 0 {
		// We have a full newline-terminated line.
		return i + 1, data[0:i], nil
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), data, nil
	}
	// Request more data.
	return 0, nil, nil
}

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile) // ignore_security_alert
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	file, err := os.Open("measurements.txt")
	pie(err)
	defer file.Close()

	num := min(8, runtime.NumCPU())
	statistics := make([]*Statistic, num)

	wg := &sync.WaitGroup{}
	ch := make(chan []byte)
	for i := 0; i < num; i++ {
		go func(idx int) {
			statistics[idx] = newStatistic()
			for lines := range ch {
				statistics[idx].ParseAndAddLines(lines)
				wg.Done()
			}
		}(i)
	}

	scanner := bufio.NewScanner(file)
	buffer := make([]byte, 256*1024*1024)
	scanner.Buffer(buffer, len(buffer))
	scanner.Split(scanManyLines)

	sep := []byte("\n")
	for scanner.Scan() {
		data := scanner.Bytes()
		count := bytes.Count(data, sep)

		step := min(count+1, max(10, (count+1)/num+1))

		var (
			n          = 0
			start      = 0
			batchStart = 0
		)
		for {
			pos := bytes.IndexByte(data[start:], '\n')
			if pos < 0 {
				wg.Add(1)
				ch <- data[batchStart:]
				break
			}
			n++
			if n%step == 0 {
				wg.Add(1)
				ch <- data[batchStart : start+pos]
				batchStart = start + pos + 1
			}
			start = start + pos + 1
		}
		wg.Wait()
	}
	pie(scanner.Err())

	statistic := mergeStatistics(statistics...)
	statistic.PrintResult()
}
