package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"runtime/pprof"
	"sort"
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

func (s *Statistic) PrintResult() {
	keys := make([]string, 0, len(s.measures))
	for key := range s.measures {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) > 0 {
		fmt.Printf("{")
		key := keys[0]
		m := s.measures[key]
		fmt.Printf("%s=%.1f/%.1f/%.1f", key, float64(m.min)/10, float64(m.sum)/float64(m.count*10), float64(m.max)/10)
		for _, key := range keys[1:] {
			m := s.measures[key]
			fmt.Printf(", %s=%.1f/%.1f/%.1f", key, float64(m.min)/10, float64(m.sum)/float64(m.count*10), float64(m.max)/10)
		}
		fmt.Printf("}\n")
	}
}

func addStatistic(m map[string]*M, name string, val int64) {
	_m, ok := m[name]
	if !ok {
		_m = newM()
		m[name] = _m
	}
	_m.Add(val)
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

func parseLine(line []byte) ([]byte, int64) {
	idx := bytes.IndexByte(line, ';')
	val := int64(0)
	neg := false
	if line[idx+1] == '-' {
		neg = true
	}
	for _, ch := range line[idx+1:] {
		if ch >= '0' && ch <= '9' {
			val = val*10 + int64(ch-'0')
		}
	}
	if neg {
		val = -val
	}
	return line[:idx], val
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

	statistic := newStatistic()

	scanner := bufio.NewScanner(file)
	buffer := make([]byte, 100*1024*1024)
	scanner.Buffer(buffer, len(buffer))
	for scanner.Scan() {
		name, measure := parseLine(scanner.Bytes())
		statistic.Add(name, measure)
	}
	pie(scanner.Err())

	statistic.PrintResult()
}
