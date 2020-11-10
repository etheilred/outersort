package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const blockSize = (1 << 28)

type csvStream struct {
	current []string
	reader  *csv.Reader
}

func newCsvStream(file *os.File) *csvStream {
	rd := csv.NewReader(file)
	t := &csvStream{nil, rd}
	t.get()
	return t
}

func (scs *csvStream) less(other *csvStream, column int, comparer func(string, string) bool) bool {
	if scs.current == nil || other.current == nil {
		return scs.current != nil && other.current == nil
	}
	return comparer(scs.current[column], other.current[column])
}

func (scs *csvStream) get() []string {
	if scs.current != nil {
		return scs.current
	}
	rec, err := scs.reader.Read()
	if err != nil {
		scs.current = nil
		return nil
	}
	scs.current = rec
	return scs.current
}

func (scs *csvStream) pop() []string {
	t := scs.current
	rec, err := scs.reader.Read()
	if err != nil {
		scs.current = nil
	} else {
		scs.current = rec
	}
	return t
}

func buildMinHeap(streams []*csvStream, column int, comparer func(string, string) bool) {
	for i := len(streams) / 2; i >= 0; i-- {
		siftDownMin(streams, i, column, comparer)
	}
}

func siftDownMin(streams []*csvStream, i, column int, comparer func(string, string) bool) {
	for 2*i+1 < len(streams) {
		left := 2*i + 1
		right := 2*i + 2
		j := left
		if right < len(streams) && streams[right].less(streams[left], column, comparer) {
			j = right
		}
		if !streams[j].less(streams[i], column, comparer) {
			break
		}
		streams[i], streams[j] = streams[j], streams[i]
		i = j
	}
}

func extractMin(streams []*csvStream, column int, comparer func(string, string) bool) []string {
	t := streams[0].pop()
	siftDownMin(streams, 0, column, comparer)
	return t
}

func removeExt(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '.' {
			return path[:i]
		}
	}
	return path
}

func sortCsv(file *os.File, column int, comparer func(string, string) bool) error {
	rd := csv.NewReader(file)
	bufferFileNamePrefix := "buffsort"
	// split to sorted chunks-files
	buffCount, err := splitToChuncks(rd, bufferFileNamePrefix, column, comparer)

	if err != nil {
		return err
	}
	sortedPath := filepath.Dir(file.Name()) + "/" + removeExt(filepath.Base(file.Name())) + "_sorted" + filepath.Ext(file.Name())
	file, err = os.Create(sortedPath)
	// os.Open(strings.TrimSuffix(file.Name(), filepath.Ext(file.Name())) + "." + filepath.Ext(file.Name()))
	if err != nil {
		return err
	}
	wt := csv.NewWriter(file)
	csvStreams := make([]*csvStream, buffCount)
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	for i := 0; i < buffCount; i++ {
		f, _ := os.Open(bufferFileNamePrefix + fmt.Sprintf("%d", i))
		defer func(f *os.File, i int) {
			wg.Add(1)
			go func(f *os.File, i int, wg *sync.WaitGroup) {
				f.Close()
				err := os.Remove(bufferFileNamePrefix + fmt.Sprintf("%d", i))
				if err != nil {
					fmt.Println(err.Error())
				}
				wg.Done()
			}(f, i, wg)
		}(f, i)
		csvStreams[i] = newCsvStream(f)
	}
	buildMinHeap(csvStreams, column, comparer)
	for csvStreams[0].current != nil {
		wt.Write(extractMin(csvStreams, column, comparer))
	}

	wt.Flush()

	file.Close()
	fmt.Printf("sorted to: %s\n", sortedPath)
	return nil
}

func splitToChuncks(rd *csv.Reader, bufferFileNamePrefix string, column int, comparer func(string, string) bool) (int, error) {
	var arr [][]string // buffer
	bufferCounter := 0
	byteLen := 0
	record, err := rd.Read()
	// wg := &sync.WaitGroup{}
	for err == nil {
		// fmt.Println(record)
		arr = append(arr, record)
		byteLen += getLen(record)
		if byteLen >= blockSize {
			byteLen = 0
			sort.Slice(arr, func(i, j int) bool {
				return comparer(arr[i][column], arr[j][column])
			})
			//fmt.Println(arr)
			// wg.Add(1)
			// go func(wg *sync.WaitGroup, arr [][]string, bufferCounter int) {
			err := writeToCsv(bufferFileNamePrefix+fmt.Sprintf("%d", bufferCounter), arr)
			bufferCounter++
			if err != nil {
				fmt.Println(err)
			}
			arr = nil
			//wg.Done()
			// }(wg, arr, bufferCounter)
		}
		record, err = rd.Read()
	}
	if len(arr) != 0 {
		sort.Slice(arr, func(i, j int) bool {
			return comparer(arr[i][column], arr[j][column])
		})
		//fmt.Println(arr)
		// wg.Add(1)
		// go func(wg *sync.WaitGroup, arr [][]string, bufferCounter int) {
		err := writeToCsv(bufferFileNamePrefix+fmt.Sprintf("%d", bufferCounter), arr)
		bufferCounter++
		if err != nil {
			fmt.Println(err)
		}
		arr = nil
		//wg.Done()
		// }(wg, arr, bufferCounter)
	}
	// wg.Wait()
	return bufferCounter, nil
}

func writeToCsv(filename string, arr [][]string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	wt := csv.NewWriter(file)
	defer wt.Flush()
	wt.WriteAll(arr)
	// wg.Done()
	return nil
}

func getLen(record []string) int {
	l := 0
	for _, col := range record {
		l += len(col)
	}
	return l
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Not enough arguments provided\nRequired: <path:string> <column:int> (optional <int|float|string>)")
		os.Exit(1)
	}
	file, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}
	defer file.Close()
	fmt.Println(file.Name())
	column := stoi(os.Args[2])
	fn := compareStrings
	if len(os.Args) > 3 && os.Args[3] != "string" {
		if os.Args[3] == "int" {
			fn = compareInts
		} else if os.Args[3] == "float" {
			fn = compareFloats
		}
	}
	now := time.Now()
	err = sortCsv(file, column, fn)
	elapsed := time.Since(now)
	fmt.Println("elapsed time: ", elapsed)
	if err != nil {
		panic(err)
	}
}

func compareStrings(a, b string) bool {
	// a = strings.ToLower(a)
	// b = strings.ToLower(b)
	return strings.Compare(a, b) == -1
}

func compareInts(a, b string) bool {
	return stoi(a) < stoi(b)
}

func compareFloats(a, b string) bool {
	return stof(a) < stof(b)
}

func stoi(s string) int {
	a, _ := strconv.Atoi(s)
	return a
}

func stof(s string) float64 {
	a, _ := strconv.ParseFloat(s, 64)
	return a
}
