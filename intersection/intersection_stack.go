package intersection

import "sync"

type IdPairsTask struct {
	idPairs [][]int32
	pid     int
}

type Stack struct {
	workerNumber int
	data         [][]IdPairsTask
	numbers      []int
	mutexes      []sync.RWMutex
}

func newStack(workerNumber int) *Stack {
	searchIndexes := make([][]int, workerNumber)
	for wi := 0; wi < workerNumber; wi++ {
		searchIndex := make([]int, workerNumber)
		for i := 0; i < workerNumber; i++ {
			searchIndex[i] = (wi + i) % workerNumber
		}
		searchIndexes[wi] = searchIndex
	}
	return &Stack{
		workerNumber: workerNumber,
		data:         make([][]IdPairsTask, workerNumber),
		numbers:      make([]int, workerNumber),
		mutexes:      make([]sync.RWMutex, workerNumber),
	}
}

func (s *Stack) pop(cid int) (top IdPairsTask, ok bool) { // 并发安全
	for i := 0; i < s.workerNumber; i++ {
		id := (i + cid) % s.workerNumber
		s.mutexes[id].Lock()
		size := len(s.data[id])
		if size > 0 {
			top = s.data[id][size-1]
			s.data[id] = s.data[id][:size-1]
			s.numbers[id]--
			s.mutexes[id].Unlock()
			return top, true
		} else {
			s.mutexes[id].Unlock()
		}
	}

	// 没有任务
	return top, false
}

func (s *Stack) empty() bool {
	for i := 0; i < s.workerNumber; i++ {
		s.mutexes[i].RLock()
		if s.numbers[i] > 0 {
			return false
		}
		s.mutexes[i].RUnlock()
	}
	return true
}

func (s *Stack) push(nodes ...IdPairsTask) { // 并发安全
	size := len(nodes)
	if size == 0 {
		return
	}
	batch := (size + s.workerNumber - 1) / s.workerNumber
	for i := 0; i < s.workerNumber; i++ {
		start := batch * i
		if start >= size {
			break
		}
		end := batch * (i + 1)
		if end > size {
			end = size
		}
		number := start - end
		s.mutexes[i].Lock()
		s.data[i] = append(s.data[i], nodes[start:end]...)
		s.numbers[i] += number
		s.mutexes[i].Unlock()
	}
}
