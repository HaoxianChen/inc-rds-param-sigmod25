package cmap

import (
	"encoding/json"
	"fmt"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"sync"
)

var SHARD_COUNT = 32

type Stringer interface {
	fmt.Stringer
	comparable
}

// A "thread" safe map of type string:Anything.
// To avoid lock bottlenecks this map is dived to several (SHARD_COUNT) map Shards.
type ConcurrentMap[K comparable, V any] struct {
	Shards   []*ConcurrentMapShared[K, V]
	sharding func(key K) uint32
}

// A "thread" safe string to anything map.
type ConcurrentMapShared[K comparable, V any] struct {
	Items        map[K]V
	sync.RWMutex // Read Write mutex, guards access to internal map.
}

func create[K comparable, V any](sharding func(key K) uint32) ConcurrentMap[K, V] {
	m := ConcurrentMap[K, V]{
		sharding: sharding,
		Shards:   make([]*ConcurrentMapShared[K, V], SHARD_COUNT),
	}
	for i := 0; i < SHARD_COUNT; i++ {
		m.Shards[i] = &ConcurrentMapShared[K, V]{Items: make(map[K]V, 16384)}
	}
	return m
}

// Creates a new concurrent map.
func New[V any]() ConcurrentMap[string, V] {
	return create[string, V](Fnv32)
}

// Creates a new concurrent map.
func NewStringer[K Stringer, V any]() ConcurrentMap[K, V] {
	return create[K, V](strfnv32[K])
}

// Creates a new concurrent map.
func NewWithCustomShardingFunction[K comparable, V any](sharding func(key K) uint32) ConcurrentMap[K, V] {
	return create[K, V](sharding)
}

// GetShard returns shard under given key
func (m ConcurrentMap[K, V]) GetShard(key K) *ConcurrentMapShared[K, V] {
	return m.Shards[uint(m.sharding(key))%uint(SHARD_COUNT)]
}

func (m ConcurrentMap[K, V]) getShardId(key K) uint {
	return uint(m.sharding(key)) % uint(SHARD_COUNT)
}

func (m ConcurrentMap[K, V]) MSet(data map[K]V) {
	for key, value := range data {
		shard := m.GetShard(key)
		shard.Lock()
		shard.Items[key] = value
		shard.Unlock()
	}
}

func (m ConcurrentMap[K, V]) MSetUnThreadSafe(data map[K]V) {
	for key, value := range data {
		shard := m.GetShard(key)
		shard.Items[key] = value
	}
}

func (m ConcurrentMap[K, V]) MSetUnThreadSafeParallel(data map[K]V) {
	var temp = make([]K, 0, len(data))
	for k := range data {
		temp = append(temp, k)
	}
	_ = parallelBatch(0, len(temp), SHARD_COUNT, func(batchStart, batchEnd, _ int) error {
		var shard2ks = make([][]int, SHARD_COUNT)
		for i := 0; i < SHARD_COUNT; i++ {
			shard2ks[i] = make([]int, 0, (batchEnd-batchStart)/SHARD_COUNT+16)
		}
		ks := temp[batchStart:batchEnd]
		for i, k := range ks {
			shardId := m.getShardId(k)
			shard2ks[shardId] = append(shard2ks[shardId], i)
		}
		for shardId, kIds := range shard2ks {
			if len(kIds) > 0 {
				shard := m.Shards[shardId]
				shard.Lock()
				for _, kid := range kIds {
					k := ks[kid]
					shard.Items[k] = data[k]
				}
				shard.Unlock()
			}
		}
		return nil
	})
}

// Sets the given value under the specified key.
func (m ConcurrentMap[K, V]) Set(key K, value V) {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Lock()
	shard.Items[key] = value
	shard.Unlock()
}

// Callback to return new element to be inserted into the map
// It is called while lock is held, therefore it MUST NOT
// try to access other keys in same map, as it can lead to deadlock since
// Go sync.RWLock is not reentrant
type UpsertCb[V any] func(exist bool, valueInMap V, newValue V) V

// Insert or Update - updates existing element or inserts a new one using UpsertCb
func (m ConcurrentMap[K, V]) Upsert(key K, value V, cb UpsertCb[V]) (res V) {
	shard := m.GetShard(key)
	shard.Lock()
	v, ok := shard.Items[key]
	res = cb(ok, v, value)
	shard.Items[key] = res
	shard.Unlock()
	return res
}

// Sets the given value under the specified key if no value was associated with it.
func (m ConcurrentMap[K, V]) SetIfAbsent(key K, value V) bool {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Lock()
	_, ok := shard.Items[key]
	if !ok {
		shard.Items[key] = value
	}
	shard.Unlock()
	return !ok
}

// Get retrieves an element from map under given key.
func (m ConcurrentMap[K, V]) Get(key K) (V, bool) {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// Get item from shard.
	val, ok := shard.Items[key]
	shard.RUnlock()
	return val, ok
}

// Count returns the number of elements within the map.
func (m ConcurrentMap[K, V]) Count() int {
	count := 0
	for i := 0; i < len(m.Shards); i++ {
		shard := m.Shards[i]
		shard.RLock()
		count += len(shard.Items)
		shard.RUnlock()
	}
	return count
}

// Looks up an item under specified key
func (m ConcurrentMap[K, V]) Has(key K) bool {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// See if element is within shard.
	_, ok := shard.Items[key]
	shard.RUnlock()
	return ok
}

// Remove removes an element from the map.
func (m ConcurrentMap[K, V]) Remove(key K) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	delete(shard.Items, key)
	shard.Unlock()
}

// RemoveCb is a callback executed in a map.RemoveCb() call, while Lock is held
// If returns true, the element will be removed from the map
type RemoveCb[K any, V any] func(key K, v V, exists bool) bool

// RemoveCb locks the shard containing the key, retrieves its current value and calls the callback with those params
// If callback returns true and element exists, it will remove it from the map
// Returns the value returned by the callback (even if element was not present in the map)
func (m ConcurrentMap[K, V]) RemoveCb(key K, cb RemoveCb[K, V]) bool {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	v, ok := shard.Items[key]
	remove := cb(key, v, ok)
	if remove && ok {
		delete(shard.Items, key)
	}
	shard.Unlock()
	return remove
}

// Pop removes an element from the map and returns it
func (m ConcurrentMap[K, V]) Pop(key K) (v V, exists bool) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	v, exists = shard.Items[key]
	delete(shard.Items, key)
	shard.Unlock()
	return v, exists
}

// IsEmpty checks if map is empty.
func (m ConcurrentMap[K, V]) IsEmpty() bool {
	return m.Count() == 0
}

// Used by the Iter & IterBuffered functions to wrap two variables together over a channel,
type Tuple[K comparable, V any] struct {
	Key K
	Val V
}

// Iter returns an iterator which could be used in a for range loop.
//
// Deprecated: using IterBuffered() will get a better performence
func (m ConcurrentMap[K, V]) Iter() <-chan Tuple[K, V] {
	chans := snapshot(m)
	ch := make(chan Tuple[K, V])
	go fanIn(chans, ch)
	return ch
}

// IterBuffered returns a buffered iterator which could be used in a for range loop.
func (m ConcurrentMap[K, V]) IterBuffered() <-chan Tuple[K, V] {
	chans := snapshot(m)
	total := 0
	for _, c := range chans {
		total += cap(c)
	}
	ch := make(chan Tuple[K, V], total)
	go fanIn(chans, ch)
	return ch
}

// Clear removes all Items from map.
func (m ConcurrentMap[K, V]) Clear() {
	for item := range m.IterBuffered() {
		m.Remove(item.Key)
	}
}

func (m ConcurrentMap[K, V]) OneKeyUnThreadSafe() (key K) {
	for _, shard := range m.Shards {
		for k := range shard.Items {
			return k
		}
	}
	logger.Warn("empty map")
	return key
}

func (m ConcurrentMap[K, V]) OneValueUnThreadSafe() (value V) {
	for _, shard := range m.Shards {
		for _, v := range shard.Items {
			return v
		}
	}
	logger.Warn("empty map")
	return value
}

// Returns a array of channels that contains elements in each shard,
// which likely takes a snapshot of `m`.
// It returns once the size of each buffered channel is determined,
// before all the channels are populated using goroutines.
func snapshot[K comparable, V any](m ConcurrentMap[K, V]) (chans []chan Tuple[K, V]) {
	//When you access map Items before initializing.
	if len(m.Shards) == 0 {
		panic(`cmap.ConcurrentMap is not initialized. Should run New() before usage.`)
	}
	chans = make([]chan Tuple[K, V], SHARD_COUNT)
	wg := sync.WaitGroup{}
	wg.Add(SHARD_COUNT)
	// Foreach shard.
	for index, shard := range m.Shards {
		go func(index int, shard *ConcurrentMapShared[K, V]) {
			// Foreach key, value pair.
			shard.RLock()
			chans[index] = make(chan Tuple[K, V], len(shard.Items))
			wg.Done()
			for key, val := range shard.Items {
				chans[index] <- Tuple[K, V]{key, val}
			}
			shard.RUnlock()
			close(chans[index])
		}(index, shard)
	}
	wg.Wait()
	return chans
}

// fanIn reads elements from channels `chans` into channel `out`
func fanIn[K comparable, V any](chans []chan Tuple[K, V], out chan Tuple[K, V]) {
	wg := sync.WaitGroup{}
	wg.Add(len(chans))
	for _, ch := range chans {
		go func(ch chan Tuple[K, V]) {
			for t := range ch {
				out <- t
			}
			wg.Done()
		}(ch)
	}
	wg.Wait()
	close(out)
}

// Items returns all Items as map[string]V
func (m ConcurrentMap[K, V]) Items() map[K]V {
	tmp := make(map[K]V)

	// Insert Items to temporary map.
	for item := range m.IterBuffered() {
		tmp[item.Key] = item.Val
	}

	return tmp
}

// Iterator callbacalled for every key,value found in
// maps. RLock is held for all calls for a given shard
// therefore callback sess consistent view of a shard,
// but not across the Shards
type IterCb[K comparable, V any] func(key K, v V)

// Callback based iterator, cheapest way to read
// all elements in a map.
func (m ConcurrentMap[K, V]) IterCb(fn IterCb[K, V]) {
	for idx := range m.Shards {
		shard := (m.Shards)[idx]
		shard.RLock()
		for key, value := range shard.Items {
			fn(key, value)
		}
		shard.RUnlock()
	}
}

// Keys returns all keys as []string
func (m ConcurrentMap[K, V]) Keys() []K {
	count := m.Count()
	ch := make(chan K, count)
	go func() {
		// Foreach shard.
		wg := sync.WaitGroup{}
		wg.Add(SHARD_COUNT)
		for _, shard := range m.Shards {
			go func(shard *ConcurrentMapShared[K, V]) {
				// Foreach key, value pair.
				shard.RLock()
				for key := range shard.Items {
					ch <- key
				}
				shard.RUnlock()
				wg.Done()
			}(shard)
		}
		wg.Wait()
		close(ch)
	}()

	// Generate keys
	keys := make([]K, 0, count)
	for k := range ch {
		keys = append(keys, k)
	}
	return keys
}

// Reviles ConcurrentMap "private" variables to json marshal.
func (m ConcurrentMap[K, V]) MarshalJSON() ([]byte, error) {
	// Create a temporary map, which will hold all item spread across Shards.
	tmp := make(map[K]V)

	// Insert Items to temporary map.
	for item := range m.IterBuffered() {
		tmp[item.Key] = item.Val
	}
	return json.Marshal(tmp)
}
func strfnv32[K fmt.Stringer](key K) uint32 {
	return Fnv32(key.String())
}

func Fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	keyLength := len(key)
	for i := 0; i < keyLength; i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

// Reverse process of Marshal.
func (m *ConcurrentMap[K, V]) UnmarshalJSON(b []byte) (err error) {
	tmp := make(map[K]V)

	// Unmarshal into a single map.
	if err := json.Unmarshal(b, &tmp); err != nil {
		return err
	}

	// foreach key,value pair in temporary map insert into our concurrent map.
	for key, val := range tmp {
		m.Set(key, val)
	}
	return nil
}

func parallelBatch(start, end, degree int, worker func(batchStart, batchEnd, workerId int) error) error {
	number := end - start
	batchSize := number / degree
	if number%degree != 0 {
		batchSize++
	}
	wg := sync.WaitGroup{}
	var err error
	for workerId := 0; workerId < degree; workerId++ {
		batchStart := start + batchSize*workerId
		if batchStart >= end {
			continue
		}
		batchEnd := start + batchSize*(workerId+1)
		if batchEnd > end {
			batchEnd = end
		}

		wg.Add(1)
		go func(batchStart, batchEnd, workerId int) {
			defer wg.Done()
			e := worker(batchStart, batchEnd, workerId)
			if e != nil {
				err = e
			}
		}(batchStart, batchEnd, workerId)
	}

	wg.Wait()
	return err
}
