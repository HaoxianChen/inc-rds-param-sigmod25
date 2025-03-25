package storage_utils

import (
	"errors"
	"fmt"
	"github.com/vmihailenco/msgpack/v5"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock_v3/utils/blocking/blocking_conf"
	"gitlab.grandhoo.com/rock/rock_v3/utils/cmap"
	"gitlab.grandhoo.com/rock/storage/config"
	"gitlab.grandhoo.com/rock/storage/storage2/database/types/special_type/decimal"
	"gitlab.grandhoo.com/rock/storage/storage2/database/types/special_type/money"
	"gitlab.grandhoo.com/rock/storage/storage2/database/types/special_type/timetype"
	"gitlab.grandhoo.com/rock/storage/storage2/utils"
	"gitlab.grandhoo.com/rock/storage/storage2/utils/hasher"
	"math"
	"strconv"
	"strings"
	"sync"
)

func sameLeftRight(p1 rds.Predicate, p2 rds.Predicate) bool {
	return p1.LeftColumn.TableId == p2.LeftColumn.TableId && p1.LeftColumn.ColumnId == p2.LeftColumn.ColumnId &&
		p1.RightColumn.TableId == p2.RightColumn.TableId && p1.RightColumn.ColumnId == p2.RightColumn.ColumnId
}

func Max[N int | int32 | int64](a, b N) N {
	if a > b {
		return a
	} else {
		return b
	}
}

func Min[N int | int32 | int64](a, b N) N {
	if a < b {
		return a
	} else {
		return b
	}
}

type Pair[T1, T2 any] struct {
	Left  T1
	Right T2
}

// RowSet 行号集合，以及 interestingTableColumn 值
// 注意：修改字段需要修改对应的 EncodeMsgpack DecodeMsgpack
// (rs RowSet) EncodeMsgpack(e *msgpack.Encoder) error
// (rs *RowSet) DecodeMsgpack(d *msgpack.Decoder) error
type RowSet struct {
	RowIds    []int32
	CellsList [][]string
	// 注意：修改字段需要修改对应的 EncodeMsgpack DecodeMsgpack
}

const tableScanBatchSize = config.ShardSize // 批量读表大小

const maxPredicateSize predicateId = 8
const constPredicateType = 0
const structPredicateType = 1
const foreignKeyPredicateType = 2

var TooManyCubeKeyErr = errors.New("too many cube key")
var StopSignal = errors.New("too many cube key")
var hyperCubeMu = sync.Mutex{}

type CubeKey [maxPredicateSize]string // 2023年10月19日 修改类型，性能优化

type tableName = string // 表名类型

type columnName = string // 类名类型
type columnId = int      // 存储中列编号
type tableId int         // 表 id 类型

type predicateId = int // 谓词 id 类型

type tabNameId Pair[tableName, tableId] // 表名+表id

type colNamePid Pair[columnName, predicateId] // 列名+对应的谓词id

type tableColumn Pair[tableName, columnName]

type tableColumnPidSlice Pair[tabNameId, []colNamePid]

type stringBlockingTokenIds Pair[string, []blocking_conf.TokenId]

const cubeKeySplit = "\a"

const cubeKeySplitByte = '\a'

func (key CubeKey) Get(i int) string {
	return key[i]
}

func (key CubeKey) GetBatch(indexes []int) []string {
	r := make([]string, len(indexes))
	for i, id := range indexes {
		r[i] = key[id]
	}
	return r
}

func (key CubeKey) String() string {
	sb := strings.Builder{}
	for i, s := range key {
		sb.WriteString(s)
		if i < maxPredicateSize-1 {
			sb.WriteString(cubeKeySplit)
		}
	}
	return sb.String()
}

func cubeKeyHash(key CubeKey) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)

	for _, s := range key {
		sl := len(s)
		switch sl {
		case 0:
			continue
		case 1:
			hash *= prime32
			hash ^= uint32(s[0])
		case 2:
			hash *= prime32
			hash ^= uint32(s[0])
			hash *= prime32
			hash ^= uint32(s[1])
		case 3:
			hash *= prime32
			hash ^= uint32(s[0])
			hash *= prime32
			hash ^= uint32(s[1])
			hash *= prime32
			hash ^= uint32(s[2])
		default:
			hash ^= cmap.Fnv32(s)
			//hash *= prime32
			//hash ^= uint32(s[sl-3])
			//hash *= prime32
			//hash ^= uint32(s[sl-2])
			//hash *= prime32
			//hash ^= uint32(s[sl-1])

		}
	}

	return hash
}

func (key CubeKey) EncodeMsgpack(e *msgpack.Encoder) error {
	return e.EncodeString(key.String())
}

func (key *CubeKey) DecodeMsgpack(d *msgpack.Decoder) error {
	str, err := d.DecodeString()
	if err != nil {
		return err
	}
	*key = CubeKeyFromString(str)
	return nil
}

func (rs RowSet) EncodeMsgpack(e *msgpack.Encoder) error {
	// RowIds
	rowIdsLen := int32(len(rs.RowIds))
	err := e.EncodeInt32(rowIdsLen)
	if err != nil {
		return err
	}
	for _, rid := range rs.RowIds {
		err = e.EncodeInt32(rid)
		if err != nil {
			return err
		}
	}
	// CellsList
	cellsListLen := int32(len(rs.CellsList))
	err = e.EncodeInt32(cellsListLen)
	if err != nil {
		return err
	}
	if cellsListLen > 0 {
		cellsLen := int32(len(rs.CellsList[0]))
		err = e.EncodeInt32(cellsLen)
		if err != nil {
			return err
		}
		if cellsLen > 0 {
			for _, cells := range rs.CellsList {
				for _, cell := range cells {
					err = e.EncodeString(cell)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (rs *RowSet) DecodeMsgpack(d *msgpack.Decoder) error {
	// RowIds
	rowIdsLen, err := d.DecodeInt32()
	if err != nil {
		return err
	}
	rs.RowIds = make([]int32, rowIdsLen)
	for i := int32(0); i < rowIdsLen; i++ {
		rs.RowIds[i], err = d.DecodeInt32()
		if err != nil {
			return err
		}
	}
	// CellsList
	cellsListLen, err := d.DecodeInt32()
	if err != nil {
		return err
	}
	if cellsListLen > 0 {
		rs.CellsList = make([][]string, cellsListLen)
		cellsLen, err := d.DecodeInt32()
		if err != nil {
			return err
		}
		if cellsLen > 0 {
			for i := int32(0); i < cellsListLen; i++ {
				cells := make([]string, cellsLen)
				for j := int32(0); j < cellsLen; j++ {
					cells[j], err = d.DecodeString()
				}
				rs.CellsList[i] = cells
			}
		}
	}
	return nil
}

func CubeKeyFromString(s string) CubeKey {
	var key CubeKey
	copy(key[:], strings.Split(s, cubeKeySplit))
	return key
}

func (key CubeKey) Len() int {
	return maxPredicateSize
}

func (key CubeKey) Slice() []string {
	return key[:]
}

func (key CubeKey) HashCode() int64 {
	var h int64 = hasher.NilHash
	for i := 0; i < maxPredicateSize; i++ {
		h += hasher.HashString(key[i])
	}
	return h
}

func (key CubeKey) HasNull(indexes []int) bool {
	for _, s := range key.GetBatch(indexes) {
		if s == "Null" || s == "" || s == "nil" {
			return true
		}
	}
	return false
}

func (key CubeKey) Hashy(hashBase int64) CubeKey {
	for i := range key {
		s := key[i]
		if s != "" {
			key[i] = strconv.FormatInt(hasher.HashString(s)%hashBase, 36)
		}
	}
	return key
}

func (key CubeKey) HashySkip(hashBase int64, skips []bool) CubeKey {
	for i, skip := range skips {
		if !skip {
			s := key[i]
			if s != "" {
				key[i] = strconv.FormatInt(hasher.HashString(s)%hashBase, 36)
			}
		}
	}
	return key
}

func (p Pair[T1, T2]) String() string {
	return fmt.Sprintf("[%s:%s]", fmt.Sprintf("%v", p.Left), fmt.Sprintf("%v", p.Right))
}

func slices2string[E any](es []E) string {
	sb := strings.Builder{}
	sb.WriteString("[")
	for i, e := range es {
		sb.WriteString(fmt.Sprintf("%v", e))
		if i < len(es)-1 {
			sb.WriteString(", ")
		}
	}
	sb.WriteString("]")
	return sb.String()
}

func pop[E any, S ~[]E](s S) (first E, others S) {
	return s[0], s[1:]
}

func PopNumber[N1, N2 utils.Number](s []any) (first N2, others []any) {
	f, s := pop(s)
	return N2(f.(N1)), s
}

func parseFloat64(s string) float64 {
	if len(s) == 0 {
		return math.NaN()
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return math.NaN()
	}
	return f
}

func float64Of(i any) float64 {
	if i == nil {
		return math.NaN()
	}
	switch v := i.(type) {
	case int64:
		return float64(v)
	case float64:
		return v
	case decimal.BigDecimal:
		return v.FloatVal()
	case timetype.Time:
		return float64(v.UnixMilli())
	case money.Money:
		return v.FloatVal()
	case string:
		return parseFloat64(v)
	default:
		s := utils.ToString(i)
		return parseFloat64(s)
	}
}

func Mapping[E, R any](es []E, c func(E) R) []R {
	rs := make([]R, len(es))
	for i, e := range es {
		rs[i] = c(e)
	}
	return rs
}

// Filtrate 原地过滤
func Filtrate[E any, S ~[]E](es S, filter func(e E) bool) S {
	var k = 0
	for i := range es {
		if filter(es[i]) {
			if i != k {
				es[i], es[k] = es[k], es[i]
			}
			k++
		}
	}
	return es[:k]
}

func MapKVs[K comparable, V any, M ~map[K]V](m M) []Pair[K, V] {
	var ps = make([]Pair[K, V], 0, len(m))
	for k, v := range m {
		ps = append(ps, Pair[K, V]{k, v})
	}
	return ps
}

func predicatesToStrings(ps []rds.Predicate) []string {
	return Mapping(ps, func(e rds.Predicate) string {
		return e.PredicateStr
	})
}

// ParallelBatch 并发跑，workerId 即 0...degree 编号
func ParallelBatch(start, end, degree int, worker func(batchStart, batchEnd, workerId int) error) error {
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
