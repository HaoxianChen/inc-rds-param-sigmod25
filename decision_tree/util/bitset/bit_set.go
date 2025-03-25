package bitset

const (
	KeepByMap    = 0
	KeepBySlice  = 1
	KeepStrategy = KeepByMap
)

func NewBitSetByCurStrategy(bitLength int) BitSet {
	if KeepStrategy == KeepByMap {
		return NewBitSetByMapWithCap(bitLength)
	} else {
		return NewBitSetBySliceWithCap(bitLength)
	}
}

type BitSet interface {
	FillWithOnes(start, num int)                 // FillWithOnes 将从start开始的num位置为1
	SetBit(pos int)                              // SetBit 设置该位为1
	GetBit(pos int) int                          // GetBit 获取该位bit，返回0则该位为0，返回非0则是1
	Shift(offset uint64)                         // Shift 移动offset位
	Union(other BitSet)                          // Union 与另一个位串进行或操作
	UnionWithOffset(other BitSet, offset uint64) // UnionWithOffset 与另一个位串移位后进行或操作
	Intersect(other BitSet)                      // Intersect 与另一个位串进行与操作
	Count() uint64                               // Count 计数，位串中有几个1
	AllOneBitsInUint32() []uint32                // AllOneBitsInUint32 获得具体有哪几位为1
	AllOneBitsInUint64() []uint64                // AllOneBitsInUint64 获得具体有哪几位为1
	Clear()                                      // Clear 清空一下数据，为了垃圾回收
	GetSimpleCount() uint64                      // GetSimpleCount 多算子专用接口
	//ByMap()(bool,map[int]uint64)                 // fixme:改成反射了
	//BySlice()(bool,[]uint64)
}
