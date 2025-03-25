package bitset

type BitSetByMap struct {
	data        map[int]uint64 // 这里不按[]uint64来存，考虑到稀疏串的问题，然后key以后可能要改uint64
	simpleCount uint64         //暂时给多算子使用的一个单位，不影响其他业务
}

func NewBitSetByMap(d map[int]uint64) *BitSetByMap {
	return &BitSetByMap{d, 0}
}

// NewBitSetByMapWithCap 预先分配位串的长度，输入参数为需要的位数
func NewBitSetByMapWithCap(bitLength int) *BitSetByMap {
	// fixme:考虑一下，对于bitLength小于0的情况要不要返回nil，这样外面初始化时可能要改
	blockSize := 0
	if bitLength > 0 {
		blockSize = (bitLength + 63) / 64
	}
	data := make(map[int]uint64, blockSize)
	//for i := 0 ; i < blockSize; i++ {  // 在map里存下这些块
	//	data[i] = 0
	//}
	return &BitSetByMap{
		data: data,
	}
}

// FillWithOnes 设置从start这一位开始的num位为1
func (bitSet *BitSetByMap) FillWithOnes(start, num int) {
	if start < 0 || num < 0 {
		// panic("negative param in fill-ones")
		// fixme:可以加个错误处理
		return
	}
	startBlockIndex := start / 64 // 设置哪一块，从0起
	startPosInBlock := start % 64

	endBlockIndex := (start + num) / 64 // 结束于哪一块，可能会多加一个空块
	endPosInBlock := (start + num) % 64

	if startBlockIndex != endBlockIndex {
		(*bitSet).data[startBlockIndex] |= ^((1 << startPosInBlock) - 1) // 这一块从starPos(可以取到)开始全部置1

		for i := startBlockIndex + 1; i < endBlockIndex; i++ {
			(*bitSet).data[i] = ^uint64(0)
		}

		(*bitSet).data[endBlockIndex] |= (1 << endPosInBlock) - 1 // 这一块从0开始到endPos(取不到)全部置1
	} else {
		// 在一块里操作
		(*bitSet).data[startBlockIndex] |= (^((1 << startPosInBlock) - 1)) & ((1 << endPosInBlock) - 1) // 上面两个交集
	}
}

// SetBit 设置该位为1
func (bitSet *BitSetByMap) SetBit(pos int) {
	if pos < 0 {
		// panic("negative position in set bit")
		// fixme:可以加个错误处理
		return
	}
	blockIndex := pos / 64 // 设置哪一块，从0起
	posInBlock := pos % 64
	/*if _, has := (*bitSet).data[blockIndex]; !has {
		(*bitSet).data[blockIndex] = 0
	}*/
	if (*bitSet).data[blockIndex]&(1<<posInBlock) == 0 {
		(*bitSet).data[blockIndex] |= 1 << posInBlock
		(*bitSet).simpleCount += 1
	}
}

// GetBit 获取该位bit，返回0则该位为0，返回非0则是1
func (bitSet *BitSetByMap) GetBit(pos int) int {
	if pos < 0 {
		// panic("negative position in set bit")
		// fixme:可以加个错误处理
		return 0
	}
	blockIndex := pos / 64 // 设置哪一块，从0起
	posInBlock := pos % 64
	if block, has := (*bitSet).data[blockIndex]; !has {
		return 0
	} else {
		if block&(1<<posInBlock) != 0 {
			return 1
		} else {
			return 0
		}
	}
}

// Shift 移动offset位
func (bitSet *BitSetByMap) Shift(offset uint64) {
	newMap := make(map[int]uint64)

	// 设offset为k，移动后最多得到两块数据，记为高一块和低一块
	// 一共64位进行移动，只要看第0位和第63位的移动
	// ((63+k) % 64) + 1，有多少位移到高一块
	// (0+k+63) / 64，高一块的地址

	// 分别拿到要移到高一块和低一块的那几位
	highBitsNum := int(((63 + offset) % 64) + 1)
	lowBitsNum := 64 - highBitsNum

	lowBitsMask := uint64((1 << lowBitsNum) - 1)
	highBitsMask := ^lowBitsMask

	highBlockOffset := int((offset + 63) / 64)

	for blockIndex, blockData := range (*bitSet).data {
		highBits := blockData & highBitsMask
		lowBits := blockData & lowBitsMask

		blockIndex += highBlockOffset
		newMap[blockIndex] |= highBits >> lowBitsNum

		if lowBitsNum == 0 {
			// 都在一块里，就是移动了64的整数倍
			// 那就不用再存低位了
		} else {
			// 高位和低位要分开存
			blockIndex -= 1 // 不会为负
			newMap[blockIndex] |= lowBits << highBitsNum
		}
	}

	(*bitSet).data = newMap
}

// Union 与另一个位串进行或操作
func (bitSet *BitSetByMap) Union(other BitSet) {
	switch other.(type) {
	case *BitSetByMap:
		dataInOther := (*other.(*BitSetByMap)).data
		for blockIndex, blockDataInOther := range dataInOther {
			(*bitSet).data[blockIndex] |= blockDataInOther
		}
	default:
		for _, data := range other.AllOneBitsInUint64() {
			bitSet.SetBit(int(data))
		}

	}
}

// UnionWithOffset 和Union方法有点不同，另一个BitSet每一位要加上offset后再进行合并，相当于把另一个位串中各位移动offset位之后再合并
func (bitSet *BitSetByMap) UnionWithOffset(other BitSet, offset uint64) {
	dataInOther := (*other.(*BitSetByMap)).data // 可能panic
	//isByMap, dataInOther := other.ByMap()
	//if !isByMap {
	//	panic("inConsistent")
	//}

	// 设offset为k，移动后最多得到两块数据，记为高一块和低一块
	// 一共64位进行移动，只要看第0位和第63位的移动
	// ((63+k) % 64) + 1，有多少位移到高一块
	// (0+k+63) / 64，高一块的地址

	// 分别拿到要移到高一块和低一块的那几位
	highBitsNum := int(((63 + offset) % 64) + 1)
	lowBitsNum := 64 - highBitsNum

	lowBitsMask := uint64((1 << lowBitsNum) - 1)
	highBitsMask := ^lowBitsMask

	highBlockOffset := int((offset + 63) / 64)

	for blockIndex, blockDataInOther := range dataInOther {
		highBits := blockDataInOther & highBitsMask
		lowBits := blockDataInOther & lowBitsMask

		blockIndex += highBlockOffset
		(*bitSet).data[blockIndex] |= highBits >> lowBitsNum

		if lowBitsNum == 0 {
			// 都在一块里，就是移动了64的整数倍
			// 那就不用再存低位了
		} else {
			// 高位和低位要分开存
			blockIndex -= 1 // 不会为负
			(*bitSet).data[blockIndex] |= lowBits << highBitsNum
		}
	}
}

// Intersect 与另一个位串进行与操作
func (bitSet *BitSetByMap) Intersect(other BitSet) {
	dataInOther := (*other.(*BitSetByMap)).data // 可能panic
	//isByMap, dataInOther := other.ByMap()
	//if !isByMap {
	//	panic("inConsistent")
	//}
	for blockIndex, blockDataInOther := range dataInOther {
		if blockDataInOwn, has := (*bitSet).data[blockIndex]; has {
			(*bitSet).data[blockIndex] = blockDataInOwn & blockDataInOther
		} else {
			(*bitSet).data[blockIndex] = blockDataInOther
		}
	}
}

// Count 计数，位串中有几个1
func (bitSet *BitSetByMap) Count() uint64 {
	var count uint64 = 0
	for _, block := range (*bitSet).data {
		for keep := block; keep != 0; {
			count += 1
			keep = keep & (keep - 1)
		}
	}
	return count
}

// GetSimpleCount 返回结构中的simpleCount数值
func (bitSet *BitSetByMap) GetSimpleCount() uint64 {
	return bitSet.simpleCount
}

// AllOneBitsInUint32 获得具体有哪几位为1
func (bitSet *BitSetByMap) AllOneBitsInUint32() []uint32 {
	res := make([]uint32, 0, bitSet.Count())
	for blockIndex, block := range (*bitSet).data {
		for onePos := uint32(0); block != 0; onePos++ {
			if block&0x1 == 1 {
				// 最低位是1
				res = append(res, (uint32(blockIndex)<<6)+onePos)
			}
			block >>= 1 // 如果是int的话，是逻辑位移，所以还是改uint64了
		}
	}
	return res
}

// AllOneBitsInUint64 获得具体有哪几位为1
func (bitSet *BitSetByMap) AllOneBitsInUint64() []uint64 {
	res := make([]uint64, 0, bitSet.Count())
	for blockIndex, block := range (*bitSet).data {
		for onePos := uint64(0); block != 0; onePos++ {
			if block&0x1 == 1 {
				// 最低位是1
				res = append(res, (uint64(blockIndex)<<6)+onePos)
			}
			block >>= 1 // 如果是int的话，是逻辑位移，所以还是改uint64了
		}
	}
	return res
}

// Clear 清一下map，为了垃圾回收
func (bitSet *BitSetByMap) Clear() {
	(*bitSet).data = make(map[int]uint64, 0)
}

func (bitSet *BitSetByMap) ByMap() (bool, map[int]uint64) {
	return true, (*bitSet).data
}

func (bitSet *BitSetByMap) BySlice() (bool, []uint64) {
	return false, nil
}
